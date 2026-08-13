import glob
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import crawler


# ============================================================
# CONFIG
# ============================================================

BATCH_FILES = int(
    os.getenv("BACKFILL_BATCH_FILES", "250")
)

INSERT_WORKERS = int(
    os.getenv("BACKFILL_INSERT_WORKERS", "6")
)

DRY_RUN = (
    os.getenv("BACKFILL_DRY_RUN", "0") == "1"
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("amu-backfill")


# ============================================================
# URL SAFETY
# ============================================================

ABSOLUTE_URL_RE = re.compile(
    r"^https?://",
    re.IGNORECASE,
)

EMBEDDED_ABSOLUTE_URL_RE = re.compile(
    r"https?://",
    re.IGNORECASE,
)


def is_safe_candidate(url):
    """
    Final safety filter before inserting a discovered URL.

    Rejects:
      - malformed URLs containing another absolute URL inside
        the path/query
      - userinfo URLs
      - non-http/https schemes
      - URLs that are not within configured allowed domains
    """

    if not url:
        return False

    url = url.strip()

    if not ABSOLUTE_URL_RE.match(url):
        return False

    try:
        parsed = urlparse(url)

        if parsed.scheme.lower() not in ("http", "https"):
            return False

        if not parsed.hostname:
            return False

        # Never allow username/password URLs.
        if parsed.username or parsed.password:
            return False

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Reject malformed URLs such as:
        #
        # https://api.amu.ac.in/storage/https://youtube.com/...
        #
        # There should not be another absolute http/https URL
        # embedded inside path/query/fragment.
        # ----------------------------------------------------

        remainder = (
            (parsed.path or "")
            + "?"
            + (parsed.query or "")
            + "#"
            + (parsed.fragment or "")
        )

        if EMBEDDED_ABSOLUTE_URL_RE.search(remainder):
            return False

        normalized = crawler.normalize_url(url)

        if not normalized:
            return False

        if not crawler.allowed_url(normalized):
            return False

        return True

    except Exception:
        return False


# ============================================================
# BASE URL
# ============================================================

def load_base_url(html_path):
    """
    Get original page URL from metadata.

    Falls back to the Markdown file if metadata is missing.
    """

    name = os.path.splitext(
        os.path.basename(html_path)
    )[0]

    metadata_path = os.path.join(
        crawler.METADATA_DIR,
        name + ".json",
    )

    try:

        with open(
            metadata_path,
            "r",
            encoding="utf-8",
            errors="replace",
        ) as f:

            meta = json.load(f)

        url = meta.get("url")

        if url:

            normalized = crawler.normalize_url(
                url
            )

            if normalized and crawler.allowed_url(
                normalized
            ):
                return normalized

    except Exception:
        pass

    # --------------------------------------------------------
    # Markdown fallback
    # --------------------------------------------------------

    md_path = os.path.join(
        crawler.PAGES_DIR,
        name + ".md",
    )

    try:

        with open(
            md_path,
            "r",
            encoding="utf-8",
            errors="replace",
        ) as f:

            text = f.read(10000)

        m = re.search(
            r"^Source URL:\s*(\S+)",
            text,
            re.MULTILINE,
        )

        if m:

            normalized = crawler.normalize_url(
                m.group(1)
            )

            if normalized and crawler.allowed_url(
                normalized
            ):
                return normalized

    except Exception:
        pass

    return None


# ============================================================
# SCAN ONE HTML FILE
# ============================================================

def scan_one(html_path):

    base_url = load_base_url(
        html_path
    )

    if not base_url:
        return None, set()

    try:

        with open(
            html_path,
            "r",
            encoding="utf-8",
            errors="replace",
        ) as f:

            html = f.read()

        candidates = (
            crawler.extract_candidate_urls(
                base_url,
                html,
            )
        )

        # ----------------------------------------------------
        # Final safety filtering.
        # This is deliberately repeated here even though
        # crawler.extract_candidate_urls() also normalizes/
        # filters URLs.
        # ----------------------------------------------------

        safe_candidates = set()

        rejected = 0

        for candidate in candidates:

            if is_safe_candidate(
                candidate
            ):

                safe_candidates.add(
                    crawler.normalize_url(
                        candidate
                    )
                )

            else:

                rejected += 1

        if rejected:
            log.debug(
                "Rejected %d unsafe candidates from %s",
                rejected,
                base_url,
            )

        return (
            base_url,
            safe_candidates,
        )

    except Exception as e:

        log.warning(
            "Scan failed %s: %s",
            html_path,
            e,
        )

        return (
            base_url,
            set(),
        )


# ============================================================
# INSERT ONE URL
# ============================================================

def insert_one(item):

    url, parent_url, depth = item

    try:

        if not is_safe_candidate(
            url
        ):

            return 0

        if crawler.insert_url(
            url,
            parent_url=parent_url,
            depth=depth,
        ):

            return 1

    except Exception as e:

        log.debug(
            "Insert failed %s: %s",
            url,
            e,
        )

    return 0


# ============================================================
# FLUSH CANDIDATES
# ============================================================

def flush_candidates(candidate_map):

    if not candidate_map:
        return 0

    if DRY_RUN:

        log.info(
            "DRY RUN: would evaluate %d candidates",
            len(candidate_map),
        )

        return 0

    inserted = 0

    items = [
        (
            url,
            parent_url,
            depth,
        )

        for url, (
            parent_url,
            depth,
        ) in candidate_map.items()
    ]

    with ThreadPoolExecutor(
        max_workers=INSERT_WORKERS
    ) as pool:

        futures = [
            pool.submit(
                insert_one,
                item,
            )

            for item in items
        ]

        for future in as_completed(
            futures
        ):

            try:

                inserted += future.result()

            except Exception:

                log.exception(
                    "Candidate insert worker failed"
                )

    return inserted


# ============================================================
# MAIN
# ============================================================

def main():

    # Load DB columns used by crawler.insert_url().
    crawler.load_table_columns()

    html_files = sorted(
        glob.glob(
            os.path.join(
                crawler.PAGES_DIR,
                "*.html",
            )
        )
    )

    log.info(
        "AMU discovery backfill starting"
    )

    log.info(
        "HTML files found = %d",
        len(html_files),
    )

    log.info(
        "Batch files = %d",
        BATCH_FILES,
    )

    log.info(
        "Insert workers = %d",
        INSERT_WORKERS,
    )

    log.info(
        "Dry run = %s",
        DRY_RUN,
    )

    total_files = 0
    total_candidates = 0
    total_inserted = 0
    total_rejected = 0

    # --------------------------------------------------------
    # URL -> (source page, depth)
    # --------------------------------------------------------

    candidate_map = {}

    for start in range(
        0,
        len(html_files),
        BATCH_FILES,
    ):

        batch = html_files[
            start:start + BATCH_FILES
        ]

        for html_path in batch:

            total_files += 1

            base_url, candidates = (
                scan_one(
                    html_path
                )
            )

            if not base_url:
                continue

            # ------------------------------------------------
            # Existing crawler has no MAX_DEPTH.
            #
            # For backfill, source depth is deliberately
            # reconstructed conservatively as 0, therefore
            # newly discovered links start at depth 1.
            # This does NOT limit future crawling.
            # ------------------------------------------------

            source_depth = 0

            for url in candidates:

                if not is_safe_candidate(
                    url
                ):

                    total_rejected += 1
                    continue

                candidate_map.setdefault(
                    url,
                    (
                        base_url,
                        source_depth + 1,
                    ),
                )

        batch_candidates = len(
            candidate_map
        )

        inserted = flush_candidates(
            candidate_map
        )

        total_candidates += (
            batch_candidates
        )

        total_inserted += inserted

        log.info(
            "Backfill progress: "
            "files=%d/%d | "
            "candidates=%d | "
            "inserted=%d | "
            "total_inserted=%d | "
            "rejected=%d",
            total_files,
            len(html_files),
            batch_candidates,
            inserted,
            total_inserted,
            total_rejected,
        )

        candidate_map.clear()

    log.info(
        "AMU discovery backfill complete | "
        "files=%d | "
        "candidates=%d | "
        "inserted=%d | "
        "rejected=%d",
        total_files,
        total_candidates,
        total_inserted,
        total_rejected,
    )


if __name__ == "__main__":
    main()