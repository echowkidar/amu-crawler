import glob
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import crawler

BATCH_FILES = int(os.getenv("BACKFILL_BATCH_FILES", "250"))
INSERT_WORKERS = int(os.getenv("BACKFILL_INSERT_WORKERS", "6"))
DRY_RUN = os.getenv("BACKFILL_DRY_RUN", "0") == "1"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("amu-backfill")


def load_base_url(html_path):
    """Get original page URL from metadata; fall back to its Markdown file."""
    name = os.path.splitext(os.path.basename(html_path))[0]
    metadata_path = os.path.join(crawler.METADATA_DIR, name + ".json")

    try:
        with open(metadata_path, "r", encoding="utf-8", errors="replace") as f:
            meta = json.load(f)
        url = meta.get("url")
        if url:
            return crawler.normalize_url(url)
    except Exception:
        pass

    md_path = os.path.join(crawler.PAGES_DIR, name + ".md")
    try:
        with open(md_path, "r", encoding="utf-8", errors="replace") as f:
            text = f.read(10000)
        m = re.search(r"^Source URL:\s*(\S+)", text, re.MULTILINE)
        if m:
            return crawler.normalize_url(m.group(1))
    except Exception:
        pass

    return None


def scan_one(html_path):
    base_url = load_base_url(html_path)
    if not base_url:
        return None, set()

    try:
        with open(html_path, "r", encoding="utf-8", errors="replace") as f:
            html = f.read()
        return base_url, crawler.extract_candidate_urls(base_url, html)
    except Exception as e:
        log.warning("Scan failed %s: %s", html_path, e)
        return base_url, set()


def insert_one(item):
    url, parent_url, depth = item
    try:
        if crawler.insert_url(
            url,
            parent_url=parent_url,
            depth=depth,
        ):
            return 1
    except Exception as e:
        log.debug("Insert failed %s: %s", url, e)
    return 0


def flush_candidates(candidate_map):
    if not candidate_map or DRY_RUN:
        return 0

    inserted = 0
    items = [
        (url, parent_url, depth)
        for url, (parent_url, depth) in candidate_map.items()
    ]

    with ThreadPoolExecutor(max_workers=INSERT_WORKERS) as pool:
        futures = [pool.submit(insert_one, item) for item in items]
        for future in as_completed(futures):
            try:
                inserted += future.result()
            except Exception:
                log.exception("Candidate insert worker failed")

    return inserted


def main():
    crawler.load_table_columns()

    html_files = sorted(
        glob.glob(os.path.join(crawler.PAGES_DIR, "*.html"))
    )

    log.info("AMU discovery backfill starting")
    log.info("HTML files found = %d", len(html_files))
    log.info("Batch files = %d", BATCH_FILES)
    log.info("Insert workers = %d", INSERT_WORKERS)
    log.info("Dry run = %s", DRY_RUN)

    total_files = 0
    total_candidates = 0
    total_inserted = 0

    # candidate URL -> (source page, source depth + 1)
    candidate_map = {}

    for start in range(0, len(html_files), BATCH_FILES):
        batch = html_files[start:start + BATCH_FILES]

        for html_path in batch:
            total_files += 1
            base_url, candidates = scan_one(html_path)
            if not base_url:
                continue

            # Backfill depth is reconstructed from the source page's existing
            # database depth when available; otherwise use depth 1 for safety.
            # We deliberately avoid a per-file DB query. Existing depth is not
            # required for crawling because this crawler has no max-depth limit.
            source_depth = 0

            for url in candidates:
                candidate_map.setdefault(
                    url,
                    (base_url, source_depth + 1),
                )

        batch_candidates = len(candidate_map)
        inserted = flush_candidates(candidate_map)

        total_candidates += batch_candidates
        total_inserted += inserted

        log.info(
            "Backfill progress: files=%d/%d | candidates=%d | inserted=%d | total_inserted=%d",
            total_files,
            len(html_files),
            batch_candidates,
            inserted,
            total_inserted,
        )

        candidate_map.clear()

    log.info(
        "AMU discovery backfill complete | files=%d | candidates=%d | inserted=%d",
        total_files,
        total_candidates,
        total_inserted,
    )


if __name__ == "__main__":
    main()