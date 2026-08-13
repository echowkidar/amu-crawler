import os
import re
import json
import time
import hashlib
import html as html_lib
import logging
import threading
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urldefrag

import requests
import psycopg
import fitz
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# CONFIG
# ============================================================

DB_HOST = os.getenv("DB_HOST", "firecrawl-nuq-postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "firecrawl")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_SCHEMA = os.getenv("DB_SCHEMA", "amu_crawler")
DB_TABLE = os.getenv("DB_TABLE", "urls")

FIRECRAWL_URL = os.getenv("FIRECRAWL_URL", "http://firecrawl-api:3002").rstrip("/")
FIRECRAWL_TIMEOUT = int(os.getenv("FIRECRAWL_TIMEOUT", "180"))

HTML_WORKERS = int(os.getenv("HTML_WORKERS", "12"))
PDF_WORKERS = int(os.getenv("PDF_WORKERS", "16"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "8"))
STALE_MINUTES = int(os.getenv("STALE_MINUTES", "30"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.0"))

# A URL with less than this amount of extracted PDF text is treated
# as a scanned/mostly-image PDF and is handed to the OCR queue.
PDF_MIN_TEXT_CHARS = int(os.getenv("PDF_MIN_TEXT_CHARS", "80"))

CORPUS = os.getenv("CORPUS", "/amu-corpus")
PAGES_DIR = os.path.join(CORPUS, "pages")
PDF_DIR = os.path.join(CORPUS, "pdf")
METADATA_DIR = os.path.join(CORPUS, "metadata")
OCR_DIR = os.path.join(CORPUS, "ocr")
OCR_QUEUE = os.path.join(OCR_DIR, "queue")
LOG_DIR = os.path.join(CORPUS, "logs")

TABLE_COLUMNS = set()

ALLOWED_DOMAINS = {
    d.strip().lower()
    for d in re.split(r"[\s,]+", os.getenv("ALLOWED_DOMAINS", "amu.ac.in"))
    if d.strip()
}

OLD_DOMAIN = "old.amu.ac.in"
OLD_BACKOFF_BASE = float(os.getenv("OLD_BACKOFF_BASE", "10"))
OLD_BACKOFF_MAX = float(os.getenv("OLD_BACKOFF_MAX", "900"))

HTML_EXTENSIONS = {
    ".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".jspx",
    ".xhtml", ""
}
PDF_EXTENSIONS = {".pdf"}

# ============================================================
# LOGGING
# ============================================================

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, "crawler.log"), encoding="utf-8")
    ],
)
log = logging.getLogger("amu-crawler")

# ============================================================
# DB
# ============================================================

def qname():
    s = DB_SCHEMA.replace('"', '""')
    t = DB_TABLE.replace('"', '""')
    return f'"{s}"."{t}"'


def db():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        connect_timeout=15,
    )


def ensure_schema():
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{DB_SCHEMA}"')
            cur.execute(f"""
                ALTER TABLE {qname()}
                ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ
            """)
            cur.execute(f"""
                ALTER TABLE {qname()}
                ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ
            """)
            cur.execute(f"""
                ALTER TABLE {qname()}
                ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_amu_crawler_status_retry
                ON {qname()} (status, next_retry_at, id)
            """)
            conn.commit()


def reset_stale_processing():
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {qname()}
                SET status='pending',
                    next_retry_at=NOW(),
                    last_error=COALESCE(last_error, 'Recovered stale processing row')
                WHERE status='processing'
                  AND COALESCE(last_error, '') <> 'Queued for OCR'
                  AND (
                      last_attempt_at IS NULL OR
                      last_attempt_at < NOW() - (%s * INTERVAL '1 minute')
                  )
            """, (STALE_MINUTES,))
            n = cur.rowcount
            conn.commit()
            log.info("Recovered %d stale processing rows", n)


def requeue_failed():
    # Existing failed rows are not deleted. Rows below MAX_RETRIES are
    # made eligible again. Their retry_count remains intact.
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {qname()}
                SET status='pending',
                    next_retry_at=COALESCE(next_retry_at, NOW())
                WHERE status='failed'
                  AND COALESCE(retry_count, 0) < %s
            """, (MAX_RETRIES,))
            n = cur.rowcount
            conn.commit()
            log.info("Requeued %d retryable failed URLs", n)


def claim_url(kind):
    """
    Atomically claim one pending URL.

    kind='pdf'    -> known .pdf URLs
    kind='html'   -> everything else

    A stale processing row is recovered separately at startup.
    """
    if kind == "pdf":
        predicate = """
            (
                lower(split_part(split_part(url, '?', 1), '#', 1))
                LIKE '%%.pdf'
            )
        """
    else:
        predicate = """
            NOT (
                lower(split_part(split_part(url, '?', 1), '#', 1))
                LIKE '%%.pdf'
            )
        """

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id, url, COALESCE(retry_count, 0), COALESCE(depth, 0)
                FROM {qname()}
                WHERE status='pending'
                  AND COALESCE(next_retry_at, NOW()) <= NOW()
                  AND COALESCE(retry_count, 0) < %s
                  AND {predicate}
                ORDER BY
                    CASE
                        WHEN split_part(split_part(url, '://', 2), '/', 1) = %s
                        THEN 1 ELSE 0
                    END,
                    id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            """, (MAX_RETRIES, OLD_DOMAIN))
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None

            row_id, url, retry_count, depth = row
            cur.execute(f"""
                UPDATE {qname()}
                SET status='processing',
                    last_attempt_at=NOW()
                WHERE id=%s
            """, (row_id,))
            conn.commit()
            return {
                "id": row_id,
                "url": url,
                "retry_count": retry_count,
                "depth": depth,
            }


def update_row(row_id, status, retry_count=None, http_status=None,
               last_error=None, completed=False, next_retry_at=None):
    sets = ["status=%s"]
    vals = [status]

    # Only update columns that exist in the legacy table.
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
            """, (DB_SCHEMA, DB_TABLE))
            cols = {r[0] for r in cur.fetchall()}

            if "retry_count" in cols and retry_count is not None:
                sets.append("retry_count=%s")
                vals.append(retry_count)

            if "http_status" in cols and http_status is not None:
                sets.append("http_status=%s")
                vals.append(http_status)

            if "last_error" in cols:
                sets.append("last_error=%s")
                vals.append(last_error)

            if "last_attempt_at" in cols:
                sets.append("last_attempt_at=NOW()")

            if "completed_at" in cols and completed:
                sets.append("completed_at=NOW()")

            if "next_retry_at" in cols:
                if next_retry_at is None:
                    sets.append("next_retry_at=NULL")
                else:
                    sets.append("next_retry_at=%s")
                    vals.append(next_retry_at)

            vals.append(row_id)
            cur.execute(
                f"UPDATE {qname()} SET {', '.join(sets)} WHERE id=%s",
                vals,
            )
            conn.commit()


# ============================================================
# URL / CORPUS
# ============================================================

def normalize_url(url):
    if not url:
        return None
    try:
        url, _ = urldefrag(url.strip())
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.hostname:
            return None
        # Remove default ports and fragments; preserve paths/query.
        host = p.hostname.lower()
        if p.port and not ((p.scheme == "http" and p.port == 80) or
                           (p.scheme == "https" and p.port == 443)):
            netloc = f"{host}:{p.port}"
        else:
            netloc = host
        if p.username or p.password:
            return None
        path = p.path or "/"
        return p._replace(netloc=netloc, path=path, fragment="").geturl()
    except Exception:
        return None


def allowed_url(url):
    try:
        host = (urlparse(url).hostname or "").lower()
        return any(host == d or host.endswith("." + d) for d in ALLOWED_DOMAINS)
    except Exception:
        return False


def url_hash(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def insert_url(url, parent_url=None, depth=None):
    url = normalize_url(url)
    if not url or not allowed_url(url):
        return False

    values = [url]
    cols = ["url"]

    # Advisory lock prevents duplicate discovery even if the legacy table
    # has no UNIQUE constraint on normalized_url.
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (url,))

            cur.execute(
                f"SELECT 1 FROM {qname()} WHERE url=%s OR normalized_url=%s LIMIT 1",
                (url, url),
            )
            if cur.fetchone():
                conn.commit()
                return False

            if "status" in TABLE_COLUMNS:
                cols.append("status")
                values.append("pending")
            if "retry_count" in TABLE_COLUMNS:
                cols.append("retry_count")
                values.append(0)
            if "parent_url" in TABLE_COLUMNS:
                cols.append("parent_url")
                values.append(parent_url)
            if "depth" in TABLE_COLUMNS:
                cols.append("depth")
                values.append(depth if depth is not None else 0)
            if "discovered_at" in TABLE_COLUMNS:
                cols.append("discovered_at")
                # PostgreSQL default normally handles this; explicitly setting
                # NOW() below is easier for old schemas.
                cols[-1] = "discovered_at"
            if "normalized_url" in TABLE_COLUMNS:
                cols.append("normalized_url")
                values.append(url)

            placeholders = ",".join(["%s"] * len(values))
            col_sql = ",".join(cols)

            if "discovered_at" in TABLE_COLUMNS:
                # Replace the value placeholder for discovered_at with NOW().
                pieces = []
                for c in cols:
                    pieces.append("NOW()" if c == "discovered_at" else "%s")
                placeholders = ",".join(pieces)

            cur.execute(
                f"INSERT INTO {qname()} ({col_sql}) VALUES ({placeholders})",
                values,
            )
            conn.commit()
            return True


def save_atomic(path, data, mode="w"):
    tmp = path + ".tmp"
    with open(tmp, mode, encoding="utf-8", errors="replace") as f:
        f.write(data)
    os.replace(tmp, path)


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def metadata_path(h):
    return os.path.join(METADATA_DIR, h + ".json")


# ============================================================
# HTTP
# ============================================================

_thread_local = threading.local()


def session():
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        retry = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=32,
            pool_maxsize=32,
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.headers.update({
            "User-Agent": "AMU-RAG-Crawler/3.0 (+research corpus crawler)"
        })
        _thread_local.session = s
    return _thread_local.session


def backoff_seconds(url, retry_count):
    host = (urlparse(url).hostname or "").lower()
    if host == OLD_DOMAIN:
        return min(OLD_BACKOFF_MAX, OLD_BACKOFF_BASE * (2 ** max(0, retry_count - 1)))
    return min(300, 2 ** max(0, retry_count - 1))


def retry_failed(row, error, http_status=None):
    retry_count = int(row["retry_count"]) + 1
    if retry_count >= MAX_RETRIES:
        update_row(
            row["id"], "failed", retry_count=retry_count,
            http_status=http_status, last_error=error,
            completed=False, next_retry_at=None
        )
        return

    delay = backoff_seconds(row["url"], retry_count)
    next_at = datetime.now(timezone.utc).timestamp() + delay
    next_dt = datetime.fromtimestamp(next_at, tz=timezone.utc)

    update_row(
        row["id"], "pending", retry_count=retry_count,
        http_status=http_status, last_error=error,
        completed=False, next_retry_at=next_dt
    )


# ============================================================
# PDF
# ============================================================

def process_pdf(row):
    row_id, url, retry_count = row["id"], row["url"], row["retry_count"]
    h = url_hash(url)
    pdf_path = os.path.join(PDF_DIR, h + ".pdf")

    try:
        log.info("PDF download: %s", url)
        r = session().get(url, timeout=(20, 180), verify=False, allow_redirects=True)
        r.raise_for_status()

        content_type = (r.headers.get("content-type") or "").lower()
        if not r.content.startswith(b"%PDF") and "pdf" not in content_type:
            raise RuntimeError(f"Not a PDF response: content-type={content_type}")

        with open(pdf_path + ".tmp", "wb") as f:
            f.write(r.content)
        os.replace(pdf_path + ".tmp", pdf_path)

        text_parts = []
        with fitz.open(pdf_path) as doc:
            for page in doc:
                t = page.get_text("text") or ""
                if t.strip():
                    text_parts.append(t)

        text = "\n".join(text_parts).strip()

        meta = {
            "url": url,
            "type": "pdf",
            "content_type": content_type,
            "http_status": r.status_code,
            "pages": len(text_parts),
            "ocr_required": len(text) < PDF_MIN_TEXT_CHARS,
        }

        if len(text) >= PDF_MIN_TEXT_CHARS:
            md = (
                "# AMU PDF Document\n\n"
                f"Source URL: {url}\n\n"
                "Document type: Text PDF\n\n"
                "---\n\n"
                f"{text}\n"
            )
            save_atomic(os.path.join(PAGES_DIR, h + ".md"), md)
            save_json(metadata_path(h), meta)
            update_row(
                row_id, "completed", retry_count=retry_count,
                http_status=r.status_code, last_error=None,
                completed=True, next_retry_at=None
            )
            log.info("PDF TEXT completed: %s", url)
            return

        # Scanned / image-heavy PDF: hand off to persistent OCR service.
        job = {
            "id": row_id,
            "url": url,
            "hash": h,
            "pdf": pdf_path,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        job_path = os.path.join(OCR_QUEUE, h + ".json")
        save_json(job_path, job)
        save_json(metadata_path(h), meta)

        # OCR worker will change processing -> completed.
        update_row(
            row_id, "processing", retry_count=retry_count,
            http_status=r.status_code, last_error="Queued for OCR",
            completed=False, next_retry_at=None
        )
        log.info("PDF queued for OCR: %s", url)

    except Exception as e:
        log.error("PDF failed %s: %s", url, e)
        retry_failed(row, "PDF: " + str(e))


# ============================================================
# HTML / FIRECRAWL
# ============================================================

def firecrawl_scrape(url):
    payload = {
        "url": url,
        "formats": ["markdown", "html"],
        "onlyMainContent": False,
    }
    r = session().post(
        FIRECRAWL_URL + "/v1/scrape",
        json=payload,
        timeout=FIRECRAWL_TIMEOUT,
    )
    if r.status_code >= 400:
        raise RuntimeError(
            f"Firecrawl HTTP {r.status_code}: {r.text[:500]}"
        )
    obj = r.json()
    data = obj.get("data", obj)
    markdown = data.get("markdown") or ""
    html = data.get("html") or ""
    metadata = data.get("metadata") or {}
    status_code = metadata.get("statusCode") or r.status_code
    return markdown, html, metadata, int(status_code)


DISCOVERY_SKIP_EXTENSIONS = {
    ".css", ".js", ".mjs", ".map",
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".bmp", ".tif", ".tiff",
    ".woff", ".woff2", ".ttf", ".eot", ".otf",
    ".mp3", ".wav", ".ogg", ".mp4", ".webm", ".avi", ".mov", ".mkv",
}

DISCOVERY_DIRECT_EXTENSIONS = {
    ".pdf", ".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".jspx",
    ".xhtml", ".json", ".xml", ".txt", ".csv",
}

DISCOVERY_URL_KEY_RE = re.compile(
    r"[\"'](?:href|url|link|src|cv|pdf_url|pdf|file|download|path|time_table|timetable|attachment|document_url|routerLink|routerlink|ng-reflect-router-link|data-url|data-href|data-link)[\"']\s*[:=]\s*[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)

DISCOVERY_ABSOLUTE_URL_RE = re.compile(
    r"https?://[^\s\"'<>\\]+",
    re.IGNORECASE,
)

DISCOVERY_RELATIVE_PATH_RE = re.compile(
    r"[\"'](/[^\"'<>\s]+)[\"']"
)


def is_discoverable_candidate(url):
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        path = (p.path or "/").lower()
        ext = os.path.splitext(path)[1]

        # Ignore static assets.
        if ext in DISCOVERY_SKIP_EXTENSIONS:
            return False

        # Directly crawlable document/page types.
        if ext in DISCOVERY_DIRECT_EXTENSIONS:
            return True

        # ----------------------------------------------------
        # LMS special cases
        #
        # These are session/UI/static endpoints, not useful
        # crawl targets for the AMU corpus.
        # ----------------------------------------------------
        if host == "lms.amu.ac.in":
            if path.startswith("/theme/switchdevice.php"):
                return False

            if path.startswith("/theme/image.php"):
                return False

        # API paths themselves are not crawl targets.
        # Embedded URLs inside their HTML/JSON are extracted
        # separately.
        if "/api/" in path:
            if (
                host == "api.amu.ac.in"
                and path.startswith("/api/")
            ):
                return False

            return True

        # Ignore common static directories.
        if path.startswith(("/assets/", "/static/")):
            return False

        return not ext or path.endswith("/")

    except Exception:
        return False


def resolve_candidate_url(base_url, raw_value):
    """
    Safely resolve a discovered URL.

    Absolute URLs are kept absolute; relative URLs are resolved
    against base_url. Malformed nested URLs such as
    https://api.amu.ac.in/storage/https://www.youtube.com/...
    are rejected.
    """
    if raw_value is None:
        return None

    value = html_lib.unescape(str(raw_value)).strip()
    value = value.replace("\\/", "/").replace("&quot;", '"')
    value = value.strip().strip('"').strip("'").strip()

    if not value:
        return None

    if value.lower().startswith((
        "#", "mailto:", "javascript:", "tel:", "data:",
        "blob:", "about:"
    )):
        return None

    if value.startswith(("{{", "[[", "<%", "$")):
        return None

    # Absolute URLs must NOT be passed through urljoin(base_url, ...).
    if re.match(r"^https?://", value, re.IGNORECASE):
        candidate = value

    # Protocol-relative URL.
    elif value.startswith("//"):
        scheme = urlparse(base_url).scheme or "https"
        candidate = scheme + ":" + value

    # Relative URL.
    else:
        candidate = urljoin(base_url, value)

    normalized = normalize_url(candidate)
    if not normalized:
        return None

    parsed = urlparse(normalized)

    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return None

    if parsed.username or parsed.password:
        return None

    # Reject malformed nested absolute URLs:
    # /storage/https://www.youtube.com/...
    remainder = (
        (parsed.path or "")
        + "?"
        + (parsed.query or "")
    )
    if re.search(r"https?://", remainder, re.IGNORECASE):
        return None

    if not allowed_url(normalized):
        return None

    if not is_discoverable_candidate(normalized):
        return None

    return normalized


def _add_candidate(candidates, base_url, raw_value):
    new_url = resolve_candidate_url(
        base_url,
        raw_value,
    )
    if new_url:
        candidates.add(new_url)


def extract_candidate_urls(base_url, html):
    """Extract crawlable page/document/API URLs without executing JS."""
    candidates = set()
    if not html:
        return candidates

    soup = BeautifulSoup(html, "lxml")
    navigation_attrs = (
        "href", "src", "routerlink", "routerLink", "ng-reflect-router-link",
        "data-href", "data-url", "data-link",
    )

    for tag in soup.find_all(True):
        for attr in navigation_attrs:
            value = tag.get(attr)
            if value:
                _add_candidate(candidates, base_url, value)
        for attr_name, attr_value in tag.attrs.items():
            if not attr_name.lower().startswith("on") or not isinstance(attr_value, str):
                continue
            for m in DISCOVERY_ABSOLUTE_URL_RE.finditer(attr_value):
                _add_candidate(candidates, base_url, m.group(0))
            for m in DISCOVERY_RELATIVE_PATH_RE.finditer(attr_value):
                _add_candidate(candidates, base_url, m.group(1))

    for script in soup.find_all("script"):
        script_text = script.string or script.get_text() or ""
        if not script_text:
            continue
        for m in DISCOVERY_ABSOLUTE_URL_RE.finditer(script_text):
            _add_candidate(candidates, base_url, m.group(0))
        for m in DISCOVERY_URL_KEY_RE.finditer(script_text):
            _add_candidate(candidates, base_url, m.group(1))
        for m in DISCOVERY_RELATIVE_PATH_RE.finditer(script_text):
            _add_candidate(candidates, base_url, m.group(1))

    for m in DISCOVERY_ABSOLUTE_URL_RE.finditer(html):
        _add_candidate(candidates, base_url, m.group(0))

    return candidates


def discover_links(base_url, html, depth):
    discovered = 0
    for new_url in sorted(extract_candidate_urls(base_url, html)):
        try:
            if insert_url(new_url, parent_url=base_url, depth=depth + 1):
                discovered += 1
        except Exception as e:
            log.debug("Discovery insert failed %s: %s", new_url, e)
    return discovered


def process_html(row):
    row_id, url, retry_count = row["id"], row["url"], row["retry_count"]
    h = url_hash(url)

    try:
        log.info("HTML Firecrawl: %s", url)
        markdown, html, metadata, status_code = firecrawl_scrape(url)

        if not markdown.strip():
            # Keep HTML as a fallback corpus document if Firecrawl did not
            # produce markdown.
            soup = BeautifulSoup(html or "", "lxml")
            markdown = soup.get_text("\n", strip=True)

        if not markdown.strip():
            raise RuntimeError("Firecrawl returned empty content")

        md = (
            "# AMU Web Page\n\n"
            f"Source URL: {url}\n\n"
            "---\n\n"
            f"{markdown}\n"
        )
        save_atomic(os.path.join(PAGES_DIR, h + ".md"), md)

        if html:
            save_atomic(os.path.join(PAGES_DIR, h + ".html"), html)

        save_json(
            metadata_path(h),
            {
                "url": url,
                "type": "html",
                "http_status": status_code,
                "firecrawl": metadata,
            },
        )

        # THIS IS THE CONTINUOUS DISCOVERY STEP.
        discovered = discover_links(url, html, int(row.get("depth") or 0))

        update_row(
            row_id, "completed", retry_count=retry_count,
            http_status=status_code, last_error=None,
            completed=True, next_retry_at=None
        )
        log.info(
            "HTML completed: %s | discovered=%d",
            url, discovered
        )

    except Exception as e:
        log.error("HTML failed %s: %s", url, e)
        retry_failed(row, "HTML: " + str(e))


# ============================================================
# WORKER LOOPS
# ============================================================

def html_loop():
    while True:
        try:
            row = claim_url("html")

            if not row:
                time.sleep(POLL_SECONDS)
                continue

            process_html(row)

        except Exception:
            log.exception("HTML worker exception; continuing")
            time.sleep(2)


def pdf_loop():
    while True:
        try:
            row = claim_url("pdf")

            if not row:
                time.sleep(POLL_SECONDS)
                continue

            process_pdf(row)

        except Exception:
            log.exception("PDF worker exception; continuing")
            time.sleep(2)


def load_table_columns():
    global TABLE_COLUMNS
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
            """, (DB_SCHEMA, DB_TABLE))
            TABLE_COLUMNS = {r[0] for r in cur.fetchall()}
    return TABLE_COLUMNS


def main():
    for d in [PAGES_DIR, PDF_DIR, METADATA_DIR, OCR_DIR, OCR_QUEUE, LOG_DIR]:
        os.makedirs(d, exist_ok=True)

    ensure_schema()

    load_table_columns()

    log.info("Database columns detected: %s", sorted(TABLE_COLUMNS))
    log.info("=" * 50)
    log.info("AMU CRAWLER PRODUCTION STARTING")
    log.info("HTML workers = %d", HTML_WORKERS)
    log.info("PDF workers = %d", PDF_WORKERS)
    log.info("Corpus = %s", CORPUS)
    log.info("Firecrawl = %s", FIRECRAWL_URL)
    log.info("Max retries = %d", MAX_RETRIES)
    log.info("Allowed domains = %s", sorted(ALLOWED_DOMAINS))
    log.info("=" * 50)

    reset_stale_processing()
    requeue_failed()

    # Separate pools prevent slow PDFs from starving HTML discovery.
    with ThreadPoolExecutor(max_workers=HTML_WORKERS, thread_name_prefix="html") as hp, \
         ThreadPoolExecutor(max_workers=PDF_WORKERS, thread_name_prefix="pdf") as pp:
        for _ in range(HTML_WORKERS):
            hp.submit(html_loop)
        for _ in range(PDF_WORKERS):
            pp.submit(pdf_loop)

        # Keep main process alive.
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    main()