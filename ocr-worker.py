import os
import glob
import json
import time
import subprocess
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import psycopg
import fitz


DB_HOST = os.getenv("DB_HOST", "firecrawl-nuq-postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "firecrawl")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_SCHEMA = os.getenv("DB_SCHEMA", "amu_crawler")
DB_TABLE = os.getenv("DB_TABLE", "urls")

CORPUS = os.getenv("CORPUS", "/amu-corpus")
QUEUE = os.path.join(CORPUS, "ocr", "queue")
PAGES_DIR = os.path.join(CORPUS, "pages")
OCR_DIR = os.path.join(CORPUS, "ocr")
METADATA_DIR = os.path.join(CORPUS, "metadata")

OCR_WORKERS = int(os.getenv("OCR_WORKERS", "3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "8"))
OCR_LANG = os.getenv("OCR_LANG", "eng+hin+urd")
POLL_SECONDS = float(os.getenv("OCR_POLL_SECONDS", "2"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
log = logging.getLogger("amu-ocr")


def qname():
    return (
        '"' + DB_SCHEMA.replace('"', '""') +
        '"."' + DB_TABLE.replace('"', '""') + '"'
    )


def db():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        connect_timeout=15,
    )


def update(row_id, status, error=None, increment_retry=False):
    with db() as conn:
        with conn.cursor() as cur:
            if increment_retry:
                cur.execute(f"""
                    UPDATE {qname()}
                    SET status=%s,
                        retry_count=COALESCE(retry_count, 0) + 1,
                        last_error=%s,
                        next_retry_at=NOW() + INTERVAL '30 seconds'
                    WHERE id=%s
                    RETURNING retry_count
                """, (status, error, row_id))
                row = cur.fetchone()
                retry_count = int(row[0]) if row else MAX_RETRIES
                if retry_count >= MAX_RETRIES:
                    cur.execute(f"""
                        UPDATE {qname()}
                        SET status='failed',
                            next_retry_at=NULL
                        WHERE id=%s
                    """, (row_id,))
            else:
                cur.execute(f"""
                    UPDATE {qname()}
                    SET status=%s,
                        last_error=%s,
                        completed_at=CASE WHEN %s='completed' THEN NOW()
                                         ELSE completed_at END,
                        next_retry_at=CASE WHEN %s='pending' THEN NOW()
                                          ELSE next_retry_at END
                    WHERE id=%s
                """, (status, error, status, status, row_id))
            conn.commit()


def recover_processing_jobs():
    for p in glob.glob(os.path.join(QUEUE, "*.processing")):
        target = p[:-11]
        try:
            os.replace(p, target)
        except Exception:
            pass


def claim_job():
    jobs = sorted(glob.glob(os.path.join(QUEUE, "*.json")))
    for job in jobs:
        processing = job + ".processing"
        try:
            os.replace(job, processing)
            return processing
        except FileNotFoundError:
            continue
        except OSError:
            continue
    return None


def atomic_write(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", errors="replace") as f:
        f.write(text)
    os.replace(tmp, path)


def process(job_path):
    with open(job_path, "r", encoding="utf-8") as f:
        job = json.load(f)

    row_id = job["id"]
    url = job["url"]
    h = job["hash"]
    pdf = job["pdf"]

    sidecar = os.path.join(OCR_DIR, h + ".txt")
    md_path = os.path.join(PAGES_DIR, h + ".md")
    metadata_path = os.path.join(METADATA_DIR, h + ".json")

    try:
        log.info("OCR START: %s", url)

        cmd = [
            "ocrmypdf",
            "-l", OCR_LANG,
            "--deskew",
            "--rotate-pages",
            "--sidecar", sidecar,
            "--output-type", "none",
            "--force-ocr",
            pdf,
            "/dev/null",
        ]

        result = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=int(os.getenv("OCR_TIMEOUT", "1800")),
        )

        if result.stdout:
            log.debug("OCR output for %s: %s", url, result.stdout[-2000:])

        text = ""
        if os.path.exists(sidecar):
            with open(sidecar, "r", encoding="utf-8", errors="replace") as f:
                text = f.read().strip()

        if not text:
            with fitz.open(pdf) as doc:
                text = "\n".join(
                    (page.get_text("text") or "")
                    for page in doc
                ).strip()

        if not text:
            raise RuntimeError("OCR produced empty text")

        md = (
            "# AMU PDF Document\n\n"
            f"Source URL: {url}\n\n"
            "Document type: Scanned/OCR PDF\n\n"
            f"OCR language: {OCR_LANG}\n\n"
            "---\n\n"
            f"{text}\n"
        )
        atomic_write(md_path, md)

        metadata = {
            "url": url,
            "type": "pdf",
            "ocr": True,
            "ocr_engine": "OCRmyPDF/Tesseract",
            "language": OCR_LANG,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        atomic_write(metadata_path, json.dumps(
            metadata, ensure_ascii=False, indent=2
        ))

        update(row_id, "completed", None)
        os.remove(job_path)
        log.info("OCR COMPLETED: %s", url)

    except Exception as e:
        error = "OCR: " + str(e)
        log.error("OCR FAILED %s: %s", url, error)
        update(row_id, "pending", error, increment_retry=True)

        # Put it back into the queue. The crawler/worker can retry it after
        # a restart without losing the job.
        original = job_path[:-11] if job_path.endswith(".processing") else job_path
        try:
            os.replace(job_path, original)
        except Exception:
            pass


def worker_loop():
    while True:
        job = claim_job()
        if not job:
            time.sleep(POLL_SECONDS)
            continue
        try:
            process(job)
        except Exception:
            log.exception("Unexpected OCR worker exception")
            time.sleep(1)


def main():
    for d in [QUEUE, PAGES_DIR, OCR_DIR, METADATA_DIR]:
        os.makedirs(d, exist_ok=True)

    recover_processing_jobs()

    log.info("=" * 50)
    log.info("AMU OCR WORKER")
    log.info("Persistent workers = %d", OCR_WORKERS)
    log.info("Languages = %s", OCR_LANG)
    log.info("Queue = %s", QUEUE)
    log.info("=" * 50)

    with ThreadPoolExecutor(
        max_workers=OCR_WORKERS,
        thread_name_prefix="ocr"
    ) as pool:
        for _ in range(OCR_WORKERS):
            pool.submit(worker_loop)
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    main()
