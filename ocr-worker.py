import os
import glob
import json
import time
import subprocess
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import psycopg
import fitz


# ==========================================================
# DATABASE
# ==========================================================

DB_HOST = os.getenv("DB_HOST", "firecrawl-nuq-postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "firecrawl")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_SCHEMA = os.getenv("DB_SCHEMA", "amu_crawler")
DB_TABLE = os.getenv("DB_TABLE", "urls")


# ==========================================================
# CORPUS
# ==========================================================

CORPUS = os.getenv("CORPUS", "/amu-corpus")

QUEUE = os.path.join(CORPUS, "ocr", "queue")
PAGES_DIR = os.path.join(CORPUS, "pages")
OCR_DIR = os.path.join(CORPUS, "ocr")
METADATA_DIR = os.path.join(CORPUS, "metadata")


# ==========================================================
# OCR SETTINGS
# ==========================================================

OCR_WORKERS = int(os.getenv("OCR_WORKERS", "1"))

OCR_LANG = os.getenv(
    "OCR_LANG",
    "eng+hin+urd"
)

OCR_DPI = int(
    os.getenv("OCR_DPI", "200")
)

OCR_PSM = int(
    os.getenv("OCR_PSM", "6")
)

OCR_PAGE_TIMEOUT = int(
    os.getenv("OCR_PAGE_TIMEOUT", "300")
)

OCR_POLL_SECONDS = float(
    os.getenv("OCR_POLL_SECONDS", "2")
)

MAX_RETRIES = int(
    os.getenv("MAX_RETRIES", "8")
)


# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)

log = logging.getLogger("amu-ocr")


# ==========================================================
# DATABASE HELPERS
# ==========================================================

def qname():
    return (
        '"'
        + DB_SCHEMA.replace('"', '""')
        + '"."'
        + DB_TABLE.replace('"', '""')
        + '"'
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


# ==========================================================
# DATABASE STATUS UPDATE
# ==========================================================

def update_completed(row_id):
    """
    Mark OCR job completed.
    """

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute(
                f"""
                UPDATE {qname()}
                SET
                    status = 'completed',
                    last_error = NULL,
                    completed_at = NOW(),
                    next_retry_at = NULL
                WHERE id = %s
                """,
                (row_id,),
            )

        conn.commit()


def update_failed(row_id, error):
    """
    Handle OCR failure.

    IMPORTANT:
    - Retry count is incremented here.
    - If retry limit is reached -> permanently failed.
    - Otherwise job becomes pending with a future retry time.
    """

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute(
                f"""
                UPDATE {qname()}
                SET
                    status = CASE
                        WHEN COALESCE(retry_count, 0) + 1 >= %s
                        THEN 'failed'
                        ELSE 'pending'
                    END,

                    retry_count =
                        COALESCE(retry_count, 0) + 1,

                    last_error = %s,

                    next_retry_at = CASE
                        WHEN COALESCE(retry_count, 0) + 1 >= %s
                        THEN NULL
                        ELSE NOW() + INTERVAL '30 seconds'
                    END

                WHERE id = %s

                RETURNING
                    status,
                    retry_count,
                    next_retry_at
                """,
                (
                    MAX_RETRIES,
                    error,
                    MAX_RETRIES,
                    row_id,
                ),
            )

            row = cur.fetchone()

        conn.commit()

    if row:
        status = row[0]
        retry_count = int(row[1])
        next_retry = row[2]

        if status == "failed":

            log.error(
                "OCR PERMANENTLY FAILED | id=%s | retries=%d | error=%s",
                row_id,
                retry_count,
                error,
            )

        else:

            log.warning(
                "OCR RETRY SCHEDULED | id=%s | retry=%d/%d | next=%s",
                row_id,
                retry_count,
                MAX_RETRIES,
                next_retry,
            )


# ==========================================================
# STALE PROCESSING JOB RECOVERY
# ==========================================================

def recover_processing_jobs():

    recovered = 0

    for p in glob.glob(
        os.path.join(QUEUE, "*.json.processing")
    ):

        target = p[:-11]

        try:

            os.replace(p, target)
            recovered += 1

        except FileNotFoundError:
            continue

        except OSError as e:

            log.warning(
                "Could not recover processing job %s: %s",
                p,
                e,
            )

    if recovered:

        log.info(
            "Recovered %d stale OCR processing jobs",
            recovered,
        )


# ==========================================================
# CLAIM JOB
# ==========================================================

def claim_job():

    jobs = sorted(
        glob.glob(
            os.path.join(QUEUE, "*.json")
        )
    )

    for job in jobs:

        processing = job + ".processing"

        try:

            os.replace(
                job,
                processing
            )

            return processing

        except FileNotFoundError:

            continue

        except OSError:

            continue

    return None


# ==========================================================
# ATOMIC WRITE
# ==========================================================

def atomic_write(path, text):

    tmp = path + ".tmp"

    with open(
        tmp,
        "w",
        encoding="utf-8",
        errors="replace",
    ) as f:

        f.write(text)

    os.replace(
        tmp,
        path
    )


# ==========================================================
# OCR ONE PAGE
# ==========================================================

def ocr_page(page, page_number, total_pages):

    """
    Render one PDF page and run Tesseract.

    Rendering is done at configured DPI.

    OCR timeout protects worker from a single pathological page.
    """

    pix = page.get_pixmap(
        dpi=OCR_DPI,
        colorspace=fitz.csGRAY,
        alpha=False,
    )

    width = pix.width
    height = pix.height

    image_path = (
        f"/tmp/"
        f"amu_ocr_{os.getpid()}_{page_number}.png"
    )

    try:

        pix.save(image_path)

        start = time.time()

        cmd = [
            "tesseract",
            image_path,
            "stdout",
            "-l",
            OCR_LANG,
            "--psm",
            str(OCR_PSM),
        ]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=OCR_PAGE_TIMEOUT,
        )

        text = (
            result.stdout
            if result.stdout
            else ""
        ).strip()

        elapsed = time.time() - start

        log.info(
            "OCR PAGE: %s/%s | size=%sx%s | chars=%d | %.1fs",
            page_number,
            total_pages,
            width,
            height,
            len(text),
            elapsed,
        )

        return text

    finally:

        try:
            os.remove(image_path)
        except FileNotFoundError:
            pass


# ==========================================================
# PROCESS PDF
# ==========================================================

def process(job_path):

    with open(
        job_path,
        "r",
        encoding="utf-8",
    ) as f:

        job = json.load(f)

    row_id = job["id"]
    url = job["url"]
    h = job["hash"]
    pdf = job["pdf"]

    sidecar = os.path.join(
        OCR_DIR,
        h + ".txt",
    )

    md_path = os.path.join(
        PAGES_DIR,
        h + ".md",
    )

    metadata_path = os.path.join(
        METADATA_DIR,
        h + ".json",
    )

    try:

        log.info(
            "OCR START: %s",
            url,
        )

        # --------------------------------------------------
        # OPEN PDF
        # --------------------------------------------------

        try:

            doc = fitz.open(pdf)

        except Exception as e:

            raise RuntimeError(
                f"Unable to open PDF: {e}"
            )

        try:

            total_pages = len(doc)

            if total_pages <= 0:

                raise RuntimeError(
                    "PDF contains zero pages"
                )

            log.info(
                "OCR PDF: %s | pages=%d | dpi=%d",
                url,
                total_pages,
                OCR_DPI,
            )

            all_text = []

            # --------------------------------------------------
            # PAGE BY PAGE OCR
            # --------------------------------------------------

            for page_number in range(
                1,
                total_pages + 1,
            ):

                page = doc[
                    page_number - 1
                ]

                text = ocr_page(
                    page,
                    page_number,
                    total_pages,
                )

                all_text.append(
                    f"--- PAGE {page_number} ---\n\n"
                    f"{text}\n"
                )

        finally:

            doc.close()

        # --------------------------------------------------
        # COMBINE OCR
        # --------------------------------------------------

        text = "\n".join(
            all_text
        ).strip()

        # --------------------------------------------------
        # EMPTY OCR
        # --------------------------------------------------

        if not text:

            raise RuntimeError(
                "OCR produced empty text"
            )

        # --------------------------------------------------
        # SAVE OCR TEXT
        # --------------------------------------------------

        atomic_write(
            sidecar,
            text + "\n",
        )

        # --------------------------------------------------
        # SAVE MARKDOWN
        # --------------------------------------------------

        md = (
            "# AMU PDF Document\n\n"

            f"Source URL: {url}\n\n"

            "Document type: Scanned/OCR PDF\n\n"

            f"OCR engine: Tesseract\n\n"

            f"OCR language: {OCR_LANG}\n\n"

            f"OCR DPI: {OCR_DPI}\n\n"

            f"OCR PSM: {OCR_PSM}\n\n"

            f"Pages: {total_pages}\n\n"

            "---\n\n"

            f"{text}\n"
        )

        atomic_write(
            md_path,
            md,
        )

        # --------------------------------------------------
        # SAVE METADATA
        # --------------------------------------------------

        metadata = {

            "url": url,

            "type": "pdf",

            "ocr": True,

            "ocr_engine": "Tesseract",

            "language": OCR_LANG,

            "dpi": OCR_DPI,

            "psm": OCR_PSM,

            "pages": total_pages,

            "characters": len(text),

            "completed_at":
                datetime.now(
                    timezone.utc
                ).isoformat(),
        }

        atomic_write(
            metadata_path,
            json.dumps(
                metadata,
                ensure_ascii=False,
                indent=2,
            ),
        )

        # --------------------------------------------------
        # DATABASE SUCCESS
        # --------------------------------------------------

        update_completed(
            row_id
        )

        # --------------------------------------------------
        # REMOVE PROCESSING FILE
        # --------------------------------------------------

        try:

            os.remove(
                job_path
            )

        except FileNotFoundError:

            pass

        log.info(
            "OCR COMPLETED: %s | pages=%d | chars=%d",
            url,
            total_pages,
            len(text),
        )

    except Exception as e:

        error = (
            "OCR: "
            + str(e)
        )

        log.error(
            "OCR FAILED %s: %s",
            url,
            error,
        )

        # --------------------------------------------------
        # DATABASE FAILURE / RETRY
        # --------------------------------------------------

        try:

            update_failed(
                row_id,
                error,
            )

        except Exception:

            log.exception(
                "Failed to update database for OCR job id=%s",
                row_id,
            )

        # --------------------------------------------------
        # IMPORTANT:
        #
        # DO NOT PUT FAILED JOB BACK INTO QUEUE.
        #
        # This prevents the previous infinite retry loop.
        # --------------------------------------------------

        try:

            os.remove(
                job_path
            )

        except FileNotFoundError:

            pass


# ==========================================================
# WORKER LOOP
# ==========================================================

def worker_loop():

    while True:

        job = claim_job()

        if not job:

            time.sleep(
                OCR_POLL_SECONDS
            )

            continue

        try:

            process(job)

        except Exception:

            log.exception(
                "Unexpected OCR worker exception"
            )

            time.sleep(1)


# ==========================================================
# MAIN
# ==========================================================

def main():

    for d in [
        QUEUE,
        PAGES_DIR,
        OCR_DIR,
        METADATA_DIR,
    ]:

        os.makedirs(
            d,
            exist_ok=True,
        )

    recover_processing_jobs()

    log.info(
        "=" * 50
    )

    log.info(
        "AMU OCR WORKER"
    )

    log.info(
        "Persistent workers = %d",
        OCR_WORKERS,
    )

    log.info(
        "Languages = %s",
        OCR_LANG,
    )

    log.info(
        "DPI = %d",
        OCR_DPI,
    )

    log.info(
        "PSM = %d",
        OCR_PSM,
    )

    log.info(
        "Queue = %s",
        QUEUE,
    )

    log.info(
        "=" * 50
    )

    with ThreadPoolExecutor(
        max_workers=OCR_WORKERS,
        thread_name_prefix="ocr",
    ) as pool:

        for _ in range(
            OCR_WORKERS
        ):

            pool.submit(
                worker_loop
            )

        while True:

            time.sleep(3600)


if __name__ == "__main__":

    main()