import os
import glob
import json
import time
import subprocess
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

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
    "eng+hin+urd",
)

OCR_DPI = int(
    os.getenv("OCR_DPI", "200"),
)

OCR_PSM = int(
    os.getenv("OCR_PSM", "6"),
)

OCR_PAGE_TIMEOUT = int(
    os.getenv("OCR_PAGE_TIMEOUT", "300"),
)

OCR_POLL_SECONDS = float(
    os.getenv("OCR_POLL_SECONDS", "2"),
)

# ----------------------------------------------------------
# OCR WATCHDOG
# ----------------------------------------------------------

OCR_WATCHDOG_ENABLED = (
    os.getenv("OCR_WATCHDOG_ENABLED", "1") == "1"
)

OCR_WATCHDOG_INTERVAL = int(
    os.getenv("OCR_WATCHDOG_INTERVAL", "60")
)

OCR_WATCHDOG_STALE_MINUTES = int(
    os.getenv("OCR_WATCHDOG_STALE_MINUTES", "15")
)
MAX_RETRIES = int(
    os.getenv("MAX_RETRIES", "8"),
)

OCR_RETRY_DELAY = int(
    os.getenv("OCR_RETRY_DELAY", "30"),
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
# OCR WATCHDOG STATE
# ==========================================================

heartbeat_lock = threading.Lock()

last_progress_at = time.time()
active_job_path = None
active_job_started_at = None


def mark_worker_progress():
    global last_progress_at

    with heartbeat_lock:
        last_progress_at = time.time()


def set_active_job(job_path):
    global active_job_path
    global active_job_started_at

    with heartbeat_lock:
        active_job_path = job_path
        active_job_started_at = time.time()
        last_progress_at = time.time()


def clear_active_job():
    global active_job_path
    global active_job_started_at
    global last_progress_at

    with heartbeat_lock:
        active_job_path = None
        active_job_started_at = None
        last_progress_at = time.time()

# ==========================================================
# DB
# ==========================================================

def qname():
    schema = DB_SCHEMA.replace('"', '""')
    table = DB_TABLE.replace('"', '""')
    return f'"{schema}"."{table}"'


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
# DATABASE - SUCCESS
# ==========================================================

def mark_completed(row_id):
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


# ==========================================================
# DATABASE - FAILURE / RETRY
# ==========================================================

def mark_failed_or_retry(row_id, error):
    """
    Increment retry_count.

    retry_count < MAX_RETRIES:
        status = pending
        next_retry_at = now + OCR_RETRY_DELAY

    retry_count >= MAX_RETRIES:
        status = failed
        next_retry_at = NULL
    """

    with db() as conn:
        with conn.cursor() as cur:

            cur.execute(
                f"""
                SELECT COALESCE(retry_count, 0)
                FROM {qname()}
                WHERE id = %s
                """,
                (row_id,),
            )

            row = cur.fetchone()

            current_retry = int(row[0]) if row else 0
            new_retry = current_retry + 1

            if new_retry >= MAX_RETRIES:

                cur.execute(
                    f"""
                    UPDATE {qname()}
                    SET
                        status = 'failed',
                        retry_count = %s,
                        last_error = %s,
                        next_retry_at = NULL
                    WHERE id = %s
                    """,
                    (
                        new_retry,
                        error,
                        row_id,
                    ),
                )

                status = "failed"
                next_retry_at = None

            else:

                next_retry_at = (
                    datetime.now(timezone.utc)
                    + timedelta(seconds=OCR_RETRY_DELAY)
                )

                cur.execute(
                    f"""
                    UPDATE {qname()}
                    SET
                        status = 'pending',
                        retry_count = %s,
                        last_error = %s,
                        next_retry_at = %s
                    WHERE id = %s
                    """,
                    (
                        new_retry,
                        error,
                        next_retry_at,
                        row_id,
                    ),
                )

                status = "pending"

        conn.commit()

    return status, new_retry, next_retry_at


# ==========================================================
# STALE JOB RECOVERY
# ==========================================================

def recover_processing_jobs():
    """
    On startup, recover previous .json.processing files.

    These are assumed to be abandoned jobs from an earlier
    worker process/container shutdown.
    """

    recovered = 0

    for path in glob.glob(
        os.path.join(QUEUE, "*.json.processing")
    ):

        target = path[:-11]

        try:
            os.replace(path, target)
            recovered += 1

        except FileNotFoundError:
            continue

        except OSError as e:
            log.warning(
                "Could not recover %s: %s",
                path,
                e,
            )

    if recovered:
        log.info(
            "Recovered %d stale OCR processing jobs",
            recovered,
        )


# ==========================================================
# RETRY FILE PROMOTION
# ==========================================================

def promote_retry_jobs():
    """
    Retry files use:

        *.json.retry

    Their filesystem mtime is the retry eligibility time.

    Once OCR_RETRY_DELAY has elapsed they are moved back to:

        *.json
    """

    while True:

        try:

            now = time.time()

            for path in glob.glob(
                os.path.join(QUEUE, "*.json.retry")
            ):

                try:

                    mtime = os.path.getmtime(path)

                    if now - mtime < OCR_RETRY_DELAY:
                        continue

                    target = path[:-6]

                    os.replace(
                        path,
                        target,
                    )

                    log.info(
                        "Retrying OCR job: %s",
                        os.path.basename(target),
                    )

                except FileNotFoundError:
                    continue

                except OSError as e:
                    log.warning(
                        "Retry promotion failed %s: %s",
                        path,
                        e,
                    )

        except Exception:
            log.exception(
                "Unexpected retry promoter error"
            )

        time.sleep(
            min(
                2.0,
                OCR_RETRY_DELAY,
            )
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
                processing,
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
        path,
    )


# ==========================================================
# OCR ONE PAGE
# ==========================================================

def ocr_page(page, page_number, total_pages):

    """
    Render one page and run Tesseract.

    Returns ONLY actual OCR text.

    Page markers are NOT returned here and therefore can never
    inflate the OCR character count.
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
            result.stdout or ""
        ).strip()

        elapsed = time.time() - start

        mark_worker_progress()
        
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
# PROCESS OCR JOB
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

    txt_path = os.path.join(
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

            # --------------------------------------------------
            # IMPORTANT:
            #
            # page_sections contains formatting markers.
            # actual_chars counts ONLY OCR text.
            # --------------------------------------------------

            page_sections = []
            actual_chars = 0

            for page_number in range(
                1,
                total_pages + 1,
            ):

                page = doc[
                    page_number - 1
                ]

                page_text = ocr_page(
                    page,
                    page_number,
                    total_pages,
                )

                actual_chars += len(
                    page_text
                )

                page_sections.append(
                    f"--- PAGE {page_number} ---\n\n"
                    f"{page_text}\n"
                )

        finally:

            doc.close()

        # --------------------------------------------------
        # FINAL OCR TEXT
        # --------------------------------------------------

        text = "\n".join(
            page_sections
        ).strip()

        # --------------------------------------------------
        # CRITICAL EMPTY OCR CHECK
        #
        # Do NOT use len(text) here because page markers
        # themselves are text.
        # --------------------------------------------------

        if actual_chars <= 0:

            raise RuntimeError(
                "OCR produced empty text"
            )

        # --------------------------------------------------
        # SAVE TXT
        # --------------------------------------------------

        atomic_write(
            txt_path,
            text + "\n",
        )

        # --------------------------------------------------
        # SAVE MARKDOWN
        # --------------------------------------------------

        md = (
            "# AMU PDF Document\n\n"

            f"Source URL: {url}\n\n"

            "Document type: Scanned/OCR PDF\n\n"

            "OCR engine: Tesseract\n\n"

            f"OCR language: {OCR_LANG}\n\n"

            f"OCR DPI: {OCR_DPI}\n\n"

            f"OCR PSM: {OCR_PSM}\n\n"

            f"Pages: {total_pages}\n\n"

            f"Actual OCR characters: {actual_chars}\n\n"

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
            "characters": actual_chars,
            "completed_at": datetime.now(
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
        # DATABASE COMPLETE
        # --------------------------------------------------

        mark_completed(
            row_id
        )

        # --------------------------------------------------
        # REMOVE PROCESSING JOB
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
            actual_chars,
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
        # UPDATE DATABASE
        # --------------------------------------------------

        try:

            status, retry_count, next_retry_at = (
                mark_failed_or_retry(
                    row_id,
                    error,
                )
            )

        except Exception:

            log.exception(
                "Database failure while updating OCR job %s",
                row_id,
            )

            status = "pending"
            retry_count = None
            next_retry_at = None

        # --------------------------------------------------
        # MAX RETRIES REACHED
        # --------------------------------------------------

        if status == "failed":

            try:
                os.remove(
                    job_path
                )

            except FileNotFoundError:
                pass

            log.error(
                "OCR PERMANENTLY FAILED | id=%s | retries=%s",
                row_id,
                retry_count,
            )

            return

        # --------------------------------------------------
        # RETRY
        #
        # IMPORTANT:
        # Do NOT return immediately as *.json.
        #
        # Otherwise the same job can be picked up again
        # immediately and create an infinite retry loop.
        #
        # Put it into *.json.retry and let the retry promoter
        # release it after OCR_RETRY_DELAY.
        # --------------------------------------------------

        retry_path = (
            job_path[:-11]
            + ".json.retry"
            if job_path.endswith(".json.processing")
            else job_path + ".retry"
        )

        try:

            os.replace(
                job_path,
                retry_path,
            )

            log.warning(
                "OCR RETRY WAIT | id=%s | retry=%s/%s | delay=%ss",
                row_id,
                retry_count,
                MAX_RETRIES,
                OCR_RETRY_DELAY,
            )

        except FileNotFoundError:
            pass

        except Exception:

            log.exception(
                "Could not move failed OCR job to retry queue"
            )


# ==========================================================
# WORKER LOOP
# ==========================================================

# ==========================================================
# OCR WATCHDOG
# ==========================================================

def watchdog_loop():

    log.info(
        "OCR watchdog enabled=%s interval=%ss stale=%sm",
        OCR_WATCHDOG_ENABLED,
        OCR_WATCHDOG_INTERVAL,
        OCR_WATCHDOG_STALE_MINUTES,
    )

    while True:

        try:

            if not OCR_WATCHDOG_ENABLED:
                time.sleep(
                    OCR_WATCHDOG_INTERVAL
                )
                continue

            with heartbeat_lock:
                progress_at = last_progress_at
                job_path = active_job_path
                job_started_at = active_job_started_at

            # ------------------------------------------------
            # Nothing active.
            # ------------------------------------------------
            if not job_path:

                time.sleep(
                    OCR_WATCHDOG_INTERVAL
                )

                continue

            now = time.time()

            stale_seconds = (
                OCR_WATCHDOG_STALE_MINUTES * 60
            )

            no_progress_for = (
                now - progress_at
            )

            job_age = (
                now - job_started_at
                if job_started_at
                else 0
            )

            # ------------------------------------------------
            # Worker is healthy.
            # ------------------------------------------------
            if no_progress_for < stale_seconds:

                time.sleep(
                    OCR_WATCHDOG_INTERVAL
                )

                continue

            # ------------------------------------------------
            # Worker appears stuck.
            #
            # IMPORTANT:
            # Do NOT rename/delete .processing here.
            #
            # Exit the container instead. Docker's
            # restart: unless-stopped will restart it and
            # recover_processing_jobs() will safely recover
            # the abandoned job.
            # ------------------------------------------------

            processing_count = len(
                glob.glob(
                    os.path.join(
                        QUEUE,
                        "*.json.processing"
                    )
                )
            )

            log.error(
                "OCR WATCHDOG: no progress for %.1f minutes | "
                "job_age=%.1f minutes | processing_files=%d | "
                "job=%s | restarting worker",
                no_progress_for / 60,
                job_age / 60,
                processing_count,
                os.path.basename(job_path),
            )

            # Give the log a moment to flush.
            for handler in logging.getLogger().handlers:
                try:
                    handler.flush()
                except Exception:
                    pass

            os._exit(1)

        except Exception:

            log.exception(
                "Unexpected OCR watchdog error"
            )

        time.sleep(
            OCR_WATCHDOG_INTERVAL
        )

def worker_loop():

    while True:

        job = claim_job()

        if not job:

            time.sleep(
                OCR_POLL_SECONDS
            )

            continue

        set_active_job(job)

        try:

            process(job)

        except Exception:

            log.exception(
                "Unexpected OCR worker exception"
            )

            time.sleep(1)

        finally:

            clear_active_job()


# ==========================================================
# MAIN
# ==========================================================

def main():

    for directory in [
        QUEUE,
        PAGES_DIR,
        OCR_DIR,
        METADATA_DIR,
    ]:

        os.makedirs(
            directory,
            exist_ok=True,
        )

    # Recover jobs left as .processing after
    # container crash/restart.
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
        "Page timeout = %ds",
        OCR_PAGE_TIMEOUT,
    )

    log.info(
        "Retry delay = %ds",
        OCR_RETRY_DELAY,
    )

    log.info(
        "Max retries = %d",
        MAX_RETRIES,
    )

    log.info(
        "Queue = %s",
        QUEUE,
    )

    log.info(
        "=" * 50
    )

    # Retry promoter is lightweight and only checks
    # *.json.retry files.
    retry_thread = threading.Thread(
        target=promote_retry_jobs,
        name="ocr-retry",
        daemon=True,
    )

    retry_thread.start()
    
    if OCR_WATCHDOG_ENABLED:

        watchdog_thread = threading.Thread(
            target=watchdog_loop,
            name="ocr-watchdog",
            daemon=True,
        )

        watchdog_thread.start()
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

            time.sleep(
                3600
            )


if __name__ == "__main__":
    main()