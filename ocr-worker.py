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


# ============================================================
# DATABASE
# ============================================================

DB_HOST = os.getenv("DB_HOST", "firecrawl-nuq-postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "firecrawl")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_SCHEMA = os.getenv("DB_SCHEMA", "amu_crawler")
DB_TABLE = os.getenv("DB_TABLE", "urls")


# ============================================================
# CORPUS
# ============================================================

CORPUS = os.getenv("CORPUS", "/amu-corpus")

QUEUE = os.path.join(
    CORPUS,
    "ocr",
    "queue",
)

PAGES_DIR = os.path.join(
    CORPUS,
    "pages",
)

OCR_DIR = os.path.join(
    CORPUS,
    "ocr",
)

METADATA_DIR = os.path.join(
    CORPUS,
    "metadata",
)


# ============================================================
# OCR SETTINGS
# ============================================================

# Keep this at 1 initially.
# OCR is CPU + memory intensive.
OCR_WORKERS = int(
    os.getenv("OCR_WORKERS", "1")
)

MAX_RETRIES = int(
    os.getenv("MAX_RETRIES", "8")
)

OCR_LANG = os.getenv(
    "OCR_LANG",
    "eng+hin+urd"
)

POLL_SECONDS = float(
    os.getenv("OCR_POLL_SECONDS", "2")
)

# Render scanned PDF pages at 200 DPI.
OCR_DPI = int(
    os.getenv("OCR_DPI", "200")
)

# Tesseract page segmentation mode.
OCR_PSM = os.getenv(
    "OCR_PSM",
    "6"
)

# Maximum time allowed for Tesseract on one page.
OCR_PAGE_TIMEOUT = int(
    os.getenv("OCR_PAGE_TIMEOUT", "300")
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv(
        "LOG_LEVEL",
        "INFO"
    ).upper(),
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(threadName)s | "
        "%(message)s"
    ),
)

log = logging.getLogger(
    "amu-ocr"
)


# ============================================================
# DATABASE HELPERS
# ============================================================

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


def update(
    row_id,
    status,
    error=None,
    increment_retry=False,
):
    """
    Update crawler queue database state.

    On failure:
        retry_count is incremented.

    Once MAX_RETRIES is reached:
        status becomes failed.

    On success:
        status becomes completed.
    """

    with db() as conn:

        with conn.cursor() as cur:

            if increment_retry:

                cur.execute(
                    f"""
                    UPDATE {qname()}
                    SET
                        status=%s,
                        retry_count=
                            COALESCE(retry_count, 0) + 1,
                        last_error=%s,
                        next_retry_at=
                            NOW() + INTERVAL '30 seconds'
                    WHERE id=%s
                    RETURNING retry_count
                    """,
                    (
                        status,
                        error,
                        row_id,
                    ),
                )

                row = cur.fetchone()

                retry_count = (
                    int(row[0])
                    if row
                    else MAX_RETRIES
                )

                if retry_count >= MAX_RETRIES:

                    cur.execute(
                        f"""
                        UPDATE {qname()}
                        SET
                            status='failed',
                            next_retry_at=NULL
                        WHERE id=%s
                        """,
                        (row_id,),
                    )

            else:

                cur.execute(
                    f"""
                    UPDATE {qname()}
                    SET
                        status=%s,
                        last_error=%s,

                        completed_at=
                            CASE
                                WHEN %s='completed'
                                THEN NOW()
                                ELSE completed_at
                            END,

                        next_retry_at=
                            CASE
                                WHEN %s='pending'
                                THEN NOW()
                                ELSE next_retry_at
                            END

                    WHERE id=%s
                    """,
                    (
                        status,
                        error,
                        status,
                        status,
                        row_id,
                    ),
                )

        conn.commit()


# ============================================================
# QUEUE RECOVERY
# ============================================================

def recover_processing_jobs():
    """
    Recover jobs left as:

        *.json.processing

    after a worker/container restart.

    They are returned to:

        *.json
    """

    recovered = 0

    for processing_path in glob.glob(
        os.path.join(
            QUEUE,
            "*.json.processing"
        )
    ):

        target_path = processing_path[:-11]

        try:

            os.replace(
                processing_path,
                target_path,
            )

            recovered += 1

        except Exception as e:

            log.warning(
                "Could not recover OCR job %s: %s",
                processing_path,
                e,
            )

    if recovered:

        log.info(
            "Recovered %d stale OCR processing jobs",
            recovered,
        )


# ============================================================
# QUEUE CLAIM
# ============================================================

def claim_job():
    """
    Atomically claim one OCR queue job.

    Normal:

        file.json

    becomes:

        file.json.processing
    """

    jobs = sorted(
        glob.glob(
            os.path.join(
                QUEUE,
                "*.json"
            )
        )
    )

    for job in jobs:

        processing = (
            job + ".processing"
        )

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


# ============================================================
# FILE HELPERS
# ============================================================

def atomic_write(
    path,
    text,
):
    """
    Write through a temporary file and
    atomically replace the destination.
    """

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


# ============================================================
# TESSERACT
# ============================================================

def run_tesseract(
    image_path,
):
    """
    Run Tesseract against ONE rendered PDF page.

    stdout contains OCR text.
    """

    result = subprocess.run(
        [
            "tesseract",
            image_path,
            "stdout",
            "-l",
            OCR_LANG,
            "--psm",
            OCR_PSM,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=OCR_PAGE_TIMEOUT,
        check=True,
    )

    return result.stdout.strip()


# ============================================================
# PROCESS ONE OCR JOB
# ============================================================

def process(
    job_path,
):

    # --------------------------------------------------------
    # Read queue JSON
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Output paths
    # --------------------------------------------------------

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

        # ----------------------------------------------------
        # Verify PDF exists
        # ----------------------------------------------------

        if not os.path.exists(pdf):

            raise FileNotFoundError(
                f"PDF not found: {pdf}"
            )

        # ----------------------------------------------------
        # Page-by-page OCR
        # ----------------------------------------------------

        page_texts = []

        with fitz.open(pdf) as doc:

            total_pages = len(doc)

            if total_pages <= 0:

                raise RuntimeError(
                    "PDF contains no pages"
                )

            log.info(
                "OCR PDF: %s | pages=%d | dpi=%d",
                url,
                total_pages,
                OCR_DPI,
            )

            # ------------------------------------------------
            # Process ONE page at a time
            # ------------------------------------------------

            for page_no, page in enumerate(
                doc,
                start=1,
            ):

                start_time = time.time()

                image_path = os.path.join(
                    "/tmp",
                    f"amu-ocr-{h}-{page_no}.png",
                )

                try:

                    # ----------------------------------------
                    # Render one page
                    # ----------------------------------------

                    pix = page.get_pixmap(
                        dpi=OCR_DPI,
                        colorspace=fitz.csGRAY,
                        alpha=False,
                    )

                    pix.save(
                        image_path
                    )

                    # ----------------------------------------
                    # OCR one page
                    # ----------------------------------------

                    text = run_tesseract(
                        image_path
                    )

                    page_texts.append(
                        text
                    )

                    elapsed = (
                        time.time()
                        - start_time
                    )

                    log.info(
                        "OCR PAGE: %s | "
                        "%d/%d | "
                        "size=%dx%d | "
                        "chars=%d | "
                        "%.1fs",
                        url,
                        page_no,
                        total_pages,
                        pix.width,
                        pix.height,
                        len(text),
                        elapsed,
                    )

                finally:

                    # ----------------------------------------
                    # Always delete temporary page image
                    # ----------------------------------------

                    try:

                        os.remove(
                            image_path
                        )

                    except FileNotFoundError:

                        pass

        # ----------------------------------------------------
        # Combine page text
        # ----------------------------------------------------

        text = "\n\n".join(
            (
                f"--- Page {page_no} ---\n"
                f"{page_text}"
            )
            for page_no, page_text in enumerate(
                page_texts,
                start=1,
            )
            if page_text
        ).strip()

        # ----------------------------------------------------
        # Empty OCR check
        # ----------------------------------------------------

        if not text:

            raise RuntimeError(
                "OCR produced empty text"
            )

        # ----------------------------------------------------
        # Save OCR sidecar
        # ----------------------------------------------------

        atomic_write(
            sidecar,
            text,
        )

        # ----------------------------------------------------
        # Save Markdown
        # ----------------------------------------------------

        md = (
            "# AMU PDF Document\n\n"
            f"Source URL: {url}\n\n"
            "Document type: Scanned/OCR PDF\n\n"
            f"OCR language: {OCR_LANG}\n\n"
            f"OCR DPI: {OCR_DPI}\n\n"
            f"OCR PSM: {OCR_PSM}\n\n"
            "---\n\n"
            f"{text}\n"
        )

        atomic_write(
            md_path,
            md,
        )

        # ----------------------------------------------------
        # Save metadata
        # ----------------------------------------------------

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
            "completed_at": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),
        }

        atomic_write(
            metadata_path,
            json.dumps(
                metadata,
                ensure_ascii=False,
                indent=2,
            ),
        )

        # ----------------------------------------------------
        # Mark DB completed
        # ----------------------------------------------------

        update(
            row_id,
            "completed",
            None,
        )

        # ----------------------------------------------------
        # Remove queue job ONLY after successful DB update
        # ----------------------------------------------------

        os.remove(
            job_path
        )

        log.info(
            "OCR COMPLETED: %s | "
            "pages=%d | chars=%d",
            url,
            total_pages,
            len(text),
        )

    except Exception as e:

        # ----------------------------------------------------
        # OCR failure
        # ----------------------------------------------------

        error = (
            "OCR: "
            + str(e)
        )

        log.error(
            "OCR FAILED %s: %s",
            url,
            error,
        )

        # ----------------------------------------------------
        # Update DB and increment retry
        # ----------------------------------------------------

        update(
            row_id,
            "pending",
            error,
            increment_retry=True,
        )

        # ----------------------------------------------------
        # Return job to normal queue
        # ----------------------------------------------------

        original = (
            job_path[:-11]
            if job_path.endswith(
                ".processing"
            )
            else job_path
        )

        try:

            os.replace(
                job_path,
                original,
            )

        except Exception as move_error:

            log.error(
                "Could not return failed job "
                "to queue: %s",
                move_error,
            )


# ============================================================
# WORKER LOOP
# ============================================================

def worker_loop():

    while True:

        job = claim_job()

        if not job:

            time.sleep(
                POLL_SECONDS
            )

            continue

        try:

            process(
                job
            )

        except Exception:

            log.exception(
                "Unexpected OCR worker exception"
            )

            time.sleep(1)


# ============================================================
# MAIN
# ============================================================

def main():

    # --------------------------------------------------------
    # Ensure directories exist
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Recover jobs interrupted by previous worker/container
    # --------------------------------------------------------

    recover_processing_jobs()

    # --------------------------------------------------------
    # Startup information
    # --------------------------------------------------------

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
        "PSM = %s",
        OCR_PSM,
    )

    log.info(
        "Queue = %s",
        QUEUE,
    )

    log.info(
        "=" * 50
    )

    # --------------------------------------------------------
    # Persistent OCR workers
    # --------------------------------------------------------

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

        # Keep main process alive.

        while True:

            time.sleep(
                3600
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()