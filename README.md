# AMU Crawler Production Setup

This setup intentionally reuses the existing:

- PostgreSQL database/table: amu_crawler.urls
- Docker volume: amu-crawler_amu_corpus
- Docker network: anything_firecrawl-backend
- Firecrawl service: firecrawl-api
- PostgreSQL service: firecrawl-nuq-postgres

It does NOT create a new corpus/database.

## Files

- docker-compose.yml
- Dockerfile.crawler
- crawler.py
- Dockerfile.ocr
- ocr-worker.py
- .env.example

## Build/deploy

In Portainer, Stack -> Add stack, upload/paste the compose and keep the
Dockerfile/crawler files in the same build context if Portainer supports
repository upload/build context.

If building on the server:

    cd /path/to/amu-crawler-production
    docker compose build --no-cache
    docker compose up -d

## Important

Do not delete:

    amu-crawler_amu_corpus

Do not delete or recreate:

    firecrawl-nuq-postgres

## Check status

    docker ps -a --filter name=amu-crawler-v2

    docker ps -a --filter name=amu-crawler-v2-ocr

## Check crawler logs

    docker logs --tail 100 -f amu-crawler-v2

## Check OCR logs

    docker logs --tail 100 -f amu-crawler-v2-ocr

## Check queue

    docker exec firecrawl-nuq-postgres psql -U firecrawl -d postgres \
      -c "SELECT status, COUNT(*) FROM amu_crawler.urls GROUP BY status ORDER BY status;"

## Check discovery

    docker exec firecrawl-nuq-postgres psql -U firecrawl -d postgres \
      -c "SELECT COUNT(*), MIN(discovered_at), MAX(discovered_at) FROM amu_crawler.urls;"

## Check corpus

    docker exec amu-crawler-v2 sh -c \
      'du -sh /amu-corpus; find /amu-corpus -type f | wc -l'

## Architecture

HTML:
    URL -> Firecrawl -> Markdown -> link discovery -> PostgreSQL queue

PDF:
    URL -> direct download -> PyMuPDF text extraction
        -> text sufficient: Markdown
        -> scanned/image PDF: persistent OCR queue

The OCR queue is file-based inside the same persistent corpus volume.
Jobs are atomically claimed by renaming *.json to *.json.processing.
Interrupted jobs are restored on OCR worker startup.

## AnythingLLM

The RAG-ready text is in:

    /amu-corpus/pages/*.md

Metadata is in:

    /amu-corpus/metadata/*.json

This keeps the main RAG corpus separate from the original PDF files and
OCR sidecars.
