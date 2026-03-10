FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    libgl1 \
    libglib2.0-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY backend ./backend
COPY scripts ./scripts

RUN pip install --no-cache-dir .
RUN chmod +x scripts/*.sh

ENV DATA_DIR=/data

CMD ["./scripts/run-api.sh"]
