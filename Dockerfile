FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_NO_CACHE_DIR=on \
    TZ=UTC

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -e . \
    && apt-get purge -y build-essential gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/*

COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./alembic.ini
COPY scripts ./scripts
COPY docker/entrypoint.sh ./entrypoint.sh

RUN chmod +x entrypoint.sh

RUN adduser --system --group --home /app sagepick
USER sagepick

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
