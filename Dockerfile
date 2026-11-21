FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_DEV=1 \
    UV_FROZEN=1 \
    PYTHONPATH=/app \
    UV_NO_SYNC=1 \
    PATH="/app/.venv/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY src/ src/
COPY bin/ bin/
COPY migrations/ migrations/
COPY alembic.ini ./

RUN addgroup --system --gid 1000 appuser && \
    adduser --system --uid 1000 --home /home/appuser --ingroup appuser appuser

USER 1000:1000
EXPOSE 8000

CMD ["python", "-m", "bin.container"]
