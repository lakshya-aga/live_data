# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build tools (needed by some C-extension deps)
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

# Copy only what pip needs to resolve dependencies first (layer-cache friendly)
COPY pyproject.toml .
COPY server/ server/
COPY client/ client/

# Install into an isolated prefix so we can COPY just the result
RUN pip install --no-cache-dir --prefix=/install .

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # default values — override at runtime via env vars or .env file
    HOST=0.0.0.0 \
    PORT=8765

# Bring in installed packages + entry-point script from builder
COPY --from=builder /install /usr/local

# Bring in source packages (needed at runtime since they are imported by name)
COPY server/ server/
COPY client/ client/

# Persistent volume for caches (politician holdings JSON, etc.)
RUN mkdir -p /app/data
VOLUME ["/app/data"]

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import socket; s=socket.create_connection(('localhost', int(__import__('os').environ.get('PORT','8765'))), timeout=3); s.close()" || exit 1

CMD ["data-server"]
