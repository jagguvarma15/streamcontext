# streamcontext gateway image.
#
# Multi-stage to keep the runtime image lean. The model weights for
# sentence-transformers are NOT baked in — they're downloaded on first start
# and cached under /home/streamcontext/.cache (the non-root user's home,
# mounted as a volume in compose).

FROM python:3.11-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY streamcontext/ ./streamcontext/

RUN pip install --upgrade pip && \
    pip install --prefix=/install .

# ---

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        librdkafka1 \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
WORKDIR /app
COPY streamcontext/ ./streamcontext/
COPY schemas/ ./schemas/

# Non-root user
RUN useradd --create-home --uid 1001 streamcontext && \
    mkdir -p /home/streamcontext/.cache && \
    chown -R streamcontext:streamcontext /app /home/streamcontext
USER streamcontext

# Prometheus /metrics + /health (SC_METRICS_PORT). Documentation only; the
# compose file publishes it to the host.
EXPOSE 9108

ENTRYPOINT ["python", "-m", "streamcontext.main"]
