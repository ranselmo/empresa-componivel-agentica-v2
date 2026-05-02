# ── Build stage ────────────────────────────────────────────────────
FROM golang:1.22-bullseye AS builder

# librdkafka necessária para confluent-kafka-go (CGO_ENABLED=1)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache de dependências separado do código-fonte
COPY go.mod ./
RUN go mod download || true

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /cell ./cmd/

# ── Runtime stage ──────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates tzdata curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /cell ./cell

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s \
    CMD curl -sf http://localhost:8000/health || exit 1

ENTRYPOINT ["./cell"]
