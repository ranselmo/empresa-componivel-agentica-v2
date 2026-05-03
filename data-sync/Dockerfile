FROM golang:1.22-bullseye AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc librdkafka-dev pkg-config
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /app/service ./cmd/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka1 ca-certificates curl
COPY --from=build /app/service /service
EXPOSE 9191
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s \
  CMD curl -sf http://localhost:9191/healthz/live || exit 1
ENTRYPOINT ["/service"]
