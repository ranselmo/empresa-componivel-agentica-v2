FROM golang:1.22-bookworm AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /shard-router ./cmd/

FROM gcr.io/distroless/static-debian12
COPY --from=build /shard-router /shard-router
EXPOSE 8080
ENTRYPOINT ["/shard-router"]
