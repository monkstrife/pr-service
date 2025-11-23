FROM golang:1.24.2 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o pr-service ./cmd/pr-service
FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/pr-service /app/pr-service
COPY config ./config
RUN mkdir -p /app/storage
ENV CONFIG_PATH=/app/config/prod.yaml
ENV STORAGE_PATH=/app/storage/storage.db
ENV HTTP_SERVER_PASSWORD=change_me
EXPOSE 8080
CMD ["/app/pr-service"]