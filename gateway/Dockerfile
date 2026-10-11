# ---- build ----
FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git
WORKDIR /build
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
ENV GOTOOLCHAIN=auto
RUN go mod tidy && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway

# ---- runtime ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=builder /out/gateway /usr/local/bin/gateway
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD curl -fsS http://localhost:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/gateway"]
