# Build both phoebe binaries (the interceptor and the drainer) in one image.
# Which one runs is selected by the k8s command/args, so a single image ships
# both — simpler to build and pin than two images.
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

# Static build (CGO disabled) so the binaries run on a minimal base.
ENV CGO_ENABLED=0
RUN go build -o /phoebe ./cmd/interceptor && \
    go build -o /phoebe-drainer ./cmd/drainer

FROM alpine:latest

# ca-certificates for outbound TLS (Valkey/Postgres/upstreams over TLS).
RUN apk add --no-cache ca-certificates

COPY --from=builder /phoebe /app/phoebe
COPY --from=builder /phoebe-drainer /app/phoebe-drainer

# Default to the interceptor; the drainer deployment overrides the command.
ENTRYPOINT ["/app/phoebe"]
