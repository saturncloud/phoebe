# Build all three phoebe binaries in one image; the k8s command/args select
# which one a given workload runs. One image+tag ships the whole system:
#   /app/phoebe         — the interceptor (the streaming proxy, listens :8080)
#   /app/phoebe-drainer — Valkey stream → Postgres billing_event (a Deployment)
#   /app/phoebe-rater   — batch rating job billing_event → rated_usage (a CronJob)
# Go >= 1.26.10 / 1.27.3 for CVE-2026-111943 (crypto/x509 distrusts the compromised eMudhra intermediates).
FROM golang:1.26.10-alpine AS builder

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

# Static builds (CGO disabled) so the binaries run on a minimal base.
ENV CGO_ENABLED=0
RUN go build -o /phoebe ./cmd/interceptor && \
    go build -o /phoebe-drainer ./cmd/drainer && \
    go build -o /phoebe-rater ./cmd/rater

FROM alpine:latest

# ca-certificates for outbound TLS (Valkey/Postgres/upstreams over TLS). --upgrade keeps the
# trust store current (>= 20261020 drops the emSign ECC Root CA - G3, CVE-2026-111943).
RUN apk add --no-cache --upgrade ca-certificates

COPY --from=builder /phoebe /app/phoebe
COPY --from=builder /phoebe-drainer /app/phoebe-drainer
COPY --from=builder /phoebe-rater /app/phoebe-rater

# Default to the interceptor; the drainer/rater workloads override the command.
ENTRYPOINT ["/app/phoebe"]
