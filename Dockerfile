# Build all phoebe binaries in one image; the k8s command/args select which one a
# given workload runs. One image+tag ships the whole system:
#   /app/phoebe             — the interceptor (the streaming proxy, listens :8080)
#   /app/phoebe-drainer     — Valkey stream → Postgres billing_event (a Deployment)
#   /app/phoebe-rater       — batch rating job billing_event → rated_usage (a CronJob)
#   /app/phoebe-token-push  — push hourly rated_usage snapshots to the central manager
#                             for Stripe billing (a CronJob)
#   /app/phoebe-price-fetch — pull token prices from the central pricing service into
#                             the local price file the rater reads (a CronJob)
#   /app/phoebe-migrate     — golang-migrate runner: applies the embedded schema
#                             migrations to phoebe's own Postgres (a one-shot Job /
#                             init-container before the drainer)
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
# migrations/ carries the //go:embed *.sql consumed by cmd/migrate; it must be
# present in the build context or the migrate binary fails to compile/embed.
COPY migrations/ ./migrations/

# Static builds (CGO disabled) so the binaries run on a minimal base.
ENV CGO_ENABLED=0
RUN go build -o /phoebe ./cmd/interceptor && \
    go build -o /phoebe-drainer ./cmd/drainer && \
    go build -o /phoebe-rater ./cmd/rater && \
    go build -o /phoebe-price-fetch ./cmd/price-fetch && \
    go build -o /phoebe-token-push ./cmd/token-push && \
    go build -o /phoebe-migrate ./cmd/migrate

FROM alpine:latest

# ca-certificates for outbound TLS (Valkey/Postgres/upstreams over TLS).
RUN apk add --no-cache ca-certificates

COPY --from=builder /phoebe /app/phoebe
COPY --from=builder /phoebe-drainer /app/phoebe-drainer
COPY --from=builder /phoebe-rater /app/phoebe-rater
COPY --from=builder /phoebe-price-fetch /app/phoebe-price-fetch
COPY --from=builder /phoebe-token-push /app/phoebe-token-push
COPY --from=builder /phoebe-migrate /app/phoebe-migrate

# Default to the interceptor; the drainer/rater workloads override the command.
ENTRYPOINT ["/app/phoebe"]
