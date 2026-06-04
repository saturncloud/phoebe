FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go build -o /phoebe ./cmd/interceptor

FROM alpine:latest

COPY --from=builder /phoebe /app/phoebe

ENTRYPOINT ["/app/phoebe"]
