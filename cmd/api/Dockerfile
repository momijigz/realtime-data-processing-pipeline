# Built from the repo root. The api now imports internal/generator which uses
# confluent-kafka-go (CGO + librdkafka), so we add gcc/musl-dev to the builder
# and statically link against musl libc.
FROM golang:1.25-alpine AS builder
RUN apk add --no-progress --no-cache gcc musl-dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go build -tags musl -ldflags '-extldflags "-static"' -o /out/api ./cmd/api

FROM alpine:3.20
WORKDIR /app
# The api needs the Kibana saved-objects bundle to bootstrap dashboards.
COPY assets/kibana/exports.ndjson /app/exports.ndjson
COPY --from=builder /out/api /app/api
EXPOSE 8090
ENTRYPOINT ["/app/api"]
