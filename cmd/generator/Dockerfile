# Built from the repo root. confluent-kafka-go requires CGO + librdkafka, so we
# build with -tags musl and statically link against alpine's musl libc.
FROM --platform=linux/amd64 golang:1.25-alpine AS builder
RUN apk add --no-progress --no-cache gcc musl-dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN go build -tags musl -ldflags '-extldflags "-static"' -o /out/generator ./cmd/generator

FROM alpine:3.20
WORKDIR /app
COPY assets/kibana/exports.ndjson /app/exports.ndjson
COPY --from=builder /out/generator /app/generator
ENTRYPOINT ["/app/generator"]
