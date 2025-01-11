FROM --platform=linux/amd64  golang:1.22.6-alpine3.20 AS builder
RUN apk add --no-progress --no-cache gcc musl-dev
WORKDIR /build
COPY . .
RUN go mod download

RUN go build -tags musl -ldflags '-extldflags "-static"' -o /build/main

FROM scratch
WORKDIR /app
COPY exports.ndjson .
COPY --from=builder /build/main .
ENTRYPOINT ["/app/main"]