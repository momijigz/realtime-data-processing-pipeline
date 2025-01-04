# FROM alpine:latest

# RUN wget https://dl.google.com/go/go1.21.5.linux-amd64.tar.gz
# RUN tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
# RUN export PATH=$PATH:/usr/local/go/bin
# COPY --from=golang:1.21.5-alpine /usr/local/go/ /usr/local/go/

# ENV PATH="/usr/local/go/bin:${PATH}"
# RUN apk update
# RUN echo go version
# RUN apk add librdkafka-dev
# # RUN apk add build-essential 

# RUN apk --no-cache --update add build-base 
# # RUN apk add pkg-config 
# RUN apk add pkgconfig

# WORKDIR /app
# COPY go.mod go.sum exports.ndjson ./

# RUN go mod download


# COPY *.go ./

# RUN CGO_ENABLED=0 GOOS=linux go build -o /client-app

# EXPOSE 8084

# CMD ["/client-app -limit 10000"]


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