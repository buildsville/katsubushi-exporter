FROM golang:alpine AS builder

RUN apk update && \
    apk add git build-base && \
    rm -rf /var/cache/apk/* && \
    mkdir -p "$GOPATH/src/github.com/buildsville/" && \
    git clone https://github.com/buildsville/katsubushi-exporter.git && \
    mv katsubushi-exporter "$GOPATH/src/github.com/buildsville/" && \
    cd "$GOPATH/src/github.com/buildsville/katsubushi-exporter" && \
    GOOS=linux GOARCH=amd64 go build -o /katsubushi-exporter

FROM alpine:3.7

RUN apk add --update ca-certificates

COPY --from=builder /katsubushi-exporter /katsubushi-exporter

ENTRYPOINT ["/katsubushi-exporter"]
