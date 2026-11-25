FROM golang:1.26-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY test/ ./test/
RUN go test -race ./... && go vet ./...
RUN CGO_ENABLED=0 go build -trimpath -o /out/singlefs ./cmd/singlefs

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 util-linux \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/singlefs /usr/local/bin/singlefs
RUN mkdir -p /app/data /app/mnt

ENTRYPOINT ["singlefs"]
