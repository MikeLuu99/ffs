FROM golang:1.26-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY test/ ./test/
RUN go test -race ./... && go vet ./...
RUN CGO_ENABLED=0 go build -trimpath -o /out/dfs ./cmd/dfs

FROM build AS test
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 util-linux \
    && rm -rf /var/lib/apt/lists/*
CMD ["go", "test", "-race", "-timeout=90s", "./..."]

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends fuse3 util-linux ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/dfs /usr/local/bin/dfs
RUN mkdir -p /app/data /app/mnt

ENTRYPOINT ["dfs"]
