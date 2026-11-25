# API entry point

Place the HTTP server's thin `main.go` here when transport and storage adapters
are available. It should load configuration, wire dependencies, start the
server, and handle shutdown. Keep request handling and workspace rules in
domain packages under `internal/`.

There is no API executable yet. The current implementation is
[`internal/workspace`](../../internal/workspace).
