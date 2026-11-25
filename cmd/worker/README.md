# Worker entry point

Place the background worker's thin `main.go` here when the outbox and indexing
adapters are available. It should load configuration, wire dependencies, start
jobs, and handle cancellation. Keep job behavior in its owning domain under
`internal/`.

There is no worker executable yet; the domain currently defines the outbox and
search contracts.
