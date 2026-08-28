# This fork and datax

This repository is a fork of [jackc/pgx](https://github.com/jackc/pgx), kept
as the blessed Go driver for [datax](https://github.com/sthorne/datax) — an
open-source distributed ACID SQL database that speaks the PostgreSQL wire
protocol.

There is currently **no divergence from upstream pgx**: datax's server uses
the published `github.com/jackc/pgx/v5` module directly (its `pgproto3`
package provides the server-side wire protocol implementation, and `pgtype`
the value encoding), and any released pgx works as a datax client out of the
box:

```go
conn, err := pgx.Connect(ctx, "postgres://root@localhost:26433/datax?sslmode=disable")
```

This fork exists as the home for future datax-specific driver features
(e.g. transaction-retry helpers keyed on datax's SQLSTATE 40001 semantics,
or datax-specific error metadata) if and when they diverge from upstream.
