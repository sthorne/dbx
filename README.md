[![Go Reference](https://pkg.go.dev/badge/github.com/sthorne/dbx/v5.svg)](https://pkg.go.dev/github.com/sthorne/dbx/v5)
[![Build Status](https://github.com/sthorne/dbx/actions/workflows/ci.yml/badge.svg)](https://github.com/sthorne/dbx/actions/workflows/ci.yml)

# dbx - PostgreSQL Driver and Toolkit with DNS Load Balancing

dbx is a pure Go driver and toolkit for PostgreSQL, forked from [pgx](https://github.com/jackc/pgx). The goal of the
fork is first-class DNS-based load balancing when connecting to multi-server clusters such as CockroachDB: DNS names
are expanded to the underlying server IP addresses, per-server performance and availability metrics are tracked over
time, and new connections prefer the servers with the lowest observed latency. See the `dnslb` package and the
[DNS-Based Load Balancing](#dns-based-load-balancing-and-metrics) section below.

The dbx driver is a low-level, high performance interface that exposes PostgreSQL-specific features such as `LISTEN` /
`NOTIFY` and `COPY`. It also includes an adapter for the standard `database/sql` interface.

The toolkit component is a related set of packages that implement PostgreSQL functionality such as parsing the wire protocol
and type mapping between PostgreSQL and Go. These underlying packages can be used to implement alternative drivers,
proxies, load balancers, logical replication clients, etc.

## Quick Start

### Installation

```bash
go get github.com/sthorne/dbx/v5
```

### Example Usage

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/sthorne/dbx/v5"
)

func main() {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	conn, err := dbx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	var name string
	var weight int64
	err = conn.QueryRow(context.Background(), "select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(name, weight)
}
```

### Connection Configuration

`dbx.Connect` and `pgxpool.New` accept PostgreSQL connection URLs (such as `postgres://user:pass@host:5432/db?sslmode=verify-full`) as well as `key=value` strings. See [`pgconn.ParseConfig`](https://pkg.go.dev/github.com/sthorne/dbx/v5/pgconn#ParseConfig) and the [PostgreSQL connection string documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) for supported options and environment variables.

For a step-by-step walkthrough, see the [getting started guide](https://github.com/jackc/pgx/wiki/Getting-started-with-pgx).

### DNS-Based Load Balancing and Metrics

The `dnslb` package resolves any DNS names in the connection config and expands them to the underlying IP addresses,
so a single DNS name pointing at a CockroachDB (or any multi-server PostgreSQL-compatible) cluster fans out across all
nodes. `dnslb.Pool` maintains a connection pool with standing connections to every server and cycles each operation
across them based on current activity and latency metrics: each query, batch, or transaction is routed to the server
with the lowest score — `EWMA latency × (in-use connections + 1)` plus a penalty for servers failing to accept
connections — so the fastest idle server is preferred, load spreads as servers get busy, and unavailable servers are
avoided until they recover. DNS is re-resolved in the background (every 30s by default), adding pools for servers that
join the cluster and draining pools for servers that leave, and every server is probed each cycle so latency and
availability metrics stay fresh even for idle servers.

Per-server metrics are tracked over time — availability, dial attempts and failures, active connections, query counts
and errors, slow responses, and min/max/average/EWMA latency — and are available programmatically via
`Pool.Metrics()` or as a printable table via `Pool.Report()`.

```go
pool, err := dnslb.NewPool(context.Background(),
	"postgres://root@cockroachdb.internal:26257/defaultdb",
	dnslb.PoolConfig{
		Config: dnslb.Config{
			SlowThreshold: 100 * time.Millisecond, // queries at or above this count as slow
		},
	})
if err != nil {
	// handle error
}
defer pool.Close()

// Each operation is routed to the currently best server.
rows, err := pool.Query(context.Background(), "select name, weight from widgets")

fmt.Println(pool.Report())
// SERVER     AVAIL%  DIALS  DIALERR  ACTIVE  QUERIES  ERRORS  SLOW  AVG     EWMA    MIN     MAX      LAST ERROR
// 10.0.0.11  100.0   4      0        4       1523     0       2     1.31ms  1.24ms  0.42ms  112.4ms
// 10.0.0.12  100.0   3      0        3       1102     1       0     1.52ms  1.48ms  0.51ms  48.21ms  unexpected EOF
// 10.0.0.13  50.0    2      1        1       310      0       0     9.87ms  9.61ms  4.11ms  38.05ms  dial tcp: connection refused
```

## Documentation

Package documentation and API reference are available on [pkg.go.dev](https://pkg.go.dev/github.com/sthorne/dbx/v5):
* [`pgx`](https://pkg.go.dev/github.com/sthorne/dbx/v5) — base PostgreSQL driver
* [`pgxpool`](https://pkg.go.dev/github.com/sthorne/dbx/v5/pgxpool) — concurrency-safe connection pool
* [`stdlib`](https://pkg.go.dev/github.com/sthorne/dbx/v5/stdlib) — `database/sql` compatibility adapter

## Features

* Support for approximately 70 different PostgreSQL types
* Automatic statement preparation and caching
* Batch queries
* Single-round trip query mode
* Full TLS connection control
* Binary format support for custom types (allows for much quicker encoding/decoding)
* `COPY` protocol support for faster bulk data loads
* Tracing and logging support
* Connection pool with after-connect hook for arbitrary connection setup
* `LISTEN` / `NOTIFY`
* Conversion of PostgreSQL arrays to Go slice mappings for integers, floats, and strings
* `hstore` support
* `json` and `jsonb` support
* Maps `inet` and `cidr` PostgreSQL types to `netip.Addr` and `netip.Prefix`
* Large object support
* NULL mapping to pointer to pointer
* Supports `database/sql.Scanner` and `database/sql/driver.Valuer` interfaces for custom types
* Notice response handling
* Simulated nested transactions with savepoints

## Choosing Between the pgx and database/sql Interfaces

The pgx interface is faster. Many PostgreSQL specific features such as `LISTEN` / `NOTIFY` and `COPY` are not available
through the `database/sql` interface.

The pgx interface is recommended when:

1. The application only targets PostgreSQL.
2. No other libraries that require `database/sql` are in use.

It is also possible to use the `database/sql` interface and convert a connection to the lower-level pgx interface as needed.

## Testing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for setup instructions.

## Architecture

See the presentation at Golang Estonia, [PGX Top to Bottom](https://www.youtube.com/watch?v=sXMSWhcHCf8) for a description of pgx architecture.

## Supported Go and PostgreSQL Versions

pgx supports the same versions of Go and PostgreSQL that are supported by their respective teams. For [Go](https://golang.org/doc/devel/release.html#policy) that is the two most recent major releases and for [PostgreSQL](https://www.postgresql.org/support/versioning/) the major releases in the last 5 years. This means pgx supports Go 1.25 and higher and PostgreSQL 14 and higher. pgx also is tested against the latest version of [CockroachDB](https://www.cockroachlabs.com/product/).

## Version Policy

pgx follows semantic versioning for the documented public API on stable releases. `v5` is the latest stable major version.

## PGX Family Libraries

### [github.com/jackc/pglogrepl](https://github.com/jackc/pglogrepl)

pglogrepl provides functionality to act as a client for PostgreSQL logical replication.

### [github.com/jackc/pgmock](https://github.com/jackc/pgmock)

pgmock offers the ability to create a server that mocks the PostgreSQL wire protocol. This is used internally to test pgx by purposely inducing unusual errors. pgproto3 and pgmock together provide most of the foundational tooling required to implement a PostgreSQL proxy or MitM (such as for a custom connection pooler).

### [github.com/jackc/tern](https://github.com/jackc/tern)

tern is a stand-alone SQL migration system.

### [github.com/jackc/pgerrcode](https://github.com/jackc/pgerrcode)

pgerrcode contains constants for the PostgreSQL error codes.

## Adapters for 3rd Party Types

* [github.com/jackc/pgx-gofrs-uuid](https://github.com/jackc/pgx-gofrs-uuid)
* [github.com/jackc/pgx-shopspring-decimal](https://github.com/jackc/pgx-shopspring-decimal)
* [github.com/ColeBurch/pgx-govalues-decimal](https://github.com/ColeBurch/pgx-govalues-decimal)
* [github.com/twpayne/pgx-geos](https://github.com/twpayne/pgx-geos) ([PostGIS](https://postgis.net/) and [GEOS](https://libgeos.org/) via [go-geos](https://github.com/twpayne/go-geos))
* [github.com/vgarvardt/pgx-google-uuid](https://github.com/vgarvardt/pgx-google-uuid)


## Adapters for 3rd Party Tracers

* [github.com/jackhopner/pgx-xray-tracer](https://github.com/jackhopner/pgx-xray-tracer)
* [github.com/exaring/otelpgx](https://github.com/exaring/otelpgx)

## Adapters for 3rd Party Loggers

These adapters can be used with the tracelog package.

* [github.com/jackc/pgx-go-kit-log](https://github.com/jackc/pgx-go-kit-log)
* [github.com/jackc/pgx-log15](https://github.com/jackc/pgx-log15)
* [github.com/jackc/pgx-logrus](https://github.com/jackc/pgx-logrus)
* [github.com/jackc/pgx-zap](https://github.com/jackc/pgx-zap)
* [github.com/jackc/pgx-zerolog](https://github.com/jackc/pgx-zerolog)
* [github.com/mcosta74/pgx-slog](https://github.com/mcosta74/pgx-slog)
* [github.com/kataras/pgx-golog](https://github.com/kataras/pgx-golog)

## 3rd Party Libraries with PGX Support

### [github.com/pashagolub/pgxmock](https://github.com/pashagolub/pgxmock)

pgxmock is a mock library implementing pgx interfaces.
pgxmock has one and only purpose - to simulate pgx behavior in tests, without needing a real database connection.

### [github.com/georgysavva/scany](https://github.com/georgysavva/scany)

Library for scanning data from a database into Go structs and more.

### [github.com/vingarcia/ksql](https://github.com/vingarcia/ksql)

A carefully designed SQL client for making using SQL easier,
more productive, and less error-prone on Golang.

### [github.com/otan/gopgkrb5](https://github.com/otan/gopgkrb5)

Adds GSSAPI / Kerberos authentication support.

### [github.com/wcamarao/pmx](https://github.com/wcamarao/pmx)

Explicit data mapping and scanning library for Go structs and slices.

### [github.com/stephenafamo/scan](https://github.com/stephenafamo/scan)

Type safe and flexible package for scanning database data into Go types.
Supports, structs, maps, slices and custom mapping functions.

### [github.com/z0ne-dev/mgx](https://github.com/z0ne-dev/mgx)

Code first migration library for native pgx (no database/sql abstraction).

### [github.com/amirsalarsafaei/sqlc-pgx-monitoring](https://github.com/amirsalarsafaei/sqlc-pgx-monitoring)

A database monitoring/metrics library for pgx and sqlc. Trace, log and monitor your sqlc query performance using OpenTelemetry.

### [https://github.com/nikolayk812/pgx-outbox](https://github.com/nikolayk812/pgx-outbox)

Simple Golang implementation for transactional outbox pattern for PostgreSQL using jackc/pgx driver.

### [https://github.com/Arlandaren/pgxWrappy](https://github.com/Arlandaren/pgxWrappy)

Simplifies working with the pgx library, providing convenient scanning of nested structures.

### [https://github.com/KoNekoD/pgx-colon-query-rewriter](https://github.com/KoNekoD/pgx-colon-query-rewriter)

Implementation of the pgx query rewriter to use ':' instead of '@' in named query parameters.
