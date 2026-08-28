package dnslb

import (
	"context"
	"time"

	"github.com/sthorne/dbx/v5"
	"github.com/sthorne/dbx/v5/multitracer"
)

// Apply wires the Balancer into config: DNS names are expanded to IPs and
// ordered by latency (LookupFunc), dials are timed and tracked (DialFunc),
// and query latency, errors, and slow responses are recorded per server
// (Tracer). An existing tracer on config is preserved by composing it with
// the Balancer's tracer.
func (b *Balancer) Apply(config *dbx.ConnConfig) {
	config.LookupFunc = b.LookupFunc
	config.DialFunc = b.DialFunc
	if config.Tracer != nil {
		config.Tracer = multitracer.New(config.Tracer, b.Tracer())
	} else {
		config.Tracer = b.Tracer()
	}
}

// Tracer returns a dbx.QueryTracer that records query latency, errors, and
// slow responses against the server each query ran on.
func (b *Balancer) Tracer() dbx.QueryTracer {
	return &queryTracer{b: b}
}

type queryTracer struct {
	b *Balancer
}

type ctxKey int

const queryStartKey ctxKey = iota

func (t *queryTracer) TraceQueryStart(ctx context.Context, _ *dbx.Conn, _ dbx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, queryStartKey, time.Now())
}

func (t *queryTracer) TraceQueryEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceQueryEndData) {
	start, ok := ctx.Value(queryStartKey).(time.Time)
	if !ok {
		return
	}
	key := connServerKey(conn)
	if key == "" {
		return
	}
	t.b.recordQuery(key, time.Since(start), data.Err)
}

// connServerKey returns the metrics key for the server conn is connected to,
// or "" if it cannot be determined.
func connServerKey(conn *dbx.Conn) string {
	if conn == nil {
		return ""
	}
	pgConn := conn.PgConn()
	if pgConn == nil {
		return ""
	}
	netConn := pgConn.Conn()
	if netConn == nil {
		return ""
	}
	remoteAddr := netConn.RemoteAddr()
	if remoteAddr == nil {
		return ""
	}
	return serverKey(remoteAddr.Network(), remoteAddr.String())
}
