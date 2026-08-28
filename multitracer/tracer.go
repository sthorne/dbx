// Package multitracer provides a Tracer that can combine several tracers into one.
package multitracer

import (
	"context"

	"github.com/sthorne/dbx/v5"
	"github.com/sthorne/dbx/v5/pgxpool"
)

// Tracer can combine several tracers into one.
// You can use New to automatically split tracers by interface.
type Tracer struct {
	QueryTracers       []dbx.QueryTracer
	BatchTracers       []dbx.BatchTracer
	CopyFromTracers    []dbx.CopyFromTracer
	PrepareTracers     []dbx.PrepareTracer
	ConnectTracers     []dbx.ConnectTracer
	PoolAcquireTracers []pgxpool.AcquireTracer
	PoolReleaseTracers []pgxpool.ReleaseTracer
}

// New returns new Tracer from tracers with automatically split tracers by interface.
func New(tracers ...dbx.QueryTracer) *Tracer {
	var t Tracer

	for _, tracer := range tracers {
		t.QueryTracers = append(t.QueryTracers, tracer)

		if batchTracer, ok := tracer.(dbx.BatchTracer); ok {
			t.BatchTracers = append(t.BatchTracers, batchTracer)
		}

		if copyFromTracer, ok := tracer.(dbx.CopyFromTracer); ok {
			t.CopyFromTracers = append(t.CopyFromTracers, copyFromTracer)
		}

		if prepareTracer, ok := tracer.(dbx.PrepareTracer); ok {
			t.PrepareTracers = append(t.PrepareTracers, prepareTracer)
		}

		if connectTracer, ok := tracer.(dbx.ConnectTracer); ok {
			t.ConnectTracers = append(t.ConnectTracers, connectTracer)
		}

		if poolAcquireTracer, ok := tracer.(pgxpool.AcquireTracer); ok {
			t.PoolAcquireTracers = append(t.PoolAcquireTracers, poolAcquireTracer)
		}

		if poolReleaseTracer, ok := tracer.(pgxpool.ReleaseTracer); ok {
			t.PoolReleaseTracers = append(t.PoolReleaseTracers, poolReleaseTracer)
		}
	}

	return &t
}

func (t *Tracer) TraceQueryStart(ctx context.Context, conn *dbx.Conn, data dbx.TraceQueryStartData) context.Context {
	for _, tracer := range t.QueryTracers {
		ctx = tracer.TraceQueryStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceQueryEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceQueryEndData) {
	for _, tracer := range t.QueryTracers {
		tracer.TraceQueryEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceBatchStart(ctx context.Context, conn *dbx.Conn, data dbx.TraceBatchStartData) context.Context {
	for _, tracer := range t.BatchTracers {
		ctx = tracer.TraceBatchStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *dbx.Conn, data dbx.TraceBatchQueryData) {
	for _, tracer := range t.BatchTracers {
		tracer.TraceBatchQuery(ctx, conn, data)
	}
}

func (t *Tracer) TraceBatchEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceBatchEndData) {
	for _, tracer := range t.BatchTracers {
		tracer.TraceBatchEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceCopyFromStart(ctx context.Context, conn *dbx.Conn, data dbx.TraceCopyFromStartData) context.Context {
	for _, tracer := range t.CopyFromTracers {
		ctx = tracer.TraceCopyFromStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TraceCopyFromEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceCopyFromEndData) {
	for _, tracer := range t.CopyFromTracers {
		tracer.TraceCopyFromEnd(ctx, conn, data)
	}
}

func (t *Tracer) TracePrepareStart(ctx context.Context, conn *dbx.Conn, data dbx.TracePrepareStartData) context.Context {
	for _, tracer := range t.PrepareTracers {
		ctx = tracer.TracePrepareStart(ctx, conn, data)
	}

	return ctx
}

func (t *Tracer) TracePrepareEnd(ctx context.Context, conn *dbx.Conn, data dbx.TracePrepareEndData) {
	for _, tracer := range t.PrepareTracers {
		tracer.TracePrepareEnd(ctx, conn, data)
	}
}

func (t *Tracer) TraceConnectStart(ctx context.Context, data dbx.TraceConnectStartData) context.Context {
	for _, tracer := range t.ConnectTracers {
		ctx = tracer.TraceConnectStart(ctx, data)
	}

	return ctx
}

func (t *Tracer) TraceConnectEnd(ctx context.Context, data dbx.TraceConnectEndData) {
	for _, tracer := range t.ConnectTracers {
		tracer.TraceConnectEnd(ctx, data)
	}
}

func (t *Tracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	for _, tracer := range t.PoolAcquireTracers {
		ctx = tracer.TraceAcquireStart(ctx, pool, data)
	}

	return ctx
}

func (t *Tracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	for _, tracer := range t.PoolAcquireTracers {
		tracer.TraceAcquireEnd(ctx, pool, data)
	}
}

func (t *Tracer) TraceRelease(pool *pgxpool.Pool, data pgxpool.TraceReleaseData) {
	for _, tracer := range t.PoolReleaseTracers {
		tracer.TraceRelease(pool, data)
	}
}
