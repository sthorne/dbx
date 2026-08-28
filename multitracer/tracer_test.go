package multitracer_test

import (
	"context"
	"testing"

	"github.com/sthorne/dbx/v5"
	"github.com/sthorne/dbx/v5/multitracer"
	"github.com/sthorne/dbx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type testFullTracer struct{}

func (tt *testFullTracer) TraceQueryStart(ctx context.Context, conn *dbx.Conn, data dbx.TraceQueryStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceQueryEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceQueryEndData) {
}

func (tt *testFullTracer) TraceBatchStart(ctx context.Context, conn *dbx.Conn, data dbx.TraceBatchStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceBatchQuery(ctx context.Context, conn *dbx.Conn, data dbx.TraceBatchQueryData) {
}

func (tt *testFullTracer) TraceBatchEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceBatchEndData) {
}

func (tt *testFullTracer) TraceCopyFromStart(ctx context.Context, conn *dbx.Conn, data dbx.TraceCopyFromStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceCopyFromEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceCopyFromEndData) {
}

func (tt *testFullTracer) TracePrepareStart(ctx context.Context, conn *dbx.Conn, data dbx.TracePrepareStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TracePrepareEnd(ctx context.Context, conn *dbx.Conn, data dbx.TracePrepareEndData) {
}

func (tt *testFullTracer) TraceConnectStart(ctx context.Context, data dbx.TraceConnectStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceConnectEnd(ctx context.Context, data dbx.TraceConnectEndData) {
}

func (tt *testFullTracer) TraceAcquireStart(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	return ctx
}

func (tt *testFullTracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
}

func (tt *testFullTracer) TraceRelease(pool *pgxpool.Pool, data pgxpool.TraceReleaseData) {
}

type testCopyTracer struct{}

func (tt *testCopyTracer) TraceQueryStart(ctx context.Context, conn *dbx.Conn, data dbx.TraceQueryStartData) context.Context {
	return ctx
}

func (tt *testCopyTracer) TraceQueryEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceQueryEndData) {
}

func (tt *testCopyTracer) TraceCopyFromStart(ctx context.Context, conn *dbx.Conn, data dbx.TraceCopyFromStartData) context.Context {
	return ctx
}

func (tt *testCopyTracer) TraceCopyFromEnd(ctx context.Context, conn *dbx.Conn, data dbx.TraceCopyFromEndData) {
}

func TestNew(t *testing.T) {
	t.Parallel()

	fullTracer := &testFullTracer{}
	copyTracer := &testCopyTracer{}

	mt := multitracer.New(fullTracer, copyTracer)
	require.Equal(
		t,
		&multitracer.Tracer{
			QueryTracers: []dbx.QueryTracer{
				fullTracer,
				copyTracer,
			},
			BatchTracers: []dbx.BatchTracer{
				fullTracer,
			},
			CopyFromTracers: []dbx.CopyFromTracer{
				fullTracer,
				copyTracer,
			},
			PrepareTracers: []dbx.PrepareTracer{
				fullTracer,
			},
			ConnectTracers: []dbx.ConnectTracer{
				fullTracer,
			},
			PoolAcquireTracers: []pgxpool.AcquireTracer{
				fullTracer,
			},
			PoolReleaseTracers: []pgxpool.ReleaseTracer{
				fullTracer,
			},
		},
		mt,
	)
}
