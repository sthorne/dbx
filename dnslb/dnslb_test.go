package dnslb

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBalancer(addrs ...string) *Balancer {
	return New(Config{
		Lookup: func(ctx context.Context, host string) ([]string, error) {
			return addrs, nil
		},
	})
}

func TestLookupPrefersLowestLatency(t *testing.T) {
	b := newTestBalancer("10.0.0.1", "10.0.0.2", "10.0.0.3")

	b.recordQuery("10.0.0.1", 30*time.Millisecond, nil)
	b.recordQuery("10.0.0.2", 5*time.Millisecond, nil)
	b.recordQuery("10.0.0.3", 90*time.Millisecond, nil)

	addrs, err := b.LookupFunc(context.Background(), "db.example.com")
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.2", "10.0.0.1", "10.0.0.3"}, addrs)
}

func TestLookupExploresUnknownServersFirst(t *testing.T) {
	b := newTestBalancer("10.0.0.1", "10.0.0.2")

	b.recordQuery("10.0.0.1", 1*time.Millisecond, nil)

	addrs, err := b.LookupFunc(context.Background(), "db.example.com")
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.2", "10.0.0.1"}, addrs)
}

func TestLookupPenalizesDialFailures(t *testing.T) {
	b := newTestBalancer("10.0.0.1", "10.0.0.2")

	// 10.0.0.1 is fast but currently failing; 10.0.0.2 is slow but up.
	b.recordQuery("10.0.0.1", 1*time.Millisecond, nil)
	b.recordQuery("10.0.0.2", 100*time.Millisecond, nil)
	b.mu.Lock()
	b.servers["10.0.0.1"].consecutiveDialFailures = 1
	b.mu.Unlock()

	addrs, err := b.LookupFunc(context.Background(), "db.example.com")
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.2", "10.0.0.1"}, addrs)
}

func TestLookupIPLiteralSkipsResolution(t *testing.T) {
	b := New(Config{
		Lookup: func(ctx context.Context, host string) ([]string, error) {
			t.Fatal("lookup should not be called for an IP literal")
			return nil, nil
		},
	})

	addrs, err := b.LookupFunc(context.Background(), "192.168.1.10")
	require.NoError(t, err)
	assert.Equal(t, []string{"192.168.1.10"}, addrs)
}

func TestLookupError(t *testing.T) {
	lookupErr := errors.New("no such host")
	b := New(Config{
		Lookup: func(ctx context.Context, host string) ([]string, error) {
			return nil, lookupErr
		},
	})

	_, err := b.LookupFunc(context.Background(), "db.example.com")
	assert.ErrorIs(t, err, lookupErr)
}

func TestDialFuncTracksConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	b := New(Config{})

	conn, err := b.DialFunc(context.Background(), "tcp", ln.Addr().String())
	require.NoError(t, err)

	metrics := b.Metrics()
	require.Len(t, metrics, 1)
	m := metrics[0]
	assert.Equal(t, "127.0.0.1", m.Server)
	assert.Equal(t, uint64(1), m.DialAttempts)
	assert.Equal(t, uint64(0), m.DialFailures)
	assert.Equal(t, int64(1), m.ActiveConns)
	assert.Equal(t, float64(100), m.Availability)
	assert.Greater(t, m.EWMALatency, time.Duration(0))

	// Close is idempotent for the active connection count.
	require.NoError(t, conn.Close())
	conn.Close()

	m = b.Metrics()[0]
	assert.Equal(t, int64(0), m.ActiveConns)
}

func TestDialFuncRecordsFailures(t *testing.T) {
	// Reserve a port, then close the listener so dialing it fails.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	b := New(Config{})

	_, err = b.DialFunc(context.Background(), "tcp", addr)
	require.Error(t, err)

	metrics := b.Metrics()
	require.Len(t, metrics, 1)
	m := metrics[0]
	assert.Equal(t, uint64(1), m.DialAttempts)
	assert.Equal(t, uint64(1), m.DialFailures)
	assert.Equal(t, uint64(1), m.ConsecutiveDialFailures)
	assert.Equal(t, float64(0), m.Availability)
	assert.NotEmpty(t, m.LastError)
	assert.False(t, m.LastFailure.IsZero())
}

func TestRecordQueryMetrics(t *testing.T) {
	b := New(Config{SlowThreshold: 50 * time.Millisecond})

	b.recordQuery("10.0.0.1", 10*time.Millisecond, nil)
	b.recordQuery("10.0.0.1", 60*time.Millisecond, nil)
	b.recordQuery("10.0.0.1", 20*time.Millisecond, errors.New("query failed"))

	metrics := b.Metrics()
	require.Len(t, metrics, 1)
	m := metrics[0]
	assert.Equal(t, uint64(3), m.Queries)
	assert.Equal(t, uint64(1), m.QueryErrors)
	assert.Equal(t, uint64(1), m.SlowQueries)
	assert.Equal(t, 30*time.Millisecond, m.AvgQueryLatency)
	assert.Equal(t, 10*time.Millisecond, m.MinQueryLatency)
	assert.Equal(t, 60*time.Millisecond, m.MaxQueryLatency)
	assert.Equal(t, "query failed", m.LastError)
	assert.False(t, m.LastSuccess.IsZero())
	assert.False(t, m.LastFailure.IsZero())
}

func TestEWMALatencySmoothing(t *testing.T) {
	b := New(Config{LatencySmoothing: 0.5})

	b.recordQuery("10.0.0.1", 10*time.Millisecond, nil)
	b.recordQuery("10.0.0.1", 20*time.Millisecond, nil)

	m := b.Metrics()[0]
	assert.Equal(t, 15*time.Millisecond, m.EWMALatency)
}

func TestReport(t *testing.T) {
	b := New(Config{})

	b.recordQuery("10.0.0.1", 10*time.Millisecond, nil)
	b.recordQuery("10.0.0.2", 20*time.Millisecond, errors.New("boom"))

	report := b.Report()
	assert.Contains(t, report, "SERVER")
	assert.Contains(t, report, "10.0.0.1")
	assert.Contains(t, report, "10.0.0.2")
	assert.Contains(t, report, "boom")
}

func TestServerKey(t *testing.T) {
	assert.Equal(t, "10.0.0.1", serverKey("tcp", "10.0.0.1:5432"))
	assert.Equal(t, "10.0.0.1", serverKey("tcp", "10.0.0.1"))
	assert.Equal(t, "::1", serverKey("tcp", "[::1]:5432"))
	assert.Equal(t, "/var/run/.s.PGSQL.5432", serverKey("unix", "/var/run/.s.PGSQL.5432"))
}

func TestReportAlignsColumns(t *testing.T) {
	b := New(Config{})
	b.recordQuery("10.0.0.1", 10*time.Millisecond, nil)

	lines := strings.Split(strings.TrimRight(b.Report(), "\n"), "\n")
	require.Len(t, lines, 2)
}
