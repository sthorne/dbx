package dnslb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestPool creates a Pool that resolves db.example.com to the given
// addresses, with background resolution, probes, and standing connections
// disabled so no network activity occurs.
func newTestPool(t *testing.T, addrs func() []string) *Pool {
	t.Helper()

	p, err := NewPool(context.Background(), "postgres://user@db.example.com:26257/testdb?sslmode=disable", PoolConfig{
		Config: Config{
			Lookup: func(ctx context.Context, host string) ([]string, error) {
				return addrs(), nil
			},
		},
		ResolveInterval:   -1,
		MinConnsPerServer: -1,
		DisableProbes:     true,
	})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

func staticAddrs(addrs ...string) func() []string {
	return func() []string { return addrs }
}

func TestPoolResolvesAllServers(t *testing.T) {
	p := newTestPool(t, staticAddrs("10.0.0.1", "10.0.0.2", "10.0.0.3"))
	assert.Equal(t, []string{"10.0.0.1:26257", "10.0.0.2:26257", "10.0.0.3:26257"}, p.Servers())
}

func TestPoolPickPrefersLowestLatency(t *testing.T) {
	p := newTestPool(t, staticAddrs("10.0.0.1", "10.0.0.2"))

	p.Balancer().recordQuery("10.0.0.1", 50*time.Millisecond, nil)
	p.Balancer().recordQuery("10.0.0.2", 5*time.Millisecond, nil)

	for i := 0; i < 5; i++ {
		sp, err := p.pickServer()
		require.NoError(t, err)
		assert.Equal(t, "10.0.0.2:26257", sp.key)
	}
}

func TestPoolPickCyclesEqualServers(t *testing.T) {
	p := newTestPool(t, staticAddrs("10.0.0.1", "10.0.0.2"))

	// No latency history: both score zero, so picks rotate round-robin.
	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		sp, err := p.pickServer()
		require.NoError(t, err)
		seen[sp.key]++
	}
	assert.Equal(t, 3, seen["10.0.0.1:26257"])
	assert.Equal(t, 3, seen["10.0.0.2:26257"])
}

func TestPoolPickAvoidsFailingServer(t *testing.T) {
	p := newTestPool(t, staticAddrs("10.0.0.1", "10.0.0.2"))

	// 10.0.0.1 is faster but failing to accept connections.
	b := p.Balancer()
	b.recordQuery("10.0.0.1", 1*time.Millisecond, nil)
	b.recordQuery("10.0.0.2", 100*time.Millisecond, nil)
	b.mu.Lock()
	b.servers["10.0.0.1"].consecutiveDialFailures = 1
	b.mu.Unlock()

	sp, err := p.pickServer()
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2:26257", sp.key)
}

func TestPoolRefreshAddsAndRemovesServers(t *testing.T) {
	var mu sync.Mutex
	addrs := []string{"10.0.0.1", "10.0.0.2"}
	p := newTestPool(t, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return addrs
	})
	require.Equal(t, []string{"10.0.0.1:26257", "10.0.0.2:26257"}, p.Servers())

	// A node leaves and another joins the cluster.
	mu.Lock()
	addrs = []string{"10.0.0.2", "10.0.0.3"}
	mu.Unlock()

	require.NoError(t, p.Refresh(context.Background()))
	assert.Equal(t, []string{"10.0.0.2:26257", "10.0.0.3:26257"}, p.Servers())
}

func TestPoolRefreshKeepsServersOnLookupError(t *testing.T) {
	var mu sync.Mutex
	fail := false
	p, err := NewPool(context.Background(), "postgres://user@db.example.com/testdb?sslmode=disable", PoolConfig{
		Config: Config{
			Lookup: func(ctx context.Context, host string) ([]string, error) {
				mu.Lock()
				defer mu.Unlock()
				if fail {
					return nil, assert.AnError
				}
				return []string{"10.0.0.1"}, nil
			},
		},
		ResolveInterval:   -1,
		MinConnsPerServer: -1,
		DisableProbes:     true,
	})
	require.NoError(t, err)
	t.Cleanup(p.Close)

	mu.Lock()
	fail = true
	mu.Unlock()

	assert.Error(t, p.Refresh(context.Background()))
	assert.Equal(t, []string{"10.0.0.1:5432"}, p.Servers(), "servers must survive a transient DNS failure")
}

func TestPoolNewFailsWhenNothingResolves(t *testing.T) {
	_, err := NewPool(context.Background(), "postgres://user@db.example.com/testdb?sslmode=disable", PoolConfig{
		Config: Config{
			Lookup: func(ctx context.Context, host string) ([]string, error) {
				return nil, assert.AnError
			},
		},
		ResolveInterval:   -1,
		MinConnsPerServer: -1,
	})
	assert.Error(t, err)
}

func TestPoolIPLiteralHost(t *testing.T) {
	p, err := NewPool(context.Background(), "postgres://user@192.168.1.10:26257/testdb?sslmode=disable", PoolConfig{
		Config: Config{
			Lookup: func(ctx context.Context, host string) ([]string, error) {
				t.Fatal("lookup should not be called for an IP literal")
				return nil, nil
			},
		},
		ResolveInterval:   -1,
		MinConnsPerServer: -1,
		DisableProbes:     true,
	})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	assert.Equal(t, []string{"192.168.1.10:26257"}, p.Servers())
}

func TestPoolClosedErrors(t *testing.T) {
	p := newTestPool(t, staticAddrs("10.0.0.1"))
	p.Close()

	_, err := p.pickServer()
	assert.ErrorIs(t, err, ErrPoolClosed)

	err = p.QueryRow(context.Background(), "select 1").Scan()
	assert.ErrorIs(t, err, ErrPoolClosed)

	results := p.SendBatch(context.Background(), nil)
	assert.ErrorIs(t, results.Close(), ErrPoolClosed)

	assert.ErrorIs(t, p.Refresh(context.Background()), ErrPoolClosed)

	// Close is idempotent.
	p.Close()
}

func TestPoolMultipleHosts(t *testing.T) {
	p, err := NewPool(context.Background(), "host=a.example.com,b.example.com port=26257 user=u dbname=d sslmode=disable", PoolConfig{
		Config: Config{
			Lookup: func(ctx context.Context, host string) ([]string, error) {
				switch host {
				case "a.example.com":
					return []string{"10.0.1.1"}, nil
				case "b.example.com":
					return []string{"10.0.2.1", "10.0.2.2"}, nil
				}
				return nil, assert.AnError
			},
		},
		ResolveInterval:   -1,
		MinConnsPerServer: -1,
		DisableProbes:     true,
	})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	assert.Equal(t, []string{"10.0.1.1:26257", "10.0.2.1:26257", "10.0.2.2:26257"}, p.Servers())
}
