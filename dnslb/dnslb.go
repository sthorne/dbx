// Package dnslb provides DNS-based load balancing for dbx when connecting to
// clusters that publish multiple servers behind a single DNS name, such as a
// CockroachDB cluster.
//
// [Pool] is the primary API. It expands the DNS names in the connection
// config to the underlying server IP addresses, maintains a connection pool
// with standing connections to every server, and routes each operation to a
// server chosen from current activity and latency metrics: the
// lowest-latency idle server is preferred, load spreads as servers get busy,
// and servers failing to accept connections are avoided until they recover.
// DNS is re-resolved in the background so servers joining or leaving the
// cluster are picked up automatically.
//
//	pool, err := dnslb.NewPool(ctx, "postgres://root@cockroachdb.internal:26257/defaultdb", dnslb.PoolConfig{})
//	if err != nil { ... }
//	defer pool.Close()
//
//	rows, err := pool.Query(ctx, "select ...") // routed to the best server
//
//	fmt.Println(pool.Report()) // printable per-server metrics
//
// Per-server metrics are tracked over time: connection attempts and
// failures, availability, active connections, query counts and errors, slow
// responses, and latency (min/max/average and an exponentially weighted
// moving average). Metrics are available programmatically via [Pool.Metrics]
// and as a printable table via [Pool.Report].
//
// [Balancer] is the lower-level building block: it tracks the metrics and
// can be wired directly into a single dbx or pgxpool config via
// [Balancer.Apply], which orders connection attempts by latency without
// maintaining per-server pools.
package dnslb

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// Defaults used by New when the corresponding Config field is zero.
const (
	DefaultSlowThreshold    = 250 * time.Millisecond
	DefaultLatencySmoothing = 0.2
	DefaultFailurePenalty   = 30 * time.Second
)

// Config configures a Balancer. The zero value is valid; New fills in
// defaults for any zero fields.
type Config struct {
	// Lookup resolves a host name to IP addresses. Defaults to
	// net.DefaultResolver.LookupHost.
	Lookup func(ctx context.Context, host string) ([]string, error)

	// Dialer establishes network connections. Defaults to a zero net.Dialer.
	Dialer *net.Dialer

	// SlowThreshold is the duration at or above which a query is counted as a
	// slow response. Defaults to DefaultSlowThreshold.
	SlowThreshold time.Duration

	// LatencySmoothing is the weight in (0, 1] given to the most recent
	// observation in the exponentially weighted moving average latency used
	// to order servers. Defaults to DefaultLatencySmoothing.
	LatencySmoothing float64

	// FailurePenalty is added to a server's ordering score for each
	// consecutive dial failure, pushing unavailable servers to the back until
	// a dial succeeds again. Defaults to DefaultFailurePenalty.
	FailurePenalty time.Duration
}

// Balancer resolves DNS names to the underlying server IP addresses, prefers
// the lowest-latency servers, and records per-server metrics. It is safe for
// concurrent use and is intended to be shared by all connections of a pool.
type Balancer struct {
	lookup         func(ctx context.Context, host string) ([]string, error)
	dialer         *net.Dialer
	slowThreshold  time.Duration
	alpha          float64
	failurePenalty time.Duration

	mu      sync.Mutex
	servers map[string]*serverStats
}

// New creates a Balancer, applying defaults for any zero Config fields.
func New(cfg Config) *Balancer {
	if cfg.Lookup == nil {
		cfg.Lookup = net.DefaultResolver.LookupHost
	}
	if cfg.Dialer == nil {
		cfg.Dialer = &net.Dialer{}
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = DefaultSlowThreshold
	}
	if cfg.LatencySmoothing == 0 {
		cfg.LatencySmoothing = DefaultLatencySmoothing
	}
	if cfg.FailurePenalty == 0 {
		cfg.FailurePenalty = DefaultFailurePenalty
	}

	return &Balancer{
		lookup:         cfg.Lookup,
		dialer:         cfg.Dialer,
		slowThreshold:  cfg.SlowThreshold,
		alpha:          cfg.LatencySmoothing,
		failurePenalty: cfg.FailurePenalty,
		servers:        map[string]*serverStats{},
	}
}

type serverStats struct {
	server string

	dialAttempts            uint64
	dialFailures            uint64
	consecutiveDialFailures uint64
	activeConns             int64

	queries     uint64
	queryErrors uint64
	slowQueries uint64

	totalQueryLatency time.Duration
	minQueryLatency   time.Duration
	maxQueryLatency   time.Duration

	// ewmaLatencyNs blends dial and query latency observations and is the
	// primary input to the ordering score.
	ewmaLatencyNs float64
	haveLatency   bool

	firstSeen   time.Time
	lastSuccess time.Time
	lastFailure time.Time
	lastError   string
}

// LookupFunc resolves host to the underlying IP addresses and returns them
// ordered by preference: lowest observed latency first, never-tried servers
// before servers with known latency, and servers with consecutive dial
// failures last. Assign it to pgconn.Config.LookupFunc (Apply does this),
// which dbx consults for every new connection attempt.
func (b *Balancer) LookupFunc(ctx context.Context, host string) ([]string, error) {
	var addrs []string
	if net.ParseIP(host) != nil {
		addrs = []string{host}
	} else {
		var err error
		addrs, err = b.lookup(ctx, host)
		if err != nil {
			return nil, err
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	scores := make(map[string]float64, len(addrs))
	for _, addr := range addrs {
		key := hostOnly(addr)
		b.serverLocked(key, now)
		scores[addr] = b.scoreLocked(key)
	}
	sort.SliceStable(addrs, func(i, j int) bool {
		return scores[addrs[i]] < scores[addrs[j]]
	})

	return addrs, nil
}

// scoreLocked returns the ordering score for a server; lower is preferred. A
// server with no latency history scores zero so that it is explored first.
func (b *Balancer) scoreLocked(key string) float64 {
	s := b.servers[key]
	if s == nil {
		return 0
	}
	return s.ewmaLatencyNs + float64(s.consecutiveDialFailures)*float64(b.failurePenalty)
}

// DialFunc establishes a connection to addr while recording dial latency,
// availability, and active connection counts for the server. Assign it to
// pgconn.Config.DialFunc (Apply does this).
func (b *Balancer) DialFunc(ctx context.Context, network, addr string) (net.Conn, error) {
	key := serverKey(network, addr)

	start := time.Now()
	conn, err := b.dialer.DialContext(ctx, network, addr)
	elapsed := time.Since(start)

	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.serverLocked(key, start)
	s.dialAttempts++
	if err != nil {
		s.dialFailures++
		s.consecutiveDialFailures++
		s.lastFailure = time.Now()
		s.lastError = err.Error()
		return nil, err
	}

	s.consecutiveDialFailures = 0
	s.lastSuccess = time.Now()
	s.activeConns++
	b.observeLatencyLocked(s, elapsed)

	return &trackedConn{Conn: conn, release: func() { b.connClosed(key) }}, nil
}

func (b *Balancer) connClosed(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := b.servers[key]; s != nil && s.activeConns > 0 {
		s.activeConns--
	}
}

// recordQuery records the outcome of a single query against a server.
func (b *Balancer) recordQuery(key string, elapsed time.Duration, queryErr error) {
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.serverLocked(key, now)
	s.queries++
	s.totalQueryLatency += elapsed
	if s.queries == 1 || elapsed < s.minQueryLatency {
		s.minQueryLatency = elapsed
	}
	if elapsed > s.maxQueryLatency {
		s.maxQueryLatency = elapsed
	}
	if elapsed >= b.slowThreshold {
		s.slowQueries++
	}
	b.observeLatencyLocked(s, elapsed)

	if queryErr != nil {
		s.queryErrors++
		s.lastFailure = now
		s.lastError = queryErr.Error()
	} else {
		s.lastSuccess = now
	}
}

func (b *Balancer) observeLatencyLocked(s *serverStats, elapsed time.Duration) {
	if !s.haveLatency {
		s.ewmaLatencyNs = float64(elapsed)
		s.haveLatency = true
		return
	}
	s.ewmaLatencyNs = b.alpha*float64(elapsed) + (1-b.alpha)*s.ewmaLatencyNs
}

func (b *Balancer) serverLocked(key string, now time.Time) *serverStats {
	s := b.servers[key]
	if s == nil {
		s = &serverStats{server: key, firstSeen: now}
		b.servers[key] = s
	}
	return s
}

// serverKey normalizes a dial address or remote address to the key metrics
// are tracked under: the IP for TCP addresses, the socket path for unix
// sockets.
func serverKey(network, addr string) string {
	if strings.HasPrefix(network, "unix") {
		return addr
	}
	return hostOnly(addr)
}

// hostOnly strips an optional port from addr.
func hostOnly(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// trackedConn wraps a net.Conn to decrement the server's active connection
// count exactly once when the connection is closed.
type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *trackedConn) Close() error {
	c.once.Do(c.release)
	return c.Conn.Close()
}
