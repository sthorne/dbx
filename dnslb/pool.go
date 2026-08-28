package dnslb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sthorne/dbx/v5"
	"github.com/sthorne/dbx/v5/pgconn"
	"github.com/sthorne/dbx/v5/pgxpool"
)

// Defaults used by NewPool when the corresponding PoolConfig field is zero.
const (
	DefaultResolveInterval   = 30 * time.Second
	DefaultMinConnsPerServer = 1
)

// ErrPoolClosed is returned by Pool methods after Close has been called.
var ErrPoolClosed = errors.New("dnslb: pool is closed")

// PoolConfig configures a Pool. The zero value is valid; NewPool fills in
// defaults for any zero fields.
type PoolConfig struct {
	// Config configures the Balancer that tracks metrics and scores servers.
	Config

	// ResolveInterval is how often DNS is re-resolved in the background to
	// pick up servers joining or leaving the cluster; each cycle also probes
	// every server to keep latency and availability metrics fresh. Defaults
	// to DefaultResolveInterval. A negative value disables background
	// resolution and probing; Refresh may still be called manually.
	ResolveInterval time.Duration

	// MinConnsPerServer is the number of standing connections maintained to
	// each server. Defaults to DefaultMinConnsPerServer so that every server
	// in the cluster has a live connection. A negative value means zero
	// (connections are only established on demand).
	MinConnsPerServer int32

	// MaxConnsPerServer caps the connections to each server. Zero keeps the
	// pool config's MaxConns (which applies per server).
	MaxConnsPerServer int32

	// DisableProbes turns off the background health probe query ("select 1")
	// that runs against every server each ResolveInterval.
	DisableProbes bool
}

// Pool is a load-balancing connection pool for clusters behind one or more
// DNS names, such as a CockroachDB cluster. It maintains a separate
// connection pool — with standing connections — to every server the DNS
// names resolve to, and routes each operation to the server with the lowest
// score:
//
//	EWMA latency × (in-use connections + 1) + failure penalty
//
// so traffic cycles across servers based on current activity and observed
// latency: the fastest idle server is preferred, a busy server's score rises
// with each in-use connection, and servers failing to accept connections are
// avoided until they recover. Servers with equal scores (e.g. before any
// latency has been observed) are rotated round-robin.
//
// DNS is re-resolved every ResolveInterval: pools are added for new servers
// and drained and closed for servers that leave the cluster. Each cycle also
// probes every server with a trivial query so that latency and availability
// metrics stay fresh even for servers receiving no traffic.
//
// Per-server metrics are available via Metrics and Report. Pool is safe for
// concurrent use.
type Pool struct {
	balancer        *Balancer
	template        *pgxpool.Config
	resolveInterval time.Duration
	minConns        int32
	maxConns        int32
	disableProbes   bool

	mu     sync.RWMutex
	pools  map[string]*serverPool
	closed bool

	rr   atomic.Uint64
	done chan struct{}
	wg   sync.WaitGroup
}

// serverPool is one server's connection pool. key is the routing identity
// ("ip:port" or unix socket path); metricsKey is the Balancer's key for the
// server (IP or socket path).
type serverPool struct {
	key        string
	metricsKey string
	pool       *pgxpool.Pool
}

// serverTarget is one resolved server: an IP (or unix socket directory) plus
// the port, original hostname, and TLS config it was derived from.
type serverTarget struct {
	host     string
	port     uint16
	hostname string
	source   *pgconn.FallbackConfig
}

// NewPool creates a Pool from a connection string in the same formats
// accepted by pgxpool.ParseConfig. Pool-level settings in the connection
// string (e.g. pool_max_conns) apply per server.
func NewPool(ctx context.Context, connString string, cfg PoolConfig) (*Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	return NewPoolWithConfig(ctx, poolConfig, cfg)
}

// NewPoolWithConfig creates a Pool from a pgxpool.Config, which must have
// been created by pgxpool.ParseConfig. The config is used as the template
// for every per-server pool; its host is replaced by each resolved IP, and
// its pool-level settings apply per server. NewPoolWithConfig fails if no
// server can be resolved.
func NewPoolWithConfig(ctx context.Context, poolConfig *pgxpool.Config, cfg PoolConfig) (*Pool, error) {
	if cfg.ResolveInterval == 0 {
		cfg.ResolveInterval = DefaultResolveInterval
	}
	if cfg.MinConnsPerServer == 0 {
		cfg.MinConnsPerServer = DefaultMinConnsPerServer
	}
	if cfg.MinConnsPerServer < 0 {
		cfg.MinConnsPerServer = 0
	}

	p := &Pool{
		balancer:        New(cfg.Config),
		template:        poolConfig.Copy(),
		resolveInterval: cfg.ResolveInterval,
		minConns:        cfg.MinConnsPerServer,
		maxConns:        cfg.MaxConnsPerServer,
		disableProbes:   cfg.DisableProbes,
		pools:           map[string]*serverPool{},
		done:            make(chan struct{}),
	}

	// A partial failure (some servers resolved, others not) still yields a
	// working pool; fail only when no server could be set up.
	if err := p.Refresh(ctx); err != nil && len(p.Servers()) == 0 {
		p.Close()
		return nil, err
	}

	if p.resolveInterval > 0 {
		p.wg.Add(1)
		go p.refresher()
	}

	return p, nil
}

// Balancer returns the Balancer tracking this Pool's per-server metrics.
func (p *Pool) Balancer() *Balancer { return p.balancer }

// Metrics returns a snapshot of the metrics for every server. See
// Balancer.Metrics.
func (p *Pool) Metrics() []ServerMetrics { return p.balancer.Metrics() }

// Report returns the current per-server metrics formatted as a
// human-readable table. See Balancer.Report.
func (p *Pool) Report() string { return p.balancer.Report() }

// Servers returns the servers the Pool currently maintains connections to,
// sorted.
func (p *Pool) Servers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	servers := make([]string, 0, len(p.pools))
	for key := range p.pools {
		servers = append(servers, key)
	}
	sort.Strings(servers)
	return servers
}

// Stats returns the pgxpool statistics for each server's pool, keyed by
// server.
func (p *Pool) Stats() map[string]*pgxpool.Stat {
	p.mu.RLock()
	defer p.mu.RUnlock()
	stats := make(map[string]*pgxpool.Stat, len(p.pools))
	for key, sp := range p.pools {
		stats[key] = sp.pool.Stat()
	}
	return stats
}

// Refresh re-resolves the configured DNS names now, adding pools for new
// servers and draining and closing pools for servers no longer resolved. On
// resolution failure the current set of servers is kept and the error
// returned.
func (p *Pool) Refresh(ctx context.Context) error {
	targets, err := p.resolveTargets(ctx)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return errors.New("dnslb: resolved zero addresses; keeping current servers")
	}

	var errs []error

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}

	seen := make(map[string]bool, len(targets))
	for _, target := range targets {
		key := target.key()
		if seen[key] {
			continue
		}
		seen[key] = true
		if _, ok := p.pools[key]; ok {
			continue
		}
		sp, err := p.newServerPool(ctx, target)
		if err != nil {
			errs = append(errs, fmt.Errorf("dnslb: server %s: %w", key, err))
			continue
		}
		p.pools[key] = sp
	}

	var removed []*serverPool
	for key, sp := range p.pools {
		if !seen[key] {
			delete(p.pools, key)
			removed = append(removed, sp)
		}
	}
	p.mu.Unlock()

	// Drain removed pools in the background; Close waits for connections to
	// be released.
	for _, sp := range removed {
		go sp.pool.Close()
	}

	return errors.Join(errs...)
}

// resolveTargets expands every configured host (primary and fallbacks) to
// the underlying servers.
func (p *Pool) resolveTargets(ctx context.Context) ([]serverTarget, error) {
	cc := p.template.ConnConfig
	fallbacks := []*pgconn.FallbackConfig{{Host: cc.Host, Port: cc.Port, TLSConfig: cc.TLSConfig}}
	fallbacks = append(fallbacks, cc.Fallbacks...)

	var targets []serverTarget
	var errs []error

	for _, fb := range fallbacks {
		// Unix sockets and IP literals need no resolution.
		if strings.HasPrefix(fb.Host, "/") || net.ParseIP(fb.Host) != nil {
			targets = append(targets, serverTarget{host: fb.Host, port: fb.Port, hostname: fb.Host, source: fb})
			continue
		}

		addrs, err := p.balancer.lookup(ctx, fb.Host)
		if err != nil {
			errs = append(errs, fmt.Errorf("dnslb: resolving %s: %w", fb.Host, err))
			continue
		}
		for _, addr := range addrs {
			target := serverTarget{host: addr, port: fb.Port, hostname: fb.Host, source: fb}
			if host, portStr, err := net.SplitHostPort(addr); err == nil {
				if port, err := strconv.ParseUint(portStr, 10, 16); err == nil {
					target.host = host
					target.port = uint16(port)
				}
			}
			targets = append(targets, target)
		}
	}

	if len(targets) == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return targets, nil
}

func (t serverTarget) key() string {
	if strings.HasPrefix(t.host, "/") {
		_, address := pgconn.NetworkAddress(t.host, t.port)
		return address
	}
	return net.JoinHostPort(t.host, strconv.Itoa(int(t.port)))
}

// newServerPool builds a pgxpool.Pool pinned to a single server, wired into
// the Balancer for dial and query metrics.
func (p *Pool) newServerPool(ctx context.Context, target serverTarget) (*serverPool, error) {
	config := p.template.Copy()
	cc := config.ConnConfig
	cc.Host = target.host
	cc.Port = target.port
	cc.Fallbacks = nil
	cc.TLSConfig = nil
	if target.source.TLSConfig != nil {
		tlsConfig := target.source.TLSConfig.Clone()
		if tlsConfig.ServerName == "" && !tlsConfig.InsecureSkipVerify {
			tlsConfig.ServerName = target.hostname
		}
		cc.TLSConfig = tlsConfig
	}
	p.balancer.Apply(cc)

	config.MinConns = p.minConns
	if p.maxConns > 0 {
		config.MaxConns = p.maxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	network, address := pgconn.NetworkAddress(target.host, target.port)
	return &serverPool{
		key:        target.key(),
		metricsKey: serverKey(network, address),
		pool:       pool,
	}, nil
}

// pickServer returns the server with the lowest routing score, rotating
// round-robin among servers with equal scores.
func (p *Pool) pickServer() (*serverPool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, ErrPoolClosed
	}
	if len(p.pools) == 0 {
		return nil, errors.New("dnslb: no servers available")
	}

	keys := make([]string, 0, len(p.pools))
	for key := range p.pools {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var candidates []*serverPool
	minScore := 0.0
	for _, key := range keys {
		sp := p.pools[key]
		active := int64(sp.pool.Stat().AcquiredConns())
		score := p.balancer.routeScore(sp.metricsKey, active)
		switch {
		case len(candidates) == 0 || score < minScore:
			candidates = append(candidates[:0], sp)
			minScore = score
		case score == minScore:
			candidates = append(candidates, sp)
		}
	}

	n := p.rr.Add(1) - 1
	return candidates[n%uint64(len(candidates))], nil
}

// Acquire returns a connection to the currently best-scoring server. The
// returned connection must be released with Release.
func (p *Pool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	sp, err := p.pickServer()
	if err != nil {
		return nil, err
	}
	return sp.pool.Acquire(ctx)
}

// AcquireFunc acquires a connection from the currently best-scoring server
// and calls f with it, releasing the connection when f returns.
func (p *Pool) AcquireFunc(ctx context.Context, f func(*pgxpool.Conn) error) error {
	sp, err := p.pickServer()
	if err != nil {
		return err
	}
	return sp.pool.AcquireFunc(ctx, f)
}

// Exec routes the query to the currently best-scoring server.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	sp, err := p.pickServer()
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return sp.pool.Exec(ctx, sql, args...)
}

// Query routes the query to the currently best-scoring server.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (dbx.Rows, error) {
	sp, err := p.pickServer()
	if err != nil {
		return nil, err
	}
	return sp.pool.Query(ctx, sql, args...)
}

// QueryRow routes the query to the currently best-scoring server.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) dbx.Row {
	sp, err := p.pickServer()
	if err != nil {
		return errRow{err: err}
	}
	return sp.pool.QueryRow(ctx, sql, args...)
}

// SendBatch routes the batch to the currently best-scoring server.
func (p *Pool) SendBatch(ctx context.Context, b *dbx.Batch) dbx.BatchResults {
	sp, err := p.pickServer()
	if err != nil {
		return errBatchResults{err: err}
	}
	return sp.pool.SendBatch(ctx, b)
}

// Begin starts a transaction on the currently best-scoring server. The
// transaction runs entirely on that server.
func (p *Pool) Begin(ctx context.Context) (dbx.Tx, error) {
	sp, err := p.pickServer()
	if err != nil {
		return nil, err
	}
	return sp.pool.Begin(ctx)
}

// BeginTx starts a transaction with txOptions on the currently best-scoring
// server. The transaction runs entirely on that server.
func (p *Pool) BeginTx(ctx context.Context, txOptions dbx.TxOptions) (dbx.Tx, error) {
	sp, err := p.pickServer()
	if err != nil {
		return nil, err
	}
	return sp.pool.BeginTx(ctx, txOptions)
}

// Ping pings every server concurrently. It returns nil if at least one
// server is reachable; the joined errors otherwise. Per-server failures are
// recorded in the metrics either way.
func (p *Pool) Ping(ctx context.Context) error {
	pools := p.snapshot()
	if len(pools) == 0 {
		return errors.New("dnslb: no servers available")
	}

	errs := make([]error, len(pools))
	var wg sync.WaitGroup
	for i, sp := range pools {
		wg.Add(1)
		go func(i int, sp *serverPool) {
			defer wg.Done()
			if err := sp.pool.Ping(ctx); err != nil {
				errs[i] = fmt.Errorf("dnslb: server %s: %w", sp.key, err)
			}
		}(i, sp)
	}
	wg.Wait()

	for _, err := range errs {
		if err == nil {
			return nil
		}
	}
	return errors.Join(errs...)
}

// Close stops background resolution, closes every server's pool, and waits
// for all connections to be released.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	pools := make([]*serverPool, 0, len(p.pools))
	for _, sp := range p.pools {
		pools = append(pools, sp)
	}
	p.mu.Unlock()

	close(p.done)
	p.wg.Wait()

	for _, sp := range pools {
		sp.pool.Close()
	}
}

func (p *Pool) snapshot() []*serverPool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pools := make([]*serverPool, 0, len(p.pools))
	for _, sp := range p.pools {
		pools = append(pools, sp)
	}
	return pools
}

func (p *Pool) refresher() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.resolveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			timeout := p.resolveInterval
			if timeout > 15*time.Second {
				timeout = 15 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			_ = p.Refresh(ctx)
			if !p.disableProbes {
				p.probeAll(ctx)
			}
			cancel()
		}
	}
}

// probeAll runs a trivial query against every server concurrently so that
// latency and availability metrics stay fresh for servers receiving no
// traffic. Results are recorded through the Balancer's tracer and dialer.
func (p *Pool) probeAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, sp := range p.snapshot() {
		wg.Add(1)
		go func(sp *serverPool) {
			defer wg.Done()
			_, _ = sp.pool.Exec(ctx, "select 1")
		}(sp)
	}
	wg.Wait()
}

// routeScore returns the Pool routing score for a server given the number of
// in-use connections; lower is preferred. A server with no latency history
// scores zero so that it is explored first.
func (b *Balancer) routeScore(key string, active int64) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.servers[key]
	if s == nil {
		return 0
	}
	return s.ewmaLatencyNs*float64(active+1) + float64(s.consecutiveDialFailures)*float64(b.failurePenalty)
}

// errRow is a dbx.Row that reports a routing error at Scan.
type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

// errBatchResults is a dbx.BatchResults that reports a routing error.
type errBatchResults struct{ err error }

func (r errBatchResults) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, r.err }
func (r errBatchResults) Query() (dbx.Rows, error)         { return nil, r.err }
func (r errBatchResults) QueryRow() dbx.Row                { return errRow{err: r.err} }
func (r errBatchResults) Close() error                     { return r.err }
