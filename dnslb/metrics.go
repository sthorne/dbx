package dnslb

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// ServerMetrics is a point-in-time snapshot of the metrics tracked for a
// single server.
type ServerMetrics struct {
	// Server identifies the server: an IP address, or a socket path for unix
	// socket connections.
	Server string

	// DialAttempts and DialFailures count connection attempts to the server.
	DialAttempts uint64
	DialFailures uint64

	// ConsecutiveDialFailures is the number of dial failures since the last
	// successful dial. Non-zero means the server is currently being avoided.
	ConsecutiveDialFailures uint64

	// Availability is the percentage of dial attempts that succeeded, in
	// [0, 100]. It is 100 when no dial has been attempted yet.
	Availability float64

	// ActiveConns is the number of currently open connections to the server.
	ActiveConns int64

	// Queries, QueryErrors, and SlowQueries count queries executed against
	// the server. A query is slow when it takes at least Config.SlowThreshold.
	Queries     uint64
	QueryErrors uint64
	SlowQueries uint64

	// AvgQueryLatency, MinQueryLatency, and MaxQueryLatency summarize query
	// durations. EWMALatency is the exponentially weighted moving average of
	// dial and query latency used to order servers; lower is preferred.
	AvgQueryLatency time.Duration
	MinQueryLatency time.Duration
	MaxQueryLatency time.Duration
	EWMALatency     time.Duration

	// FirstSeen, LastSuccess, and LastFailure record when the server was
	// first resolved and when it last succeeded or failed. LastError is the
	// message of the most recent dial or query error.
	FirstSeen   time.Time
	LastSuccess time.Time
	LastFailure time.Time
	LastError   string
}

// Metrics returns a snapshot of the metrics for every server the Balancer
// has seen, sorted by server.
func (b *Balancer) Metrics() []ServerMetrics {
	b.mu.Lock()
	defer b.mu.Unlock()

	metrics := make([]ServerMetrics, 0, len(b.servers))
	for _, s := range b.servers {
		m := ServerMetrics{
			Server:                  s.server,
			DialAttempts:            s.dialAttempts,
			DialFailures:            s.dialFailures,
			ConsecutiveDialFailures: s.consecutiveDialFailures,
			Availability:            100,
			ActiveConns:             s.activeConns,
			Queries:                 s.queries,
			QueryErrors:             s.queryErrors,
			SlowQueries:             s.slowQueries,
			MinQueryLatency:         s.minQueryLatency,
			MaxQueryLatency:         s.maxQueryLatency,
			EWMALatency:             time.Duration(s.ewmaLatencyNs),
			FirstSeen:               s.firstSeen,
			LastSuccess:             s.lastSuccess,
			LastFailure:             s.lastFailure,
			LastError:               s.lastError,
		}
		if s.dialAttempts > 0 {
			m.Availability = 100 * float64(s.dialAttempts-s.dialFailures) / float64(s.dialAttempts)
		}
		if s.queries > 0 {
			m.AvgQueryLatency = s.totalQueryLatency / time.Duration(s.queries)
		}
		metrics = append(metrics, m)
	}

	sort.Slice(metrics, func(i, j int) bool { return metrics[i].Server < metrics[j].Server })
	return metrics
}

// Report returns the current metrics for all servers formatted as a
// human-readable table.
func (b *Balancer) Report() string {
	var sb strings.Builder
	b.WriteReport(&sb)
	return sb.String()
}

// WriteReport writes the current metrics for all servers to w as a
// human-readable table.
func (b *Balancer) WriteReport(w io.Writer) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SERVER\tAVAIL%\tDIALS\tDIALERR\tACTIVE\tQUERIES\tERRORS\tSLOW\tAVG\tEWMA\tMIN\tMAX\tLAST ERROR")
	for _, m := range b.Metrics() {
		fmt.Fprintf(tw, "%s\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
			m.Server,
			m.Availability,
			m.DialAttempts,
			m.DialFailures,
			m.ActiveConns,
			m.Queries,
			m.QueryErrors,
			m.SlowQueries,
			fmtLatency(m.AvgQueryLatency),
			fmtLatency(m.EWMALatency),
			fmtLatency(m.MinQueryLatency),
			fmtLatency(m.MaxQueryLatency),
			m.LastError,
		)
	}
	tw.Flush()
}

func fmtLatency(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	return d.Round(10 * time.Microsecond).String()
}
