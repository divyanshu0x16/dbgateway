// Package metrics tracks basic counters for dbgateway.
// It's intentionally minimal -- a placeholder for richer per-tenant
// observability that later milestones (admission control, load balancing)
// will need.
package metrics

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds running counters for the proxy.
type Metrics struct {
	activeClients int64
	commands      int64

	mu           sync.Mutex
	commandCount map[string]int64
	shardCount   map[string]int64
}

// New creates an empty Metrics tracker.
func New() *Metrics {
	return &Metrics{
		commandCount: make(map[string]int64),
		shardCount:   make(map[string]int64),
	}
}

func (m *Metrics) ClientConnected()    { atomic.AddInt64(&m.activeClients, 1) }
func (m *Metrics) ClientDisconnected() { atomic.AddInt64(&m.activeClients, -1) }

func (m *Metrics) CommandReceived(name string) {
	atomic.AddInt64(&m.commands, 1)
	m.mu.Lock()
	m.commandCount[name]++
	m.mu.Unlock()
}

func (m *Metrics) RoutedToShard(shardAddr string) {
	m.mu.Lock()
	m.shardCount[shardAddr]++
	m.mu.Unlock()
}

// LogPeriodically starts a goroutine that logs a summary every interval,
// until ctx-like stop channel is closed. Call the returned func to stop it.
func (m *Metrics) LogPeriodically(interval time.Duration) (stop func()) {
	done := make(chan struct{})
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				m.logSnapshot()
			}
		}
	}()

	return func() { close(done) }
}

func (m *Metrics) logSnapshot() {
	clients := atomic.LoadInt64(&m.activeClients)
	total := atomic.LoadInt64(&m.commands)

	m.mu.Lock()
	shardSnapshot := make(map[string]int64, len(m.shardCount))
	for k, v := range m.shardCount {
		shardSnapshot[k] = v
	}
	m.mu.Unlock()

	log.Printf("dbgateway: clients=%d total_commands=%d shard_ops=%v", clients, total, shardSnapshot)
}
