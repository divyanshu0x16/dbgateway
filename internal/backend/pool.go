// Package backend manages the small, shared connection pools dbgateway
// keeps open to each Redis shard -- the "many clients, few backend
// connections" multiplexing idea this project is built around.
package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Pool holds one *redis.Client (itself a small internal connection pool)
// per backend shard address.
type Pool struct {
	mu      sync.RWMutex
	clients map[string]*redis.Client
	// PoolSize is the max number of TCP connections dbgateway keeps open to
	// each shard, regardless of how many clients are connected to dbgateway.
	PoolSize int
}

// NewPool builds a Pool with one client per shard address.
func NewPool(shardAddrs []string, poolSize int) *Pool {
	p := &Pool{
		clients:  make(map[string]*redis.Client, len(shardAddrs)),
		PoolSize: poolSize,
	}
	for _, addr := range shardAddrs {
		p.clients[addr] = redis.NewClient(&redis.Options{
			Addr:     addr,
			PoolSize: poolSize,
		})
	}
	return p
}

// Client returns the shared client for the given shard address.
func (p *Pool) Client(shardAddr string) (*redis.Client, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	c, ok := p.clients[shardAddr]
	if !ok {
		return nil, fmt.Errorf("backend: no client for shard %q", shardAddr)
	}
	return c, nil
}

// Ping checks connectivity to every shard and returns the first error, if any.
func (p *Pool) Ping(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for addr, c := range p.clients {
		if err := c.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("backend: shard %q unreachable: %w", addr, err)
		}
	}
	return nil
}

// Close shuts down every backend client.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for _, c := range p.clients {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
