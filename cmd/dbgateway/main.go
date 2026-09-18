// Command dbgateway is a stateless proxy tier in front of a set of Redis
// shards: clients connect to dbgateway instead of the shards directly, and
// dbgateway multiplexes them onto a small, shared pool of backend
// connections.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dbgateway/internal/backend"
	"dbgateway/internal/hashring"
	"dbgateway/internal/metrics"
	"dbgateway/internal/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":7000", "address dbgateway listens on for client connections")
	shards := flag.String("shards", "localhost:6380,localhost:6381,localhost:6382", "comma-separated list of backend Redis shard addresses")
	backendPoolSize := flag.Int("backend-pool-size", 10, "max connections dbgateway keeps open per backend shard")
	metricsInterval := flag.Duration("metrics-interval", 5*time.Second, "how often to log a metrics snapshot")
	flag.Parse()

	shardAddrs := splitAndTrim(*shards)
	if len(shardAddrs) == 0 {
		log.Fatal("dbgateway: no backend shards configured")
	}

	ring := hashring.New(shardAddrs)
	backendPool := backend.NewPool(shardAddrs, *backendPoolSize)
	defer backendPool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := backendPool.Ping(ctx); err != nil {
		cancel()
		log.Fatalf("dbgateway: startup ping failed: %v", err)
	}
	cancel()

	m := metrics.New()
	stopMetricsLog := m.LogPeriodically(*metricsInterval)
	defer stopMetricsLog()

	srv := &proxy.Server{
		Addr:    *listenAddr,
		Ring:    ring,
		Backend: backendPool,
		Metrics: m,
	}

	log.Printf("dbgateway: listening on %s, routing to shards %v", *listenAddr, shardAddrs)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		log.Fatalf("dbgateway: server exited: %v", err)
	case sig := <-sigCh:
		log.Printf("dbgateway: received %v, shutting down", sig)
	}
}

func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
