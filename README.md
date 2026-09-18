# dbgateway

A small proxy tier in front of sharded Redis, modeled loosely on Meta's
[ZGateway](https://engineering.fb.com/2026/09/03/core-infra/zgateway-proxy-zippydb-meta/)
design: clients connect to dbgateway instead of talking to shards directly,
and dbgateway multiplexes many client connections onto a small, shared pool
of backend connections per shard.

Current milestone: **basic proxy + connection multiplexing**
- RESP front-end (so any Redis client, including `redis-cli`, can connect to dbgateway)
- Consistent-hash ring routes each key to a shard
- One small `go-redis` connection pool per shard, shared across all clients
- `GET` / `SET` / `DEL` / `PING`

Not yet implemented (see Roadmap below): request batching/coalescing,
per-tenant admission control, read caching, load-aware balancing.

## 1. One-time environment setup

You need three things installed. None of them are present on this machine yet.

### Go SDK
Download and run the installer from https://go.dev/dl/ (pick the Windows
`.msi`). This also gives GoLand something to point at. Verify with:
```
go version
```
(open a **new** terminal after installing so PATH picks it up)

### GoLand
Download from https://www.jetbrains.com/go/download/ (or install via
[JetBrains Toolbox](https://www.jetbrains.com/toolbox-app/) if you prefer
managing IDE versions/updates that way — you already have Toolbox-installed
IntelliJ IDEA on this machine, so Toolbox will pick GoLand up alongside it).
On first launch, GoLand should auto-detect the Go SDK you just installed; if
not, set it manually under **Settings → Go → GOROOT**.

### Docker Desktop
Download from https://www.docker.com/products/docker-desktop/. Needed to run
the local Redis shards this project talks to. (Docker Desktop on Windows
requires WSL2 — the installer will prompt you to enable it if needed.)

## 2. Open the project

In GoLand: **File → Open** → select `C:\Sapient\zProxy`. GoLand will detect
`go.mod` and configure the module automatically.

## 3. Fetch dependencies

From GoLand's built-in terminal (or any shell, once Go is on PATH):
```
go mod tidy
```
This resolves and downloads `github.com/tidwall/redcon` (RESP server) and
`github.com/redis/go-redis/v9` (backend client), and writes `go.sum`.

## 4. Start the backend shards

```
docker compose up -d
```
This starts 3 Redis instances on `localhost:6380`, `6381`, `6382`.

## 5. Run dbgateway

From GoLand, right-click `cmd/dbgateway/main.go` → **Run**, or:
```
go run ./cmd/dbgateway
```
Flags (all optional, shown with defaults):
```
go run ./cmd/dbgateway -listen :7000 -shards localhost:6380,localhost:6381,localhost:6382 -backend-pool-size 10
```

## 6. Talk to it

Any Redis client works, since dbgateway speaks RESP. E.g. with `redis-cli`:
```
redis-cli -p 7000 SET foo bar
redis-cli -p 7000 GET foo
redis-cli -p 7000 PING
```
Watch the dbgateway logs — every 5s it prints a snapshot of connected
clients, total commands, and how ops split across shards, so you can see
keys landing on different shards via the hash ring.

## Project layout

```
cmd/dbgateway/main.go     entrypoint, flag parsing, wiring
internal/hashring/        consistent-hashing ring (key -> shard)
internal/backend/         per-shard connection pools to Redis
internal/proxy/           RESP server, command dispatch
internal/metrics/         counters + periodic logging
docker-compose.yml        3 local Redis shards
```

## Roadmap (next milestones, in rough order of the ZGateway article)

1. **Request batching/coalescing** — a per-shard batcher that merges
   concurrent requests to the same shard into one backend round trip, and
   collapses concurrent requests for the *same key* into a single fetch
   (hot-key stampede protection). This is ZGateway's signature feature.
2. **Per-tenant admission control** — tag requests with a tenant/use-case ID,
   give each tenant a fair-queued bucket, shed load from noisy tenants
   without affecting others (ZGateway's "Discriminant Load Shedding").
3. **Read caching** — in-process LRU cache in front of `GET`, with
   invalidation (simplest version: short TTL; closer-to-real version:
   subscribe to Redis keyspace notifications as a stand-in for ZippyDB's
   change-data-capture stream).
4. **Load-aware balancing** — if you add more than one dbgateway instance,
   weight routing by observed backend/proxy load instead of pure consistent
   hashing.
5. **Observability** — swap the log-based metrics for real Prometheus
   counters/histograms (`github.com/prometheus/client_golang`), one label
   per tenant/command/shard.
