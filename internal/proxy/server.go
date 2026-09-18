// Package proxy implements dbgateway's client-facing RESP server: it accepts
// ordinary Redis client connections, resolves each key's shard via the
// consistent-hash ring, and forwards the command to the small, shared
// backend connection pool for that shard.
package proxy

import (
	"context"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"

	"dbgateway/internal/backend"
	"dbgateway/internal/hashring"
	"dbgateway/internal/metrics"
)

// Server is the RESP front-end that clients connect to.
type Server struct {
	Addr    string
	Ring    *hashring.Ring
	Backend *backend.Pool
	Metrics *metrics.Metrics
}

// ListenAndServe starts the proxy and blocks until it exits.
func (s *Server) ListenAndServe() error {
	return redcon.ListenAndServe(s.Addr,
		s.handleCommand,
		s.onAccept,
		s.onClosed,
	)
}

func (s *Server) onAccept(conn redcon.Conn) bool {
	s.Metrics.ClientConnected()
	return true
}

func (s *Server) onClosed(conn redcon.Conn, err error) {
	s.Metrics.ClientDisconnected()
}

func (s *Server) handleCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR empty command")
		return
	}
	name := strings.ToUpper(string(cmd.Args[0]))
	s.Metrics.CommandReceived(name)

	switch name {
	case "PING":
		conn.WriteString("PONG")

	case "GET":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR usage: GET key")
			return
		}
		s.handleGet(conn, string(cmd.Args[1]))

	case "SET":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR usage: SET key value")
			return
		}
		s.handleSet(conn, string(cmd.Args[1]), string(cmd.Args[2]))

	case "DEL":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR DEL currently supports exactly one key (multi-key DEL spans shards -- see batching milestone)")
			return
		}
		s.handleDel(conn, string(cmd.Args[1]))

	case "QUIT":
		conn.WriteString("OK")
		conn.Close()

	default:
		conn.WriteError("ERR unknown command '" + name + "'")
	}
}

func (s *Server) shardFor(key string) (*redis.Client, string, bool) {
	shardAddr, ok := s.Ring.Get(key)
	if !ok {
		return nil, "", false
	}
	client, err := s.Backend.Client(shardAddr)
	if err != nil {
		log.Printf("proxy: %v", err)
		return nil, shardAddr, false
	}
	return client, shardAddr, true
}

func (s *Server) handleGet(conn redcon.Conn, key string) {
	client, shardAddr, ok := s.shardFor(key)
	if !ok {
		conn.WriteError("ERR no shard available for key")
		return
	}
	s.Metrics.RoutedToShard(shardAddr)

	val, err := client.Get(context.Background(), key).Result()
	switch {
	case err == redis.Nil:
		conn.WriteNull()
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteBulkString(val)
	}
}

func (s *Server) handleSet(conn redcon.Conn, key, value string) {
	client, shardAddr, ok := s.shardFor(key)
	if !ok {
		conn.WriteError("ERR no shard available for key")
		return
	}
	s.Metrics.RoutedToShard(shardAddr)

	if err := client.Set(context.Background(), key, value, 0).Err(); err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteString("OK")
}

func (s *Server) handleDel(conn redcon.Conn, key string) {
	client, shardAddr, ok := s.shardFor(key)
	if !ok {
		conn.WriteError("ERR no shard available for key")
		return
	}
	s.Metrics.RoutedToShard(shardAddr)

	n, err := client.Del(context.Background(), key).Result()
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteInt64(n)
}
