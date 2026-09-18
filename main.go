package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"hash/fnv"

	"dbgateway/redisclient"
	"github.com/redis/go-redis/v9"
)

func main() {
	fmt.Println("hello from dbgateway")
	listener, err := net.Listen("tcp", ":7001")

	if err != nil {
		panic(err)
	}

	fmt.Println("listening on :7001")

	addrs := []string{"127.0.0.1:6379", "127.0.0.1:6380"}
	clients := make([]*redis.Client, 0, len(addrs))
	for _, addr := range addrs {
		clients = append(clients, redisclient.NewClient(addr))
	}

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}

		fmt.Println("client connected:", conn.RemoteAddr())
		go handleConnection(conn, clients)
	}
}

func handleConnection(conn net.Conn, clients []*redis.Client) {
	defer conn.Close()

	ctx := context.Background()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("read error:", err)
			return
		}

		line = strings.TrimSpace(line)
		parts := strings.Fields(line)

		if len(parts) == 0 {
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "GET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR usage: GET key\n"))
				continue
			}

			clients := pickClient(clients, parts[1])
			val, err := clients.Get(ctx, parts[1]).Result()

			if err == redis.Nil {
				conn.Write([]byte("(nil)\n"))
				continue
			}
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("ERR %s\n", err)))
				continue
			}
			conn.Write([]byte(val + "\n"))

		case "SET":

			if len(parts) != 3 {
				conn.Write([]byte("ERR usage: SET key value\n"))
				continue
			}

			clients := pickClient(clients, parts[1])
			err := clients.Set(ctx, parts[1], parts[2], 0).Err()

			if err != nil {
				conn.Write([]byte(fmt.Sprintf("ERR %s\n", err)))
				continue
			}

			conn.Write([]byte("OK\n"))

		default:
			conn.Write([]byte("ERR unknown command\n"))
		}

	}
}

func pickClient(clients []*redis.Client, key string) *redis.Client {
	h := fnv.New32a()
	h.Write([]byte(key))
	return clients[h.Sum32()%uint32(len(clients))]
}
