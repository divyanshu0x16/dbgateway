package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"

	"dbgateway/redisclient"
	"dbgateway/ring"

	"github.com/redis/go-redis/v9"
)

func main() {
	fmt.Println("hello from dbgateway")
	listener, err := net.Listen("tcp", ":7001")

	if err != nil {
		panic(err)
	}

	fmt.Println("listening on :7001")
	addrs := []string{"127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381"}
	clients := make(map[string]*redis.Client)
	for _, addr := range addrs {
		clients[addr] = redisclient.NewClient(addr)
	}
	r := ring.New(addrs, 100)

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}

		fmt.Println("client connected:", conn.RemoteAddr())
		go handleConnection(conn, clients, r)
	}
}

func handleConnection(conn net.Conn, clients map[string]*redis.Client, r *ring.Ring) {
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

			client := clients[r.Get(parts[1])]
			val, err := client.Get(ctx, parts[1]).Result()

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

			client := clients[r.Get(parts[1])]
			err := client.Set(ctx, parts[1], parts[2], 0).Err()

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
