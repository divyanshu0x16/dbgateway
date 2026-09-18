package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"

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

	client := redisclient.NewClient()

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}

		fmt.Println("client connected:", conn.RemoteAddr())
		go handleConnection(conn, client)
	}
}

func handleConnection(conn net.Conn, client *redis.Client) {
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
			val, err := client.Get(ctx, parts[1]).Result()
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
