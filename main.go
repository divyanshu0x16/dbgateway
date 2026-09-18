package main

import "fmt"
import "net"

func main() {
	fmt.Println("hello from dbgateway")
	listener, err := net.Listen("tcp", ":7000")

	if err != nil {
		panic(err)
	}

	fmt.Println("listening on :7000")

	for{
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}

		fmt.Println("client connected:", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("read error:", err)
			return
		}

		fmt.Printf("received: %s\n", string(buf[:n]))
		conn.Write(buf[:n])
	}
}