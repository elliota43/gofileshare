package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

const ServerAddr = "localhost:8080"

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go [host | join <code>]")
	}

	mode := os.Args[1]

	conn, err := net.Dial("tcp", ServerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to relay server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	if mode == "host" {
		conn.Write([]byte("HOST\n"))

		code, _ := reader.ReadString('\n')
		fmt.Printf("Hosting! Tell your peer to run: go run main.go join %s", code)
	} else if mode == "join" {
		if len(os.Args) < 3 {
			log.Fatal("Please provide a code. Example: go run main.go join 1234")
		}

		code := os.Args[2]

		conn.Write([]byte(fmt.Sprintf("JOIN %s\n", code)))

		resp, _ := reader.ReadString('\n')
		if strings.HasPrefix(resp, "ERROR") {
			log.Fatalf("Server rejected join: %s", resp)
		}

		fmt.Println("Successfully joined!")
	} else {
		log.Fatal("Unknown mode. Use 'host' or 'join'.")
	}

	go func() {
		io.Copy(os.Stdout, conn)
	}()
	io.Copy(conn, os.Stdin)
}
