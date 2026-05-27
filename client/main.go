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

const ServerAddr = "16.59.53.129:8080"

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

	switch mode {
	case "host":
		handleHost(conn)
	case "join":
		handleJoin(conn)

	default:
		log.Fatal("Unknown mode. Use 'host' or 'join'.")
	}

	go func() {
		io.Copy(os.Stdout, conn)
	}()
	io.Copy(conn, os.Stdin)
}

func handleHost(conn net.Conn) {
	reader := bufio.NewReader(conn)

	conn.Write([]byte("HOST\n"))

	code, _ := reader.ReadString('\n')
	fmt.Printf("Hosting! Tell your peer to run: %s join %s", os.Args[0], code)
}

func handleJoin(conn net.Conn) {

	if len(os.Args) < 3 {
		log.Fatal("Please provide a code. Example: %s join 1234", os.Args[0])
	}

	reader := bufio.NewReader(conn)
	code := os.Args[2]

	conn.Write([]byte(fmt.Sprintf("JOIN %s\n", code)))

	resp, _ := reader.ReadString('\n')
	if strings.HasPrefix(resp, "ERROR") {
		log.Fatalf("Server rejected join: %s", resp)
	}

	fmt.Println("Successfully joined!")
}
