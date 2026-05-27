package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu      sync.Mutex
	waiting map[string]net.Conn
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	broker := &Broker{waiting: make(map[string]net.Conn)}
	log.Println("Relay Server listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go broker.handle(conn)
	}
}

func (b *Broker) handle(conn net.Conn) {
	reader := bufio.NewReader(conn)
	msg, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return
	}

	cmd := strings.TrimSpace(msg)

	if cmd == "HOST" {
		rand.Seed(time.Now().UnixNano())
		code := fmt.Sprintf("%04d", rand.Intn(10000))

		b.mu.Lock()
		b.waiting[code] = conn
		b.mu.Unlock()

		conn.Write([]byte(code + "\n"))
		log.Printf("Host parked with code: %s", code)
	} else if strings.HasPrefix(cmd, "JOIN") {
		parts := strings.Split(cmd, " ")
		if len(parts) != 2 {
			conn.Close()
			return
		}

		code := parts[1]

		b.mu.Lock()
		hostConn, exists := b.waiting[code]
		if exists {
			delete(b.waiting, code)
		}
		b.mu.Unlock()

		if !exists {
			conn.Write([]byte("ERROR: Code not found\n"))
			conn.Close()
			return
		}

		conn.Write([]byte("SUCCESS\n"))
		log.Printf("Matched code %s. Splicing connections!", code)

		go io.Copy(hostConn, conn)
		io.Copy(conn, hostConn)

		hostConn.Close()
		conn.Close()
	}
}
