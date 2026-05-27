package relay

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/elliota43/wormbeam/internal/protocol"
)

// hostTTL is how long a parked host waits for a joiner before being evicted.
const hostTTL = 5 * time.Minute

// Server is the relay broker.
// Must be initialized with New()
type Server struct {
	mu      sync.Mutex
	waiting map[string]*parkedHost
	log     *log.Logger
}

type parkedHost struct {
	conn      net.Conn
	expiresAt time.Time
}

// New constructs a Server. If logger is nil, the standard logger is used.
func New(logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}

	return &Server{
		waiting: make(map[string]*parkedHost),
		log:     logger,
	}
}

// ListenAndServe binds to addr and accepts connections until the listener errors.
func (s *Server) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	defer l.Close()

	s.log.Printf("relay listening on %s", addr)
	go s.reapLoop()

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}

		go s.handle(conn)
	}
}

// handle reads the first command from a new connection and routes it.
// It does not close the connection -- ownership transfers to the session (JOIN)
// or the waiting map (for HOST).
func (s *Server) handle(conn net.Conn) {
	r := bufio.NewReader(conn)
	cmd, err := protocol.ReadCommand(r)
	if err != nil {
		conn.Close()
		return
	}

	switch cmd.Verb {
	case protocol.VerbHost:
		s.park(conn)
	case protocol.VerbJoin:
		s.match(conn, cmd.Arg)
	default:
		_ = protocol.WriteErr(conn, "unknown command")
		conn.Close()
	}
}

// park stores a host connection under a fresh code and tells it the code
func (s *Server) park(conn net.Conn) {
	code, err := s.assignCode(conn)
	if err != nil {
		_ = protocol.WriteErr(conn, err.Error())
		conn.Close()
		return
	}

	if err := protocol.WriteCode(conn, code); err != nil {
		s.evict(code)
		conn.Close()
		return
	}

	s.log.Printf("host parked: code=%s", code)
}

func (s *Server) assignCode(conn net.Conn) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < 20; i++ {
		code := fmt.Sprintf("%04d", rand.IntN(10000))
		if _, taken := s.waiting[code]; taken {
			continue
		}

		s.waiting[code] = &parkedHost{
			conn:      conn,
			expiresAt: time.Now().Add(hostTTL),
		}
		return code, nil
	}

	return "", fmt.Errorf("no codes available")
}

func (s *Server) match(joiner net.Conn, code string) {
	s.mu.Lock()
	host, ok := s.waiting[code]
	if ok {
		delete(s.waiting, code)
	}
	s.mu.Unlock()

	if !ok {
		_ = protocol.WriteErr(joiner, "code not found")
		joiner.Close()
		return
	}

	if err := protocol.WriteOK(joiner); err != nil {
		host.conn.Close()
		joiner.Close()
		return
	}

	if err := protocol.WritePeerJoined(host.conn); err != nil {
		host.conn.Close()
		joiner.Close()
		return
	}

	s.log.Printf("matched code=%s, splicing", code)
	splice(host.conn, joiner)
}

func (s *Server) evict(code string) {
	s.mu.Lock()
	delete(s.waiting, code)
	s.mu.Unlock()
}

func (s *Server) reapLoop() {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for now := range tick.C {
		s.mu.Lock()
		for code, h := range s.waiting {
			if now.After(h.expiresAt) {
				h.conn.Close()
				delete(s.waiting, code)
				s.log.Printf("reaped expired host: code=%s", code)
			}
		}
		s.mu.Unlock()
	}
}

// splice runs a bidirectional copy between a and b. When either direction
// finishes, both conns are closed and splice returns once both copies exit.
func splice(a, b net.Conn) {
	defer a.Close()
	defer b.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	copy := func(dst, src net.Conn) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)

		_ = dst.Close()
		_ = src.Close()
	}

	go copy(a, b)
	go copy(b, a)
	wg.Wait()
}
