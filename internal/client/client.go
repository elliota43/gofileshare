package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/elliota43/wormbeam/internal/protocol"
)

// Conn wraps a net.Conn with a buffered reader.
// TODO: this is going to change when the protocol switches from
// a primitive newline-delimited style
type Conn struct {
	net.Conn
	r *bufio.Reader
}

// Dial opens a TCP connection to the relay and wraps it.
func Dial(addr string) (*Conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial relay %s: %w", addr, err)
	}

	return &Conn{Conn: c, r: bufio.NewReader(c)}, nil
}

func (c *Conn) ReadCommand() (protocol.Command, error) {
	return protocol.ReadCommand(c.r)
}

// Reader returns the buffered reader.
// Use this instead of reading from net.Conn
func (c *Conn) Reader() *bufio.Reader { return c.r }

func Host(c *Conn) error {
	if err := protocol.WriteHost(c); err != nil {
		return fmt.Errorf("send HOST: %w", err)
	}

	// Server replies with the bare code as the verb
	cmd, err := c.ReadCommand()
	if err != nil {
		return fmt.Errorf("read code: %w", err)
	}

	code := cmd.Verb
	fmt.Printf("Hosting. Share this code with your peer: %s\n", code)

	// Wait for the peer-joined notification
	cmd, err = c.ReadCommand()
	if err != nil {
		return fmt.Errorf("waiting for peer: %w", err)
	}

	if cmd.Verb != protocol.VerbPeerJoined {
		return fmt.Errorf("unexpected message from relay: %q", cmd.Verb)
	}

	fmt.Println("[system] peer connected")

	return pump(c)
}

func Join(c *Conn, code string) error {
	if err := protocol.WriteJoin(c, code); err != nil {
		return fmt.Errorf("send JOIN: %w", err)
	}

	cmd, err := c.ReadCommand()
	if err != nil {
		return fmt.Errorf("read JOIN reply: %w", err)
	}

	switch cmd.Verb {
	case protocol.VerbOK:
		fmt.Println("[system] joined, waiting for host")
	case protocol.VerbErr:
		return fmt.Errorf("relay rejected join: %s", cmd.Arg)

	default:
		return fmt.Errorf("unexpected reply: %q", cmd.Verb)
	}

	return pump(c)
}

// pump is a placeholder for the post-handshake byte pipe connecting the
// two peers.
// TODO: replace with send/recv loop
func pump(c *Conn) error {
	errCh := make(chan error, 2)
	go func() { _, err := io.Copy(os.Stdout, c.Reader()); errCh <- err }()
	go func() { _, err := io.Copy(c, os.Stdin); errCh <- err }()

	return <-errCh
}
