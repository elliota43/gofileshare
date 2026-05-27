package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/elliota43/wormbeam/internal/protocol"
	"github.com/elliota43/wormbeam/internal/transfer"
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

// Host registers with the relay, prints the code, and on peer-joined enters
// the REPL.
func Host(c *Conn) error {
	if err := protocol.WriteHost(c); err != nil {
		return fmt.Errorf("send HOST: %w", err)
	}
	cmd, err := c.ReadCommand()
	if err != nil {
		return fmt.Errorf("read code: %w", err)
	}
	code := cmd.Verb
	fmt.Printf("Hosting. Share this code with your peer: %s\n", code)

	cmd, err = c.ReadCommand()
	if err != nil {
		return fmt.Errorf("waiting for peer: %w", err)
	}
	if cmd.Verb != protocol.VerbPeerJoined {
		return fmt.Errorf("unexpected message from relay: %q", cmd.Verb)
	}
	fmt.Println("[system] peer connected")

	return runHostREPL(c)
}

// runHostREPL reads commands from stdin and dispatches them. Commands:
//
//	SEND <path>   send a file or directory
//	HELP          list commands
//	QUIT          close the connection
func runHostREPL(c *Conn) error {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 64*1024), 1<<20) // allow long paths
	fmt.Println("Commands: SEND <path>, HELP, QUIT")
	for {
		fmt.Print("host> ")
		if !in.Scan() {
			return in.Err()
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		verb, arg, _ := strings.Cut(line, " ")
		switch strings.ToUpper(verb) {
		case "HELP":
			fmt.Println("  SEND <path>   send a file or directory")
			fmt.Println("  HELP          show this list")
			fmt.Println("  QUIT          disconnect and exit")
		case "QUIT":
			return nil
		case "SEND":
			arg = strings.TrimSpace(arg)
			if arg == "" {
				fmt.Println("usage: SEND <path>")
				continue
			}
			if err := doSend(c, arg); err != nil {
				// Don't kill the REPL on a single failed transfer — the peer
				// might still be there. But if the conn itself broke, the
				// next frame I/O will surface that.
				fmt.Printf("[error] send failed: %v\n", err)
			}
		default:
			fmt.Printf("unknown command: %s (try HELP)\n", verb)
		}
	}
}

// doSend resolves path, kicks off transfer.Send, and prints progress.
func doSend(c *Conn, path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	fmt.Printf("sending %s ...\n", abs)
	start := time.Now()
	last := time.Now()
	err = transfer.Send(abs, c, c.Reader(), func(sent, total uint64) {
		// Throttle progress prints to ~5/sec; large files would otherwise
		// flood stdout.
		if time.Since(last) < 200*time.Millisecond && sent != total {
			return
		}
		last = time.Now()
		pct := float64(0)
		if total > 0 {
			pct = 100 * float64(sent) / float64(total)
		}
		fmt.Printf("\r  %s / %s (%.1f%%)",
			humanBytes(sent), humanBytes(total), pct)
		if sent == total {
			fmt.Println()
		}
	})
	if err != nil {
		return err
	}
	fmt.Printf("done in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}

// runJoinReceiver loops accepting transfers. Each Send from the host turns
// into one Receive call here. When the host disconnects (or QUITs) the read
// from the next frame returns io.EOF and we exit cleanly.
func runJoinReceiver(c *Conn) error {
	for {
		outDir, err := freshOutDir()
		if err != nil {
			return err
		}
		fmt.Printf("[system] waiting for transfer (will write to %s/)\n", outDir)

		start := time.Now()
		last := time.Now()
		err = transfer.Receive(outDir, c.Reader(), c, func(recv, total uint64) {
			if time.Since(last) < 200*time.Millisecond && recv != total {
				return
			}
			last = time.Now()
			pct := float64(0)
			if total > 0 {
				pct = 100 * float64(recv) / float64(total)
			}
			fmt.Printf("\r  %s / %s (%.1f%%)",
				humanBytes(recv), humanBytes(total), pct)
			if recv == total {
				fmt.Println()
			}
		})
		if err != nil {
			// EOF here is the normal "host disconnected" case.
			if err == io.EOF || strings.Contains(err.Error(), "EOF") {
				fmt.Println("\n[system] host disconnected")
				return nil
			}
			return err
		}
		fmt.Printf("received in %s\n", time.Since(start).Round(time.Millisecond))
	}
}

// freshOutDir picks the first available transfer_out / transfer_out-1 / ...
// directory name in the cwd.
func freshOutDir() (string, error) {
	base := "transfer_out"
	if _, err := os.Stat(base); os.IsNotExist(err) {
		return base, nil
	}
	for i := 1; i < 10000; i++ {
		name := fmt.Sprintf("%s-%d", base, i)
		if _, err := os.Stat(name); os.IsNotExist(err) {
			return name, nil
		}
	}
	return "", fmt.Errorf("could not find a free transfer_out directory")
}

func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for n2 := n / unit; n2 >= unit; n2 /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
