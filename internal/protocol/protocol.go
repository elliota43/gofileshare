package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Command is one parsed line from the relay handshake.
type Command struct {
	Verb string // "HOST", "JOIN", "OK", "ERR", "PEER_JOINED", or a 4-digit code
	Arg  string // empty for most messages; the code for JOIN; reason for ERR;
}

const (
	VerbHost       = "HOST"
	VerbJoin       = "JOIN"
	VerbOK         = "OK"
	VerbErr        = "ERR"
	VerbPeerJoined = "PEER_JOINED"
)

// ReadCommand reads one '\n' terminated line and parses it.
func ReadCommand(r *bufio.Reader) (Command, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return Command{}, err
	}

	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return Command{}, errors.New("empty command")
	}

	verb, arg, _ := strings.Cut(line, " ")
	return Command{Verb: verb, Arg: arg}, nil
}

func WriteHost(w io.Writer) error              { return writeLine(w, VerbHost) }
func WriteJoin(w io.Writer, code string) error { return writeLine(w, VerbJoin+" "+code) }
func WriteCode(w io.Writer, code string) error { return writeLine(w, code) }
func WriteOK(w io.Writer) error                { return writeLine(w, VerbOK) }
func WriteErr(w io.Writer, reason string) error {
	return writeLine(w, VerbErr+" "+reason)
}

func WritePeerJoined(w io.Writer) error { return writeLine(w, VerbPeerJoined) }

func writeLine(w io.Writer, s string) error {
	_, err := fmt.Fprintln(w, s)
	return err
}
