package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/elliota43/wormbeam/internal/client"
)

const defaultRelay = "16.59.53.129:8080"

func main() {
	relayAddr := flag.String("relay", defaultRelay, "relay server address")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}

	c, err := client.Dial(*relayAddr)
	if err != nil {
		log.Fatal(err)
	}

	defer c.Close()

	switch args[0] {
	case "host":
		err = client.Host(c)

	case "join":
		if len(args) < 2 {
			log.Fatalf("usage: %s join <code>", os.Args[0])
		}
		err = client.Join(c, args[1])

	default:
		usage()
		os.Exit(2)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [-relay addr] host\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "       %s [-relay addr] join <code>\n", os.Args[0])
}
