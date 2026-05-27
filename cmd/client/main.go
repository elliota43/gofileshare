package main

import (
	"flag"
	"log"

	"github.com/elliota43/wormbeam/internal/relay"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	s := relay.New(nil)
	log.Fatal(s.ListenAndServe(*addr))
}
