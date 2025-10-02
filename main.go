package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	// Command-line flags
	host := flag.String("h", "", "host to listen on (default: all interfaces)")
	port := flag.String("p", "8085", "port to listen on")
	flag.Parse()

	server := NewServer()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.handleHealthCheck)
	mux.Handle("/", server)

	addr := fmt.Sprintf("%s:%s", *host, *port)
	slog.Info("starting server", "addr", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("failed to start server", "error", err.Error())
		os.Exit(1)
	}
}
