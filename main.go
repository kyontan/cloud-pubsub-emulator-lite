package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8085"
	}

	server := NewServer()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.handleHealthCheck)
	mux.Handle("/", server)

	addr := fmt.Sprintf(":%s", port)
	slog.Info("starting server", "addr", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("failed to start server", "error", err.Error())
		os.Exit(1)
	}
}
