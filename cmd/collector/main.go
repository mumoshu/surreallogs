package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/surrealdb/surreallogs/internal/collector"
	"github.com/surrealdb/surreallogs/internal/config"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := collector.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create collector: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	if err := c.Run(ctx); err != nil {
		log.Fatalf("Collector failed: %v", err)
	}
}
