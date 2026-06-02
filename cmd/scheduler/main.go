// Command scheduler runs the coordinator: promotes delayed tasks and reclaims
// in-flight tasks from dead workers.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/1CellDotAi/distributedTaskQueue/internal/config"
	"github.com/1CellDotAi/distributedTaskQueue/internal/queue"
	"github.com/1CellDotAi/distributedTaskQueue/internal/scheduler"
	"github.com/1CellDotAi/distributedTaskQueue/internal/store"
)

func main() {
	cfg := config.Load()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	q, err := queue.New(cfg.RedisURL)
	if err != nil {
		log.Fatalf("queue: %v", err)
	}
	defer q.Close()

	c := scheduler.NewCoordinator(cfg, q, st)
	if err := c.Run(ctx); err != nil {
		log.Fatalf("coordinator: %v", err)
	}
}
