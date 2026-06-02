// Command worker runs a task-queue worker that consumes from Redis and executes
// registered task handlers.
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
	"github.com/1CellDotAi/distributedTaskQueue/internal/task"
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

	reg := task.NewRegistry()
	task.RegisterDefaults(reg)

	w := scheduler.NewWorker(cfg, q, st, reg)
	if err := w.Run(ctx); err != nil {
		log.Fatalf("worker: %v", err)
	}
}
