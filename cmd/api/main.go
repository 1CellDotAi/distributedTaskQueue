// Command api runs the REST + WebSocket server.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/1CellDotAi/distributedTaskQueue/internal/api"
	"github.com/1CellDotAi/distributedTaskQueue/internal/config"
	"github.com/1CellDotAi/distributedTaskQueue/internal/queue"
	"github.com/1CellDotAi/distributedTaskQueue/internal/store"
	"github.com/1CellDotAi/distributedTaskQueue/internal/ws"
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

	hub := ws.NewHub(q.Client(), queue.EventsChannel)
	go func() {
		if err := hub.Run(ctx); err != nil {
			log.Printf("ws hub stopped: %v", err)
		}
	}()

	srv := &api.Server{Store: st, Queue: q, Hub: hub}
	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("api listening on %s (%s)", cfg.HTTPAddr, cfg)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("api shutting down...")
	shutdownCtx, c2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer c2()
	_ = httpSrv.Shutdown(shutdownCtx)
}
