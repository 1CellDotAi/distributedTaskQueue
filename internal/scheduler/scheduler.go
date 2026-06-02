// Package scheduler contains the worker runtime and the coordinator that handles
// retries, delayed promotion, and orphan reclaim from dead workers.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/1CellDotAi/distributedTaskQueue/internal/config"
	"github.com/1CellDotAi/distributedTaskQueue/internal/queue"
	"github.com/1CellDotAi/distributedTaskQueue/internal/store"
	"github.com/1CellDotAi/distributedTaskQueue/internal/task"
)

// Worker pulls tasks from Redis, executes their handlers, and reports outcomes.
type Worker struct {
	ID       string
	Hostname string

	cfg      config.Config
	queue    *queue.Queue
	store    *store.Store
	registry *task.Registry

	// Counters exposed for tests / metrics.
	Completed atomic.Int64
	Failed    atomic.Int64
	Dead      atomic.Int64
}

// NewWorker constructs a Worker. ID is generated if blank.
func NewWorker(cfg config.Config, q *queue.Queue, s *store.Store, reg *task.Registry) *Worker {
	id := os.Getenv("WORKER_ID")
	if id == "" {
		id = "worker-" + uuid.NewString()[:8]
	}
	host, _ := os.Hostname()
	return &Worker{ID: id, Hostname: host, cfg: cfg, queue: q, store: s, registry: reg}
}

// Run blocks until ctx is canceled, processing tasks concurrently up to cfg.WorkerConcurrency.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.store.UpsertWorker(ctx, w.ID, w.Hostname, "alive"); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	if err := w.queue.Heartbeat(ctx, w.ID, w.cfg.HeartbeatTTL); err != nil {
		return fmt.Errorf("initial heartbeat: %w", err)
	}
	log.Printf("worker %s started (host=%s)", w.ID, w.Hostname)

	hbStop := make(chan struct{})
	go w.heartbeatLoop(ctx, hbStop)

	sem := make(chan struct{}, w.cfg.WorkerConcurrency)
	var wg sync.WaitGroup

	types := w.cfg.TaskTypes
	if len(types) == 0 {
		types = w.registry.Types()
	}
	if len(types) == 0 {
		return errors.New("no task handlers registered and no TASK_TYPES configured")
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %s shutting down...", w.ID)
			close(hbStop)
			wg.Wait()
			_ = w.store.MarkWorkerDead(context.Background(), w.ID)
			return nil
		case sem <- struct{}{}:
		}

		env, err := w.queue.LeaseAny(ctx, types, w.ID, w.cfg.LeaseTTL)
		if err != nil {
			<-sem
			if errors.Is(err, queue.ErrEmpty) {
				select {
				case <-ctx.Done():
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}
			log.Printf("lease error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		wg.Add(1)
		go func(env queue.Envelope) {
			defer wg.Done()
			defer func() { <-sem }()
			w.execute(ctx, env)
		}(*env)
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context, stop <-chan struct{}) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			if err := w.queue.Heartbeat(ctx, w.ID, w.cfg.HeartbeatTTL); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
			_ = w.store.TouchWorker(ctx, w.ID)
		}
	}
}

func (w *Worker) execute(ctx context.Context, env queue.Envelope) {
	h := w.registry.Lookup(env.Type)
	if h == nil {
		log.Printf("no handler for type %q (task %s); dead-lettering", env.Type, env.ID)
		_ = w.queue.Nack(ctx, w.ID, env.ID)
		_ = w.queue.DeadLetter(ctx, env, "no handler registered")
		_ = w.store.MarkDead(ctx, env.ID, "no handler registered")
		w.publishEvent(env, task.StatusDead, "no handler registered")
		w.Dead.Add(1)
		return
	}

	if err := w.store.MarkRunning(ctx, env.ID, w.ID); err != nil {
		log.Printf("mark running failed for %s: %v", env.ID, err)
	}
	w.publishEvent(env, task.StatusRunning, "")

	jobCtx, cancel := context.WithTimeout(ctx, w.cfg.LeaseTTL)
	defer cancel()
	result, err := h.Handle(jobCtx, task.Task{
		ID: env.ID, Type: env.Type, Payload: env.Payload,
		Priority: env.Priority, Attempts: env.Attempts, MaxAttempts: env.MaxAttempts,
	})

	if err == nil {
		_ = w.queue.Ack(ctx, w.ID, env.ID)
		_ = w.store.MarkSucceeded(ctx, env.ID, result)
		w.publishEvent(env, task.StatusSucceeded, "")
		w.Completed.Add(1)
		return
	}

	w.Failed.Add(1)
	env.Attempts++
	_ = w.queue.Nack(ctx, w.ID, env.ID)

	if env.Attempts >= env.MaxAttempts {
		_ = w.queue.DeadLetter(ctx, env, err.Error())
		_ = w.store.MarkDead(ctx, env.ID, err.Error())
		w.publishEvent(env, task.StatusDead, err.Error())
		w.Dead.Add(1)
		return
	}

	delay := BackoffDelay(env.Attempts, w.cfg.RetryBaseDelay, w.cfg.RetryMaxDelay)
	availableAt := time.Now().Add(delay)
	if err := w.queue.EnqueueDelayed(ctx, env, availableAt); err != nil {
		log.Printf("enqueue delayed failed for %s: %v", env.ID, err)
	}
	_ = w.store.MarkRetry(ctx, env.ID, availableAt, err.Error())
	w.publishEvent(env, task.StatusRetrying, err.Error())
}

func (w *Worker) publishEvent(env queue.Envelope, status task.Status, msg string) {
	ev := task.Event{
		TaskID: env.ID, Type: env.Type, Status: status, WorkerID: w.ID,
		Message: msg, Timestamp: time.Now().UTC(),
	}
	if err := w.queue.Publish(context.Background(), ev); err != nil {
		log.Printf("publish event failed: %v", err)
	}
}

// BackoffDelay returns an exponential backoff with full jitter, capped at max.
func BackoffDelay(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	if exp > float64(max) {
		exp = float64(max)
	}
	// full jitter
	d := time.Duration(rand.Float64() * exp)
	if d < base {
		d = base
	}
	return d
}

// Coordinator runs periodic background tasks: promote delayed → ready, detect
// dead workers (no heartbeat) and reclaim their in-flight tasks.
type Coordinator struct {
	cfg   config.Config
	queue *queue.Queue
	store *store.Store
}

// NewCoordinator builds a Coordinator.
func NewCoordinator(cfg config.Config, q *queue.Queue, s *store.Store) *Coordinator {
	return &Coordinator{cfg: cfg, queue: q, store: s}
}

// Run blocks until ctx is canceled.
func (c *Coordinator) Run(ctx context.Context) error {
	t := time.NewTicker(c.cfg.CoordinatorSweepInterval)
	defer t.Stop()
	log.Printf("coordinator started (sweep=%s)", c.cfg.CoordinatorSweepInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			c.tick(ctx)
		}
	}
}

func (c *Coordinator) tick(ctx context.Context) {
	c.promoteDelayed(ctx)
	c.reclaimDead(ctx)
}

func (c *Coordinator) promoteDelayed(ctx context.Context) {
	types, err := c.queue.ListTypes(ctx)
	if err != nil {
		log.Printf("list types: %v", err)
		return
	}
	// Also include delayed-only types by scanning delayed:*.
	delayedTypes, _ := c.scanDelayedTypes(ctx)
	seen := map[string]struct{}{}
	for _, t := range append(types, delayedTypes...) {
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		n, err := c.queue.PromoteDelayed(ctx, t, time.Now())
		if err != nil {
			log.Printf("promote %s: %v", t, err)
		}
		if n > 0 {
			log.Printf("promoted %d delayed tasks for type=%s", n, t)
		}
	}
}

func (c *Coordinator) scanDelayedTypes(ctx context.Context) ([]string, error) {
	var (
		cursor uint64
		types  = map[string]struct{}{}
	)
	rdb := c.queue.Client()
	for {
		keys, cur, err := rdb.Scan(ctx, cursor, "delayed:*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			types[k[len("delayed:"):]] = struct{}{}
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
	out := make([]string, 0, len(types))
	for t := range types {
		out = append(out, t)
	}
	return out, nil
}

func (c *Coordinator) reclaimDead(ctx context.Context) {
	workers, err := c.store.ListWorkers(ctx)
	if err != nil {
		log.Printf("list workers: %v", err)
		return
	}
	for _, w := range workers {
		if w.Status == "dead" {
			continue
		}
		alive, err := c.queue.IsAlive(ctx, w.ID)
		if err != nil {
			log.Printf("alive check %s: %v", w.ID, err)
			continue
		}
		if alive {
			continue
		}
		log.Printf("worker %s detected dead, reclaiming", w.ID)
		envs, err := c.queue.ReclaimInflight(ctx, w.ID)
		if err != nil {
			log.Printf("reclaim %s: %v", w.ID, err)
			continue
		}
		for _, env := range envs {
			// Requeue immediately; counts as a retry attempt-wise so we don't loop forever.
			env.Attempts++
			if env.Attempts >= env.MaxAttempts {
				_ = c.queue.DeadLetter(ctx, env, "worker died")
				_ = c.store.MarkDead(ctx, env.ID, "worker died mid-task")
				c.publishEvent(env, task.StatusDead, "worker died mid-task")
				continue
			}
			if err := c.queue.Enqueue(ctx, env); err != nil {
				log.Printf("requeue %s: %v", env.ID, err)
				continue
			}
			c.publishEvent(env, task.StatusQueued, "reassigned from dead worker "+w.ID)
		}
		// Also flip any DB rows that were running on this worker back to queued
		// (covers races where Redis inflight was already cleared).
		if _, err := c.store.RequeueOrphans(ctx, []string{w.ID}); err != nil {
			log.Printf("requeue orphans %s: %v", w.ID, err)
		}
		_ = c.store.MarkWorkerDead(ctx, w.ID)
	}
}

func (c *Coordinator) publishEvent(env queue.Envelope, status task.Status, msg string) {
	ev := task.Event{
		TaskID: env.ID, Type: env.Type, Status: status,
		Message: msg, Timestamp: time.Now().UTC(),
	}
	body, _ := json.Marshal(ev)
	_ = c.queue.Client().Publish(context.Background(), queue.EventsChannel, body).Err()
}
