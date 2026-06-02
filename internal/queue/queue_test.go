package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func newTestQueue(t *testing.T) (*Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return &Queue{rdb: rdb}, mr
}

func TestEnqueueLeaseAck(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	id := uuid.New()
	env := Envelope{ID: id, Type: "echo", Priority: 5, MaxAttempts: 3, Payload: json.RawMessage(`{"hi":1}`)}
	if err := q.Enqueue(ctx, env); err != nil {
		t.Fatal(err)
	}
	depth, _ := q.QueueDepth(ctx, "echo")
	if depth != 1 {
		t.Fatalf("expected depth 1, got %d", depth)
	}
	leased, err := q.Lease(ctx, "echo", "w1", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if leased.ID != id {
		t.Fatalf("lease returned wrong id: %s vs %s", leased.ID, id)
	}
	// in-flight should hold it
	all, err := q.rdb.HGetAll(ctx, InflightKey("w1")).Result()
	if err != nil || len(all) != 1 {
		t.Fatalf("expected 1 inflight, got %v err=%v", all, err)
	}
	if err := q.Ack(ctx, "w1", id); err != nil {
		t.Fatal(err)
	}
	all, _ = q.rdb.HGetAll(ctx, InflightKey("w1")).Result()
	if len(all) != 0 {
		t.Fatalf("expected 0 after ack, got %v", all)
	}
}

func TestPriorityOrdering(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	low := Envelope{ID: uuid.New(), Type: "x", Priority: 9, EnqueuedAt: 1}
	high := Envelope{ID: uuid.New(), Type: "x", Priority: 1, EnqueuedAt: 100}
	mid := Envelope{ID: uuid.New(), Type: "x", Priority: 5, EnqueuedAt: 50}
	for _, e := range []Envelope{low, high, mid} {
		if err := q.Enqueue(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	got1, _ := q.Lease(ctx, "x", "w1", time.Second)
	got2, _ := q.Lease(ctx, "x", "w1", time.Second)
	got3, _ := q.Lease(ctx, "x", "w1", time.Second)
	if got1.ID != high.ID || got2.ID != mid.ID || got3.ID != low.ID {
		t.Fatalf("priority order wrong: %s,%s,%s", got1.ID, got2.ID, got3.ID)
	}
}

func TestLeaseEmpty(t *testing.T) {
	q, _ := newTestQueue(t)
	if _, err := q.Lease(context.Background(), "none", "w", time.Second); err != ErrEmpty {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}
}

func TestDelayedPromotion(t *testing.T) {
	q, mr := newTestQueue(t)
	ctx := context.Background()
	env := Envelope{ID: uuid.New(), Type: "z", Priority: 5}
	if err := q.EnqueueDelayed(ctx, env, time.Now().Add(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	depth, _ := q.QueueDepth(ctx, "z")
	if depth != 0 {
		t.Fatalf("expected 0 ready, got %d", depth)
	}
	mr.FastForward(100 * time.Millisecond)
	n, err := q.PromoteDelayed(ctx, "z", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 promoted, got %d", n)
	}
	depth, _ = q.QueueDepth(ctx, "z")
	if depth != 1 {
		t.Fatalf("expected ready 1 after promotion, got %d", depth)
	}
}

func TestReclaimInflight(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	env := Envelope{ID: uuid.New(), Type: "r", Priority: 1}
	if err := q.Enqueue(ctx, env); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Lease(ctx, "r", "deadw", time.Second); err != nil {
		t.Fatal(err)
	}
	envs, err := q.ReclaimInflight(ctx, "deadw")
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 1 || envs[0].ID != env.ID {
		t.Fatalf("reclaim mismatch: %+v", envs)
	}
	left, _ := q.rdb.HGetAll(ctx, InflightKey("deadw")).Result()
	if len(left) != 0 {
		t.Fatalf("expected inflight cleared, got %v", left)
	}
}

func TestDLQ(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	env := Envelope{ID: uuid.New(), Type: "boom", Priority: 5}
	if err := q.DeadLetter(ctx, env, "kaboom"); err != nil {
		t.Fatal(err)
	}
	n, _ := q.DLQDepth(ctx, "boom")
	if n != 1 {
		t.Fatalf("expected DLQ depth 1, got %d", n)
	}
}

func TestHeartbeat(t *testing.T) {
	q, mr := newTestQueue(t)
	ctx := context.Background()
	if err := q.Heartbeat(ctx, "w", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	alive, _ := q.IsAlive(ctx, "w")
	if !alive {
		t.Fatal("expected alive immediately after heartbeat")
	}
	mr.FastForward(150 * time.Millisecond)
	alive, _ = q.IsAlive(ctx, "w")
	if alive {
		t.Fatal("expected dead after TTL expiry")
	}
}
