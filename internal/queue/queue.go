// Package queue implements a Redis-backed task queue with priority scheduling,
// delayed retries, in-flight tracking, and a dead-letter queue.
//
// Key layout:
//
//	queue:{type}        - ZSET, score = priority*1e13 + enqueue_ts (lower = higher priority / earlier)
//	delayed:{type}      - ZSET, score = available_at epoch ms
//	inflight:{worker}   - HASH, field=task_id, value=JSON envelope
//	hb:{worker}         - STRING with TTL (heartbeat)
//	dlq:{type}          - LIST of JSON envelopes
//	events:tasks        - Pub/Sub channel for status events
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Envelope is the payload stored in Redis structures. It carries enough info to
// reconstruct work without a Postgres round-trip on the hot path.
type Envelope struct {
	ID          uuid.UUID       `json:"id"`
	Type        string          `json:"type"`
	Priority    int             `json:"priority"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	Payload     json.RawMessage `json:"payload"`
	EnqueuedAt  int64           `json:"enqueued_at"`
}

// EventsChannel is the Redis pub/sub channel for task events.
const EventsChannel = "events:tasks"

// Queue is the high-level Redis queue façade.
type Queue struct {
	rdb *redis.Client
}

// New constructs a Queue from a redis:// URL.
func New(url string) (*Queue, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(opt)
	return &Queue{rdb: rdb}, nil
}

// Client returns the underlying Redis client for advanced use (pub/sub).
func (q *Queue) Client() *redis.Client { return q.rdb }

// Close releases the Redis client.
func (q *Queue) Close() error { return q.rdb.Close() }

// Ping verifies the connection.
func (q *Queue) Ping(ctx context.Context) error { return q.rdb.Ping(ctx).Err() }

// QueueKey returns the ZSET key for ready tasks of a given type.
func QueueKey(typ string) string { return "queue:" + typ }

// DelayedKey returns the ZSET key for delayed tasks of a given type.
func DelayedKey(typ string) string { return "delayed:" + typ }

// InflightKey returns the HASH key for a worker's in-flight tasks.
func InflightKey(worker string) string { return "inflight:" + worker }

// HeartbeatKey returns the heartbeat key for a worker.
func HeartbeatKey(worker string) string { return "hb:" + worker }

// DLQKey returns the list key for dead-letter tasks of a given type.
func DLQKey(typ string) string { return "dlq:" + typ }

// priorityScore packs priority and enqueue time so that lower scores pop first.
// priority is clamped to [0, 9]; lower numeric priority = more urgent.
func priorityScore(priority int, enqueueMs int64) float64 {
	if priority < 0 {
		priority = 0
	}
	if priority > 9 {
		priority = 9
	}
	return float64(priority)*1e13 + float64(enqueueMs)
}

// Enqueue adds a task to its type's ready queue.
func (q *Queue) Enqueue(ctx context.Context, env Envelope) error {
	if env.EnqueuedAt == 0 {
		env.EnqueuedAt = time.Now().UnixMilli()
	}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	score := priorityScore(env.Priority, env.EnqueuedAt)
	return q.rdb.ZAdd(ctx, QueueKey(env.Type), redis.Z{Score: score, Member: string(body)}).Err()
}

// EnqueueDelayed schedules a task to become available at `at`.
func (q *Queue) EnqueueDelayed(ctx context.Context, env Envelope, at time.Time) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return q.rdb.ZAdd(ctx, DelayedKey(env.Type), redis.Z{Score: float64(at.UnixMilli()), Member: string(body)}).Err()
}

// leaseScript atomically pops the highest-priority task from queue:{type}
// and stores it in inflight:{worker} keyed by task id. Returns the envelope JSON.
//
// KEYS[1] = queue:{type}
// KEYS[2] = inflight:{worker}
// ARGV[1] = lease ttl seconds (informational; TTLs handled by heartbeat key)
var leaseScript = redis.NewScript(`
local popped = redis.call('ZPOPMIN', KEYS[1], 1)
if #popped == 0 then return nil end
local member = popped[1]
local decoded = cjson.decode(member)
redis.call('HSET', KEYS[2], decoded.id, member)
return member
`)

// Lease pops the next task for the given type and records it as in-flight for `worker`.
// Returns ErrEmpty when no work is available.
func (q *Queue) Lease(ctx context.Context, typ, worker string, leaseTTL time.Duration) (*Envelope, error) {
	res, err := leaseScript.Run(ctx, q.rdb,
		[]string{QueueKey(typ), InflightKey(worker)},
		int64(leaseTTL.Seconds()),
	).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrEmpty
		}
		return nil, err
	}
	s, ok := res.(string)
	if !ok {
		return nil, ErrEmpty
	}
	var env Envelope
	if err := json.Unmarshal([]byte(s), &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	return &env, nil
}

// LeaseAny attempts to lease from any of the supplied types, in order.
func (q *Queue) LeaseAny(ctx context.Context, types []string, worker string, leaseTTL time.Duration) (*Envelope, error) {
	for _, t := range types {
		env, err := q.Lease(ctx, t, worker, leaseTTL)
		if err == nil {
			return env, nil
		}
		if !errors.Is(err, ErrEmpty) {
			return nil, err
		}
	}
	return nil, ErrEmpty
}

// Ack removes an in-flight task after successful processing.
func (q *Queue) Ack(ctx context.Context, worker string, id uuid.UUID) error {
	return q.rdb.HDel(ctx, InflightKey(worker), id.String()).Err()
}

// Nack removes the in-flight record without requeueing; the caller decides where it goes
// (retry via EnqueueDelayed, or DLQ).
func (q *Queue) Nack(ctx context.Context, worker string, id uuid.UUID) error {
	return q.rdb.HDel(ctx, InflightKey(worker), id.String()).Err()
}

// DeadLetter pushes a task to the dead-letter list for its type.
func (q *Queue) DeadLetter(ctx context.Context, env Envelope, reason string) error {
	wrap := struct {
		Envelope
		Reason   string `json:"reason"`
		FailedAt int64  `json:"failed_at"`
	}{env, reason, time.Now().UnixMilli()}
	body, err := json.Marshal(wrap)
	if err != nil {
		return err
	}
	return q.rdb.RPush(ctx, DLQKey(env.Type), body).Err()
}

// Heartbeat refreshes the worker heartbeat key with TTL.
func (q *Queue) Heartbeat(ctx context.Context, worker string, ttl time.Duration) error {
	return q.rdb.Set(ctx, HeartbeatKey(worker), time.Now().UnixMilli(), ttl).Err()
}

// IsAlive reports whether the worker's heartbeat key is still present.
func (q *Queue) IsAlive(ctx context.Context, worker string) (bool, error) {
	n, err := q.rdb.Exists(ctx, HeartbeatKey(worker)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ReclaimInflight removes all in-flight envelopes for a (dead) worker and returns them.
func (q *Queue) ReclaimInflight(ctx context.Context, worker string) ([]Envelope, error) {
	key := InflightKey(worker)
	all, err := q.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	var envs []Envelope
	for _, v := range all {
		var env Envelope
		if err := json.Unmarshal([]byte(v), &env); err != nil {
			continue
		}
		envs = append(envs, env)
	}
	if len(all) > 0 {
		_ = q.rdb.Del(ctx, key).Err()
	}
	return envs, nil
}

// PromoteDelayed moves any tasks in delayed:{type} whose score (available_at) is <= now
// into queue:{type}. Returns the number of tasks promoted.
func (q *Queue) PromoteDelayed(ctx context.Context, typ string, now time.Time) (int, error) {
	nowMs := now.UnixMilli()
	members, err := q.rdb.ZRangeByScore(ctx, DelayedKey(typ), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", nowMs),
		Offset: 0, Count: 256,
	}).Result()
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}
	pipe := q.rdb.TxPipeline()
	for _, m := range members {
		var env Envelope
		if err := json.Unmarshal([]byte(m), &env); err != nil {
			continue
		}
		env.EnqueuedAt = nowMs
		body, err := json.Marshal(env)
		if err != nil {
			continue
		}
		pipe.ZRem(ctx, DelayedKey(typ), m)
		pipe.ZAdd(ctx, QueueKey(env.Type), redis.Z{Score: priorityScore(env.Priority, nowMs), Member: string(body)})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return len(members), nil
}

// QueueDepth returns the count of ready tasks for a type.
func (q *Queue) QueueDepth(ctx context.Context, typ string) (int64, error) {
	return q.rdb.ZCard(ctx, QueueKey(typ)).Result()
}

// DelayedDepth returns the count of delayed tasks for a type.
func (q *Queue) DelayedDepth(ctx context.Context, typ string) (int64, error) {
	return q.rdb.ZCard(ctx, DelayedKey(typ)).Result()
}

// DLQDepth returns the count of dead-letter tasks for a type.
func (q *Queue) DLQDepth(ctx context.Context, typ string) (int64, error) {
	return q.rdb.LLen(ctx, DLQKey(typ)).Result()
}

// ListTypes scans Redis for queue keys and returns the discovered task types.
func (q *Queue) ListTypes(ctx context.Context) ([]string, error) {
	var (
		cursor uint64
		types  = map[string]struct{}{}
	)
	for {
		keys, c, err := q.rdb.Scan(ctx, cursor, "queue:*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			types[strings.TrimPrefix(k, "queue:")] = struct{}{}
		}
		cursor = c
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

// Publish broadcasts a task event on the events pub/sub channel.
func (q *Queue) Publish(ctx context.Context, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return q.rdb.Publish(ctx, EventsChannel, body).Err()
}

// ErrEmpty indicates that no task was available to lease.
var ErrEmpty = errors.New("queue empty")
