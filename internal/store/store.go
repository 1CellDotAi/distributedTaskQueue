// Package store provides a PostgreSQL-backed repository for tasks, workers, and events.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/1CellDotAi/distributedTaskQueue/internal/task"
)

//go:embed migrations/0001_init.sql
var initSQL string

// Store wraps a pgx connection pool and exposes typed task/worker operations.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to PostgreSQL using the supplied connection string and returns a Store.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the underlying connection pool.
func (s *Store) Close() { s.pool.Close() }

// Pool returns the underlying pgx pool. Intended for tests / advanced usage.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies the embedded schema. Safe to call repeatedly.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, initSQL)
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// CreateTask inserts a new task in status `queued`. ID is generated if zero.
func (s *Store) CreateTask(ctx context.Context, t *task.Task) error {
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	if t.MaxAttempts <= 0 {
		t.MaxAttempts = 3
	}
	if t.Status == "" {
		t.Status = task.StatusQueued
	}
	if t.AvailableAt.IsZero() {
		t.AvailableAt = time.Now().UTC()
	}
	if len(t.Payload) == 0 {
		t.Payload = json.RawMessage("{}")
	}
	now := time.Now().UTC()
	t.CreatedAt = now
	t.UpdatedAt = now
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tasks (id, type, payload, priority, status, attempts, max_attempts, available_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, t.ID, t.Type, []byte(t.Payload), t.Priority, t.Status, t.Attempts, t.MaxAttempts, t.AvailableAt, t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	return s.LogEvent(ctx, t.ID, "created", "")
}

// GetTask returns a task by ID. Returns pgx.ErrNoRows if not found.
func (s *Store) GetTask(ctx context.Context, id uuid.UUID) (*task.Task, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, type, payload, priority, status, attempts, max_attempts, available_at,
		       started_at, finished_at, worker_id, result, last_error, created_at, updated_at
		FROM tasks WHERE id=$1
	`, id)
	return scanTask(row)
}

// ListTasks returns up to `limit` tasks filtered by optional status/type, ordered by creation time desc.
func (s *Store) ListTasks(ctx context.Context, status, typ string, limit int) ([]task.Task, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, payload, priority, status, attempts, max_attempts, available_at,
		       started_at, finished_at, worker_id, result, last_error, created_at, updated_at
		FROM tasks
		WHERE ($1='' OR status=$1) AND ($2='' OR type=$2)
		ORDER BY created_at DESC
		LIMIT $3
	`, status, typ, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []task.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

// MarkRunning transitions a task to running and records the worker.
func (s *Store) MarkRunning(ctx context.Context, id uuid.UUID, workerID string) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status=$1, started_at=$2, worker_id=$3, updated_at=$2,
		                 attempts = attempts + 1
		WHERE id=$4
	`, task.StatusRunning, now, workerID, id)
	if err != nil {
		return err
	}
	return s.LogEvent(ctx, id, "started", "worker="+workerID)
}

// MarkSucceeded transitions a task to succeeded and stores its result.
func (s *Store) MarkSucceeded(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	now := time.Now().UTC()
	if len(result) == 0 {
		result = json.RawMessage("null")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status=$1, finished_at=$2, updated_at=$2, result=$3
		WHERE id=$4
	`, task.StatusSucceeded, now, []byte(result), id)
	if err != nil {
		return err
	}
	return s.LogEvent(ctx, id, "succeeded", "")
}

// MarkRetry schedules the task for a future retry attempt.
func (s *Store) MarkRetry(ctx context.Context, id uuid.UUID, availableAt time.Time, lastErr string) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status=$1, available_at=$2, last_error=$3, worker_id=NULL, updated_at=$4
		WHERE id=$5
	`, task.StatusRetrying, availableAt, lastErr, now, id)
	if err != nil {
		return err
	}
	return s.LogEvent(ctx, id, "retry_scheduled", lastErr)
}

// MarkDead transitions a task to the dead-letter state and inserts a DLQ row.
func (s *Store) MarkDead(ctx context.Context, id uuid.UUID, lastErr string) error {
	now := time.Now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var payload []byte
	if err := tx.QueryRow(ctx, `
		UPDATE tasks SET status=$1, finished_at=$2, updated_at=$2, last_error=$3
		WHERE id=$4 RETURNING payload
	`, task.StatusDead, now, lastErr, id).Scan(&payload); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO dead_letters (task_id, reason, payload, failed_at)
		VALUES ($1, $2, $3, $4)
	`, id, lastErr, payload, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO task_events (task_id, event_type, message) VALUES ($1, $2, $3)
	`, id, "dead", lastErr); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// MarkCanceled transitions a task to canceled state.
func (s *Store) MarkCanceled(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status=$1, finished_at=$2, updated_at=$2
		WHERE id=$3 AND status IN ('queued','retrying')
	`, task.StatusCanceled, now, id)
	if err != nil {
		return err
	}
	return s.LogEvent(ctx, id, "canceled", "")
}

// RequeueOrphans returns IDs of tasks that are running on the supplied dead workers
// and resets them to queued so they can be picked up again. Returns the affected tasks.
func (s *Store) RequeueOrphans(ctx context.Context, deadWorkerIDs []string) ([]task.Task, error) {
	if len(deadWorkerIDs) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE tasks SET status='queued', worker_id=NULL, available_at=now(), updated_at=now()
		WHERE status='running' AND worker_id = ANY($1)
		RETURNING id, type, payload, priority, status, attempts, max_attempts, available_at,
		          started_at, finished_at, worker_id, result, last_error, created_at, updated_at
	`, deadWorkerIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []task.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		_ = s.LogEvent(ctx, t.ID, "reassigned", "previous worker dead")
		out = append(out, *t)
	}
	return out, rows.Err()
}

// UpsertWorker records a worker registration / status update.
func (s *Store) UpsertWorker(ctx context.Context, id, hostname, status string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (id, hostname, status, last_heartbeat)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE SET hostname=EXCLUDED.hostname, status=EXCLUDED.status, last_heartbeat=now()
	`, id, hostname, status)
	return err
}

// TouchWorker updates a worker's heartbeat timestamp.
func (s *Store) TouchWorker(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE workers SET last_heartbeat=now(), status='alive' WHERE id=$1`, id)
	return err
}

// MarkWorkerDead flags a worker as dead.
func (s *Store) MarkWorkerDead(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE workers SET status='dead' WHERE id=$1`, id)
	return err
}

// Worker represents a worker registration row.
type Worker struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	Status        string    `json:"status"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// ListWorkers returns all known workers.
func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, hostname, status, last_heartbeat FROM workers ORDER BY last_heartbeat DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		if err := rows.Scan(&w.ID, &w.Hostname, &w.Status, &w.LastHeartbeat); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// StaleWorkers returns worker IDs whose last_heartbeat is older than `since`.
func (s *Store) StaleWorkers(ctx context.Context, threshold time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM workers WHERE status <> 'dead' AND last_heartbeat < $1`, threshold)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// LogEvent appends a row to task_events. Failures are returned but non-fatal for callers.
func (s *Store) LogEvent(ctx context.Context, id uuid.UUID, eventType, message string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO task_events (task_id, event_type, message) VALUES ($1,$2,$3)`, id, eventType, message)
	return err
}

// TaskEvent is a row from task_events.
type TaskEvent struct {
	ID        int64     `json:"id"`
	TaskID    uuid.UUID `json:"task_id"`
	EventType string    `json:"event_type"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

// ListTaskEvents returns the audit trail for a single task.
func (s *Store) ListTaskEvents(ctx context.Context, id uuid.UUID) ([]TaskEvent, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, task_id, event_type, message, created_at FROM task_events WHERE task_id=$1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TaskEvent
	for rows.Next() {
		var e TaskEvent
		if err := rows.Scan(&e.ID, &e.TaskID, &e.EventType, &e.Message, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// DeadLetter represents a row from the dead_letters table.
type DeadLetter struct {
	ID       int64           `json:"id"`
	TaskID   uuid.UUID       `json:"task_id"`
	Reason   string          `json:"reason"`
	Payload  json.RawMessage `json:"payload"`
	FailedAt time.Time       `json:"failed_at"`
}

// ListDeadLetters returns up to `limit` dead-letter records.
func (s *Store) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT id, task_id, reason, payload, failed_at FROM dead_letters ORDER BY failed_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadLetter
	for rows.Next() {
		var d DeadLetter
		var payload []byte
		if err := rows.Scan(&d.ID, &d.TaskID, &d.Reason, &payload, &d.FailedAt); err != nil {
			return nil, err
		}
		d.Payload = payload
		out = append(out, d)
	}
	return out, rows.Err()
}

// Redrive resets a dead task back to queued so it can be retried.
func (s *Store) Redrive(ctx context.Context, id uuid.UUID) (*task.Task, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE tasks SET status='queued', attempts=0, last_error='', available_at=now(),
		                 worker_id=NULL, started_at=NULL, finished_at=NULL, updated_at=now()
		WHERE id=$1 AND status='dead'
		RETURNING id, type, payload, priority, status, attempts, max_attempts, available_at,
		          started_at, finished_at, worker_id, result, last_error, created_at, updated_at
	`, id)
	t, err := scanTask(row)
	if err != nil {
		return nil, err
	}
	_ = s.LogEvent(ctx, id, "redriven", "")
	return t, nil
}

// Stats returns simple aggregate counters used by the dashboard.
type Stats struct {
	ByStatus map[string]int64 `json:"by_status"`
	ByType   map[string]int64 `json:"by_type"`
	Total    int64            `json:"total"`
}

// GetStats returns task counts grouped by status and type.
func (s *Store) GetStats(ctx context.Context) (Stats, error) {
	st := Stats{ByStatus: map[string]int64{}, ByType: map[string]int64{}}
	rows, err := s.pool.Query(ctx, `SELECT status, COUNT(*) FROM tasks GROUP BY status`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return st, err
		}
		st.ByStatus[k] = v
		st.Total += v
	}
	rows.Close()
	rows, err = s.pool.Query(ctx, `SELECT type, COUNT(*) FROM tasks GROUP BY type`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			return st, err
		}
		st.ByType[k] = v
	}
	return st, nil
}

// rowScanner abstracts pgx.Row and pgx.Rows for scanTask.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanTask(r rowScanner) (*task.Task, error) {
	var t task.Task
	var payload, result []byte
	var workerID *string
	var startedAt, finishedAt *time.Time
	err := r.Scan(&t.ID, &t.Type, &payload, &t.Priority, &t.Status, &t.Attempts, &t.MaxAttempts, &t.AvailableAt,
		&startedAt, &finishedAt, &workerID, &result, &t.LastError, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.Payload = payload
	if len(result) > 0 {
		t.Result = result
	}
	t.WorkerID = workerID
	t.StartedAt = startedAt
	t.FinishedAt = finishedAt
	return &t, nil
}

// ErrNotFound is returned when a lookup matches no rows.
var ErrNotFound = errors.New("not found")
