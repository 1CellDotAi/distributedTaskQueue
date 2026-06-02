// Package task defines the Task domain model and handler interfaces shared across services.
package task

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status represents the lifecycle state of a task.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusRetrying  Status = "retrying"
	StatusDead      Status = "dead"
	StatusCanceled  Status = "canceled"
)

// Task is the canonical task record persisted in PostgreSQL.
type Task struct {
	ID          uuid.UUID       `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	AvailableAt time.Time       `json:"available_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`
	WorkerID    *string         `json:"worker_id,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// Handler executes a single task. Implementations should be idempotent.
type Handler interface {
	Handle(ctx context.Context, t Task) (json.RawMessage, error)
}

// HandlerFunc is an adapter to allow ordinary functions to act as Handlers.
type HandlerFunc func(ctx context.Context, t Task) (json.RawMessage, error)

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, t Task) (json.RawMessage, error) {
	return f(ctx, t)
}

// Registry maps task types to handlers.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty handler registry.
func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }

// Register associates a task type with a handler.
func (r *Registry) Register(taskType string, h Handler) { r.handlers[taskType] = h }

// Lookup returns the handler for a task type, or nil if none is registered.
func (r *Registry) Lookup(taskType string) Handler { return r.handlers[taskType] }

// Types returns the list of registered task types.
func (r *Registry) Types() []string {
	out := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		out = append(out, k)
	}
	return out
}

// Event represents a state-change event broadcast to subscribers.
type Event struct {
	TaskID    uuid.UUID `json:"task_id"`
	Type      string    `json:"type"`
	Status    Status    `json:"status"`
	WorkerID  string    `json:"worker_id,omitempty"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}
