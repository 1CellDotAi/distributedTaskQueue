// Package api wires HTTP handlers for the task-queue REST surface.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/1CellDotAi/distributedTaskQueue/internal/queue"
	"github.com/1CellDotAi/distributedTaskQueue/internal/store"
	"github.com/1CellDotAi/distributedTaskQueue/internal/task"
	"github.com/1CellDotAi/distributedTaskQueue/internal/ws"
)

// Server bundles the HTTP router with its dependencies.
type Server struct {
	Store *store.Store
	Queue *queue.Queue
	Hub   *ws.Hub
}

// Router builds the chi router with all endpoints registered.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	r.Get("/readyz", s.readyz)

	r.Route("/api", func(r chi.Router) {
		r.Post("/tasks", s.createTask)
		r.Get("/tasks", s.listTasks)
		r.Get("/tasks/{id}", s.getTask)
		r.Get("/tasks/{id}/events", s.getTaskEvents)
		r.Post("/tasks/{id}/cancel", s.cancelTask)

		r.Get("/dlq", s.listDLQ)
		r.Post("/dlq/{id}/redrive", s.redriveDLQ)

		r.Get("/workers", s.listWorkers)
		r.Get("/stats", s.getStats)
	})

	if s.Hub != nil {
		r.Get("/ws", s.Hub.ServeHTTP)
	}
	return r
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Queue.Ping(ctx); err != nil {
		http.Error(w, "redis: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	if err := s.Store.Pool().Ping(ctx); err != nil {
		http.Error(w, "postgres: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ready"))
}

// CreateTaskRequest is the JSON body accepted by POST /api/tasks.
type CreateTaskRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	MaxAttempts int             `json:"max_attempts"`
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage("{}")
	}
	t := &task.Task{
		Type: req.Type, Payload: req.Payload, Priority: req.Priority,
		MaxAttempts: req.MaxAttempts, Status: task.StatusQueued,
	}
	if err := s.Store.CreateTask(r.Context(), t); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	env := queue.Envelope{
		ID: t.ID, Type: t.Type, Priority: t.Priority,
		Attempts: 0, MaxAttempts: t.MaxAttempts, Payload: t.Payload,
	}
	if err := s.Queue.Enqueue(r.Context(), env); err != nil {
		writeError(w, http.StatusInternalServerError, "enqueue: "+err.Error())
		return
	}
	_ = s.Queue.Publish(r.Context(), task.Event{
		TaskID: t.ID, Type: t.Type, Status: task.StatusQueued, Timestamp: time.Now().UTC(),
	})
	writeJSON(w, http.StatusCreated, t)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tasks, err := s.Store.ListTasks(r.Context(), q.Get("status"), q.Get("type"), parseInt(q.Get("limit"), 100))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	t, err := s.Store.GetTask(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) getTaskEvents(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	events, err := s.Store.ListTaskEvents(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) cancelTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	if err := s.Store.MarkCanceled(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "canceled"})
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	items, err := s.Store.ListDeadLetters(r.Context(), parseInt(r.URL.Query().Get("limit"), 100))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) redriveDLQ(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	t, err := s.Store.Redrive(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	env := queue.Envelope{
		ID: t.ID, Type: t.Type, Priority: t.Priority,
		Attempts: 0, MaxAttempts: t.MaxAttempts, Payload: t.Payload,
	}
	if err := s.Queue.Enqueue(r.Context(), env); err != nil {
		writeError(w, http.StatusInternalServerError, "enqueue: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.Store.ListWorkers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, workers)
}

// StatsResponse aggregates Postgres counters with live Redis queue depths.
type StatsResponse struct {
	Tasks    store.Stats              `json:"tasks"`
	Queues   map[string]QueueDepths   `json:"queues"`
	Clients  int                      `json:"ws_clients"`
}

// QueueDepths reports current Redis depths for a single task type.
type QueueDepths struct {
	Ready   int64 `json:"ready"`
	Delayed int64 `json:"delayed"`
	DLQ     int64 `json:"dlq"`
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.Store.GetStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	types, err := s.Queue.ListTypes(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	depths := map[string]QueueDepths{}
	for _, t := range types {
		ready, _ := s.Queue.QueueDepth(r.Context(), t)
		delayed, _ := s.Queue.DelayedDepth(r.Context(), t)
		dlq, _ := s.Queue.DLQDepth(r.Context(), t)
		depths[t] = QueueDepths{Ready: ready, Delayed: delayed, DLQ: dlq}
	}
	resp := StatsResponse{Tasks: st, Queues: depths}
	if s.Hub != nil {
		resp.Clients = s.Hub.ClientCount()
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return def
		}
		n = n*10 + int(r-'0')
	}
	return n
}
