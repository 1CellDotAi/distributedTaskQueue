-- 0001_init.sql: initial schema for the distributed task queue.

CREATE TABLE IF NOT EXISTS tasks (
    id            UUID PRIMARY KEY,
    type          TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    priority      INT         NOT NULL DEFAULT 5,
    status        TEXT        NOT NULL,
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 3,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    worker_id     TEXT,
    result        JSONB,
    last_error    TEXT        NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tasks_status_priority_avail_idx
    ON tasks (status, priority, available_at);
CREATE INDEX IF NOT EXISTS tasks_worker_idx ON tasks (worker_id);
CREATE INDEX IF NOT EXISTS tasks_type_idx ON tasks (type);

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    hostname        TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    current_task_id UUID,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS task_events (
    id         BIGSERIAL PRIMARY KEY,
    task_id    UUID        NOT NULL,
    event_type TEXT        NOT NULL,
    message    TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS task_events_task_idx ON task_events (task_id, created_at);

CREATE TABLE IF NOT EXISTS dead_letters (
    id         BIGSERIAL PRIMARY KEY,
    task_id    UUID        NOT NULL,
    reason     TEXT        NOT NULL DEFAULT '',
    payload    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    failed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dead_letters_task_idx ON dead_letters (task_id);
