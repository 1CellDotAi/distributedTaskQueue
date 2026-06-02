// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds runtime configuration for all binaries.
type Config struct {
	// DatabaseURL is a libpq-style connection string for PostgreSQL.
	DatabaseURL string
	// RedisURL is a redis:// URL.
	RedisURL string
	// HTTPAddr is the listen address for the API server, e.g. ":8080".
	HTTPAddr string
	// WorkerConcurrency is the number of tasks processed in parallel by a worker.
	WorkerConcurrency int
	// HeartbeatInterval controls how often workers refresh their heartbeat key.
	HeartbeatInterval time.Duration
	// HeartbeatTTL is the TTL on the heartbeat key in Redis.
	HeartbeatTTL time.Duration
	// CoordinatorSweepInterval is how often the coordinator scans for dead workers / promotes delayed tasks.
	CoordinatorSweepInterval time.Duration
	// RetryBaseDelay is the base delay for exponential backoff retries.
	RetryBaseDelay time.Duration
	// RetryMaxDelay caps the retry delay.
	RetryMaxDelay time.Duration
	// TaskTypes is the list of queues a worker should consume from. Empty means all known types.
	TaskTypes []string
	// LeaseTTL is how long a worker holds an in-flight task before it can be reclaimed.
	LeaseTTL time.Duration
}

// Load reads configuration from environment variables, applying defaults.
func Load() Config {
	return Config{
		DatabaseURL:              getEnv("DATABASE_URL", "host=localhost port=5432 user=postgres dbname=taskq sslmode=disable"),
		RedisURL:                 getEnv("REDIS_URL", "redis://localhost:6379/0"),
		HTTPAddr:                 getEnv("HTTP_ADDR", ":8080"),
		WorkerConcurrency:        getEnvInt("WORKER_CONCURRENCY", 4),
		HeartbeatInterval:        getEnvDuration("HEARTBEAT_INTERVAL", 3*time.Second),
		HeartbeatTTL:             getEnvDuration("HEARTBEAT_TTL", 9*time.Second),
		CoordinatorSweepInterval: getEnvDuration("COORDINATOR_SWEEP_INTERVAL", time.Second),
		RetryBaseDelay:           getEnvDuration("RETRY_BASE_DELAY", 500*time.Millisecond),
		RetryMaxDelay:            getEnvDuration("RETRY_MAX_DELAY", 60*time.Second),
		LeaseTTL:                 getEnvDuration("LEASE_TTL", 60*time.Second),
		TaskTypes:                splitCSV(os.Getenv("TASK_TYPES")),
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// String returns a safe summary of the config (without credentials).
func (c Config) String() string {
	return fmt.Sprintf("http=%s workers=%d heartbeat=%s/%s sweep=%s lease=%s",
		c.HTTPAddr, c.WorkerConcurrency, c.HeartbeatInterval, c.HeartbeatTTL,
		c.CoordinatorSweepInterval, c.LeaseTTL)
}
