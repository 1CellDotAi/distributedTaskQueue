// Package task: sample handlers used for demos and load tests.
package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

// RegisterDefaults registers built-in demo handlers on the supplied registry.
func RegisterDefaults(r *Registry) {
	r.Register("echo", HandlerFunc(echoHandler))
	r.Register("sleep", HandlerFunc(sleepHandler))
	r.Register("flaky", HandlerFunc(flakyHandler))
	r.Register("email", HandlerFunc(emailHandler))
}

func echoHandler(_ context.Context, t Task) (json.RawMessage, error) {
	return t.Payload, nil
}

type sleepPayload struct {
	Seconds float64 `json:"seconds"`
}

func sleepHandler(ctx context.Context, t Task) (json.RawMessage, error) {
	var p sleepPayload
	if len(t.Payload) > 0 {
		_ = json.Unmarshal(t.Payload, &p)
	}
	if p.Seconds <= 0 {
		p.Seconds = 0.1
	}
	select {
	case <-time.After(time.Duration(p.Seconds * float64(time.Second))):
		return json.RawMessage(fmt.Sprintf(`{"slept_seconds":%v}`, p.Seconds)), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type flakyPayload struct {
	FailRate float64 `json:"fail_rate"`
}

// flakyHandler fails with a configurable probability. Used to exercise retries.
func flakyHandler(_ context.Context, t Task) (json.RawMessage, error) {
	var p flakyPayload
	if len(t.Payload) > 0 {
		_ = json.Unmarshal(t.Payload, &p)
	}
	if p.FailRate <= 0 {
		p.FailRate = 0.3
	}
	if rand.Float64() < p.FailRate {
		return nil, errors.New("simulated transient failure")
	}
	return json.RawMessage(`{"ok":true}`), nil
}

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// emailHandler is a stub illustrating a typical side-effect job.
func emailHandler(_ context.Context, t Task) (json.RawMessage, error) {
	var p emailPayload
	if len(t.Payload) > 0 {
		_ = json.Unmarshal(t.Payload, &p)
	}
	if !strings.Contains(p.To, "@") {
		return nil, fmt.Errorf("invalid recipient: %q", p.To)
	}
	fmt.Fprintf(os.Stdout, "[email] would send to=%s subject=%q\n", p.To, p.Subject)
	return json.RawMessage(`{"delivered":true}`), nil
}
