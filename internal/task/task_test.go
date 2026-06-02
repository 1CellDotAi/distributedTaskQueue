package task

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRegistryRegisterLookup(t *testing.T) {
	r := NewRegistry()
	r.Register("x", HandlerFunc(func(_ context.Context, _ Task) (json.RawMessage, error) {
		return json.RawMessage(`"ok"`), nil
	}))
	if r.Lookup("x") == nil {
		t.Fatal("expected handler for x")
	}
	if r.Lookup("missing") != nil {
		t.Fatal("expected nil for unknown type")
	}
	got := r.Types()
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("unexpected types: %v", got)
	}
}

func TestDefaultHandlers(t *testing.T) {
	r := NewRegistry()
	RegisterDefaults(r)
	for _, typ := range []string{"echo", "sleep", "flaky", "email"} {
		if r.Lookup(typ) == nil {
			t.Errorf("missing handler %s", typ)
		}
	}
	out, err := r.Lookup("echo").Handle(context.Background(), Task{Payload: json.RawMessage(`{"a":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"a":1}` {
		t.Fatalf("echo returned %s", out)
	}
}
