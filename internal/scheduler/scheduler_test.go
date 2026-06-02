package scheduler

import (
	"testing"
	"time"
)

func TestBackoffDelay_Monotonic(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second
	// Compute max possible (not jittered floor) for several attempts and ensure cap respected.
	for i := 1; i <= 12; i++ {
		d := BackoffDelay(i, base, max)
		if d < base {
			t.Errorf("attempt %d: delay %s < base %s", i, d, base)
		}
		if d > max {
			t.Errorf("attempt %d: delay %s > max %s", i, d, max)
		}
	}
}

func TestBackoffDelay_Caps(t *testing.T) {
	base := time.Second
	max := 2 * time.Second
	// after many attempts the exponential explodes; ensure cap honored.
	for i := 0; i < 100; i++ {
		d := BackoffDelay(20, base, max)
		if d > max {
			t.Fatalf("delay %s exceeded max %s", d, max)
		}
	}
}
