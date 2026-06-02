// Command loadtest pushes a configurable number of tasks at the API and reports
// throughput plus a failure-rate snapshot when finished.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		base     = flag.String("url", "http://localhost:8080", "API base URL")
		n        = flag.Int("n", 1000, "number of tasks")
		conc     = flag.Int("c", 32, "concurrent submitters")
		taskType = flag.String("type", "flaky", "task type")
		failRate = flag.Float64("fail-rate", 0.05, "fail rate passed to flaky handler payload")
		priLo    = flag.Int("priority-min", 1, "min priority")
		priHi    = flag.Int("priority-max", 9, "max priority")
		maxAtt   = flag.Int("max-attempts", 5, "max attempts")
		wait     = flag.Duration("wait", 30*time.Second, "wait for completion after submit")
	)
	flag.Parse()

	type req struct {
		Type        string          `json:"type"`
		Payload     json.RawMessage `json:"payload"`
		Priority    int             `json:"priority"`
		MaxAttempts int             `json:"max_attempts"`
	}

	payload, _ := json.Marshal(map[string]float64{"fail_rate": *failRate})

	start := time.Now()
	var submitted, errs int64
	jobs := make(chan int, *n)
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 10 * time.Second}
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				priority := *priLo + (idx % (*priHi - *priLo + 1))
				body, _ := json.Marshal(req{Type: *taskType, Payload: payload, Priority: priority, MaxAttempts: *maxAtt})
				resp, err := client.Post(*base+"/api/tasks", "application/json", bytes.NewReader(body))
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode >= 300 {
					atomic.AddInt64(&errs, 1)
					continue
				}
				atomic.AddInt64(&submitted, 1)
			}
		}()
	}
	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("submitted %d/%d in %s (%.0f req/s), errors=%d", submitted, *n, elapsed, float64(submitted)/elapsed.Seconds(), errs)

	if *wait <= 0 {
		return
	}
	log.Printf("waiting up to %s for tasks to drain...", *wait)
	deadline := time.Now().Add(*wait)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		<-ticker.C
		st, err := fetchStats(client, *base)
		if err != nil {
			log.Printf("stats: %v", err)
			continue
		}
		pending := st.Tasks.ByStatus["queued"] + st.Tasks.ByStatus["running"] + st.Tasks.ByStatus["retrying"]
		fmt.Printf("status=%v pending=%d\n", st.Tasks.ByStatus, pending)
		if pending == 0 {
			break
		}
	}
	final, err := fetchStats(client, *base)
	if err != nil {
		log.Fatalf("final stats: %v", err)
	}
	total := int64(0)
	for _, v := range final.Tasks.ByStatus {
		total += v
	}
	dead := final.Tasks.ByStatus["dead"]
	var rate float64
	if total > 0 {
		rate = float64(dead) / float64(total)
	}
	fmt.Printf("FINAL by_status=%v total=%d dead=%d failure_rate=%.4f%%\n",
		final.Tasks.ByStatus, total, dead, rate*100)
}

type statsResp struct {
	Tasks struct {
		ByStatus map[string]int64 `json:"by_status"`
	} `json:"tasks"`
}

func fetchStats(c *http.Client, base string) (*statsResp, error) {
	resp, err := c.Get(base + "/api/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s statsResp
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}
