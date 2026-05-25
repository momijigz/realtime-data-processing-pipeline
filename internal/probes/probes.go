// Package probes runs background health probes against pipeline services and
// caches the most recent result for the API to serve. Each probe has a 2s
// timeout and runs in parallel with the others on each tick so a slow service
// can't slow the others down.
package probes

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Status is the semantic dot color the UI consumes.
type Status string

const (
	StatusHealthy Status = "healthy"
	StatusWarn    Status = "warn"
	StatusDown    Status = "down"
)

// Config wires URLs/brokers for the probes. All fields are required.
type Config struct {
	KafkaBrokers string
	KibanaURL    string
	ConnectURL   string
	ESURL        string
	LogstashURL  string
}

// Poller probes every Interval and exposes the latest snapshot via Get().
type Poller struct {
	cfg      Config
	interval time.Duration
	timeout  time.Duration

	mu       sync.RWMutex
	snapshot map[string]Status
}

// New returns a Poller with sensible defaults (5s interval, 2s per-probe timeout).
// Initial snapshot reports all services as "down" until the first probe completes.
func New(cfg Config) *Poller {
	return &Poller{
		cfg:      cfg,
		interval: 5 * time.Second,
		timeout:  2 * time.Second,
		snapshot: map[string]Status{
			"kafka":         StatusDown,
			"elasticsearch": StatusDown,
			"kibana":        StatusDown,
			"kafkaConnect":  StatusDown,
			"logstash":      StatusDown,
		},
	}
}

// Run blocks until ctx is cancelled, probing services every interval. Call
// once at startup in a goroutine.
func (p *Poller) Run(ctx context.Context) {
	// Probe once immediately so the UI doesn't show all-down for the first 5s.
	p.tick(ctx)

	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

// Get returns a copy of the current snapshot. Safe to call concurrently.
func (p *Poller) Get() map[string]Status {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]Status, len(p.snapshot))
	for k, v := range p.snapshot {
		out[k] = v
	}
	return out
}

// tick runs all probes in parallel and writes the result map.
func (p *Poller) tick(ctx context.Context) {
	results := make(map[string]Status)
	var mu sync.Mutex
	var wg sync.WaitGroup

	probes := map[string]func(context.Context) Status{
		"kafka":         func(c context.Context) Status { return probeKafka(c, p.cfg.KafkaBrokers, p.timeout) },
		"elasticsearch": func(c context.Context) Status { return probeHTTPJSON(c, p.cfg.ESURL+"/_cluster/health", p.timeout, esHealthOK) },
		"kibana":        func(c context.Context) Status { return probeHTTPJSON(c, p.cfg.KibanaURL+"/api/status", p.timeout, kibanaStatusOK) },
		"kafkaConnect":  func(c context.Context) Status { return probeHTTPOK(c, p.cfg.ConnectURL+"/", p.timeout) },
		"logstash":      func(c context.Context) Status { return probeHTTPOK(c, p.cfg.LogstashURL+"/", p.timeout) },
	}

	for name, fn := range probes {
		wg.Add(1)
		go func(name string, fn func(context.Context) Status) {
			defer wg.Done()
			s := fn(ctx)
			mu.Lock()
			results[name] = s
			mu.Unlock()
		}(name, fn)
	}
	wg.Wait()

	p.mu.Lock()
	p.snapshot = results
	p.mu.Unlock()
}

// ---------- individual probes ----------

// probeHTTPOK: any 2xx response is healthy.
func probeHTTPOK(ctx context.Context, url string, timeout time.Duration) Status {
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(c, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return StatusDown
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return StatusHealthy
	}
	return StatusWarn
}

// probeHTTPJSON: 2xx + ok-predicate against parsed JSON body.
func probeHTTPJSON(ctx context.Context, url string, timeout time.Duration, ok func(map[string]any) Status) Status {
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(c, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return StatusDown
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return StatusWarn
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return StatusWarn
	}
	return ok(body)
}

// esHealthOK: ES /_cluster/health returns {"status": "green"|"yellow"|"red", ...}.
// green = healthy, yellow = warn (degraded but functional), red = down.
func esHealthOK(body map[string]any) Status {
	switch body["status"] {
	case "green":
		return StatusHealthy
	case "yellow":
		return StatusWarn
	default:
		return StatusDown
	}
}

// kibanaStatusOK: /api/status returns {"status": {"overall": {"level": "available"|...}}}.
func kibanaStatusOK(body map[string]any) Status {
	s, _ := body["status"].(map[string]any)
	overall, _ := s["overall"].(map[string]any)
	switch overall["level"] {
	case "available":
		return StatusHealthy
	case "degraded":
		return StatusWarn
	default:
		return StatusDown
	}
}

// probeKafka uses the admin client to list topics within timeout. If the
// client connects and replies, Kafka is healthy.
func probeKafka(ctx context.Context, brokers string, timeout time.Duration) Status {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return StatusDown
	}
	defer admin.Close()

	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err = admin.GetMetadata(nil, true, int(timeout/time.Millisecond))
	if err != nil {
		// Either the deadline fired or the broker rejected. Either way: not healthy.
		_ = c
		return StatusDown
	}
	return StatusHealthy
}
