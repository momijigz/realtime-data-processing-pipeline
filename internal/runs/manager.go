// Package runs tracks the lifecycle of generator runs. At most one run is
// active at a time — the scale-lab argument is that interleaved runs make
// throughput measurements meaningless. Callers should use StartSteady to begin
// a run and Stop to cancel it; the Counter on an active run feeds the
// /metrics/throughput endpoint.
package runs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/momijigz/realtime-data-processing-pipeline/internal/generator"
)

// State is the lifecycle state of a run.
type State string

const (
	StateRunning  State = "running"
	StateFlushing State = "flushing" // produce loop exited, draining in-flight messages
	StateStopping State = "stopping" // user pressed Stop, draining in-flight (subset of flushing)
	StateStopped  State = "stopped"  // user-cancelled, drain complete
	StateFinished State = "finished" // ran to MessageCount, drain complete
	StateFailed   State = "failed"
)

// Run is the public view of a single generator run.
type Run struct {
	ID         string
	State      State
	StartedAt  time.Time
	FinishedAt time.Time // zero if still running
	Sent       int64     // messages produced; copied off the Counter by Snapshot()
	BytesSent  int64     // total payload bytes produced
	Err        string    // populated when State == failed
}

// ErrRunInProgress is returned by StartSteady when a run is already active.
var ErrRunInProgress = errors.New("a run is already in progress")

// ErrNoActiveRun is returned by Stop when there's no run to cancel.
var ErrNoActiveRun = errors.New("no active run")

// Manager owns the single-active-run invariant.
type Manager struct {
	mu       sync.Mutex
	current  *activeRun // nil when no run is active
	last     *Run       // the most recently completed run (read-only snapshot)
}

type activeRun struct {
	run     *Run
	counter *generator.Counter
	cancel  context.CancelFunc
}

// New returns a fresh Manager with no active run.
func New() *Manager {
	return &Manager{}
}

// StartSteady kicks off a steady-rate generator run in a goroutine and returns
// the run's metadata. Returns ErrRunInProgress if a run is already active.
func (m *Manager) StartSteady(cfg generator.SteadyConfig) (*Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current != nil {
		return nil, ErrRunInProgress
	}

	ctx, cancel := context.WithCancel(context.Background())
	counter := &generator.Counter{}
	run := &Run{
		ID:        fmt.Sprintf("run-%d", time.Now().UnixNano()),
		State:     StateRunning,
		StartedAt: time.Now(),
	}
	m.current = &activeRun{run: run, counter: counter, cancel: cancel}

	// OnLoopExit fires from inside the generator the moment the produce loop
	// exits — but BEFORE the final Flush. Flip state to "flushing" so the UI
	// stops showing live throughput / shows a draining indicator. If state is
	// already "stopping" (user pressed Stop), leave it alone — that's a more
	// specific signal.
	cfg.OnLoopExit = func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.current != nil && m.current.run.State == StateRunning {
			m.current.run.State = StateFlushing
		}
	}

	go func() {
		err := generator.RunSteady(ctx, cfg, counter)

		m.mu.Lock()
		defer m.mu.Unlock()

		// Capture final sent count + bytes from the counter.
		run.Sent = counter.Value()
		run.BytesSent = counter.Bytes()
		run.FinishedAt = time.Now()

		switch {
		case errors.Is(err, context.Canceled):
			run.State = StateStopped
		case err != nil:
			run.State = StateFailed
			run.Err = err.Error()
		default:
			run.State = StateFinished
		}

		m.last = run
		m.current = nil
	}()

	// Return a copy so callers can't mutate manager state.
	out := *run
	return &out, nil
}

// Stop cancels the active run if there is one. Returns ErrNoActiveRun
// otherwise. The goroutine in StartSteady transitions the run to stopped.
func (m *Manager) Stop() (*Run, error) {
	m.mu.Lock()
	cur := m.current
	m.mu.Unlock()

	if cur == nil {
		return nil, ErrNoActiveRun
	}
	cur.cancel()

	// Mark stopping immediately so the UI reflects user intent without waiting
	// for the goroutine to drain pending messages (producer.Flush can take ~15s).
	m.mu.Lock()
	if m.current != nil && m.current.run.State == StateRunning {
		m.current.run.State = StateStopping
	}
	m.mu.Unlock()

	out := *cur.run
	out.State = StateStopping
	out.Sent = cur.counter.Value()
	out.BytesSent = cur.counter.Bytes()
	return &out, nil
}

// Snapshot returns the current run (running) or the last completed run
// (when idle). Returns nil if no run has ever been started.
func (m *Manager) Snapshot() *Run {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current != nil {
		out := *m.current.run
		out.Sent = m.current.counter.Value()
		out.BytesSent = m.current.counter.Bytes()
		return &out
	}
	if m.last != nil {
		out := *m.last
		return &out
	}
	return nil
}

// Throughput returns the active run's current msg/s (sent / elapsed), or 0 if
// no run is active OR the run is past the produce loop (flushing/stopping).
// This prevents the UI from showing a slowly-decaying number after the
// producer has stopped writing — the elapsed time keeps growing but Sent
// doesn't, which would otherwise yield a misleading "rate" that tapers off.
func (m *Manager) Throughput() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current == nil || m.current.run.State != StateRunning {
		return 0
	}
	elapsed := time.Since(m.current.run.StartedAt).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(m.current.counter.Value()) / elapsed
}
