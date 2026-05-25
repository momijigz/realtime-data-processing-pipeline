package generator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/time/rate"
)

// SteadyConfig is the knobs for a "steady-rate" generator run.
type SteadyConfig struct {
	BootstrapServers string  // Kafka broker addr (e.g. "kafka:29092")
	Topic            string  // target topic
	MessageCount     int     // -1 = unbounded (runs until ctx is cancelled)
	TargetRate       float64 // target msg/s. 0 = unlimited (full speed)

	// FlushTimeout bounds the final producer.Flush() at the natural end of a
	// run (limit reached). It does NOT apply to the cancel/error paths, which
	// always use a short 2s drain so the Stop button stays responsive. Zero
	// or negative falls back to defaultFlushTimeout.
	FlushTimeout time.Duration

	// Producer tuning knobs (tier 1). Pointer types because zero values are
	// meaningful (e.g. linger.ms=0 means "send immediately" — distinct from
	// "use librdkafka default"). When nil, librdkafka's default applies.
	LingerMs        *int   // queue.buffering.max.ms — wait time to batch before send
	BatchSize       *int   // batch.size — max bytes per batch
	CompressionType string // none/gzip/snappy/lz4/zstd. Empty = librdkafka default (none).

	// OnLoopExit, if set, is called once the produce loop exits (limit hit
	// or ctx cancelled) but BEFORE the final Flush blocks. Callers use this
	// to flip a "flushing" UI state while in-flight messages drain.
	OnLoopExit func()
}

const defaultFlushTimeout = 15 * time.Second

// brokerDeadTimeout is how long we tolerate no successful delivery reports
// while still trying to produce. Past this, we assume the broker is gone
// (stopped, partitioned, ISR collapsed) and exit with ErrBrokerUnreachable.
// A scale lab wants the UI to reflect failure within seconds, not after the
// local librdkafka queue (default 100k msgs) fills.
const brokerDeadTimeout = 5 * time.Second

// ErrBrokerUnreachable is returned when no delivery reports succeed within
// brokerDeadTimeout. Surfaced to the user as run state "failed".
var ErrBrokerUnreachable = errors.New("broker unreachable: no successful delivery within 5s")

// Counter is a thread-safe progress counter exposed by RunSteady so callers
// (HTTP /metrics/throughput, CLI status output) can observe progress. It tracks
// both message count and total payload bytes (the JSON value length per message,
// not including Kafka framing/headers).
type Counter struct {
	n     atomic.Int64
	bytes atomic.Int64
}

func (c *Counter) Value() int64 { return c.n.Load() }
func (c *Counter) Bytes() int64 { return c.bytes.Load() }

// RunSteady produces Transactions at a steady rate until MessageCount is
// reached or ctx is cancelled. It blocks until the run completes. The Counter
// increments on each successful *delivery report* (broker ack), NOT on each
// Produce() call — so when the broker dies, Sent stops climbing immediately
// rather than filling librdkafka's local queue while reporting fake progress.
//
// When cfg.TargetRate > 0, a token-bucket rate limiter paces produces at the
// requested msg/s. The bucket's burst capacity is sized to allow short spikes
// without throttling the steady state.
//
// If no successful delivery report arrives within brokerDeadTimeout while the
// loop is still trying to produce, RunSteady exits with ErrBrokerUnreachable
// so the UI flips to "failed" within seconds of a broker stop.
func RunSteady(ctx context.Context, cfg SteadyConfig, counter *Counter) error {
	kafkaCfg := kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	}
	// Tier-1 tuning knobs — only set when caller specified, so librdkafka
	// defaults apply otherwise. The librdkafka name for "linger" is the
	// historical "queue.buffering.max.ms" (Confluent docs alias both).
	if cfg.LingerMs != nil {
		kafkaCfg["queue.buffering.max.ms"] = *cfg.LingerMs
	}
	if cfg.BatchSize != nil {
		kafkaCfg["batch.size"] = *cfg.BatchSize
	}
	if cfg.CompressionType != "" {
		kafkaCfg["compression.type"] = cfg.CompressionType
	}

	producer, err := kafka.NewProducer(&kafkaCfg)
	if err != nil {
		return fmt.Errorf("new producer: %w", err)
	}
	// Producer Close is deferred via cleanup() below so we can also wait
	// for the delivery-report drainer to exit before returning.

	// nil limiter = unlimited / full speed.
	var limiter *rate.Limiter
	if cfg.TargetRate > 0 {
		// Burst = max(1, 10% of rate) gives a tiny cushion against scheduler
		// jitter without letting the actual rate drift far above the target.
		burst := int(cfg.TargetRate / 10)
		if burst < 1 {
			burst = 1
		}
		limiter = rate.NewLimiter(rate.Limit(cfg.TargetRate), burst)
	}

	// Signal "loop exiting, about to flush" exactly once, regardless of exit
	// path (limit hit, ctx cancel, etc).
	signaledExit := false
	signalExit := func() {
		if !signaledExit && cfg.OnLoopExit != nil {
			cfg.OnLoopExit()
		}
		signaledExit = true
	}

	// Track time of last successful delivery for broker-health watchdog.
	// Initialize to "now" so the timeout doesn't fire before the first ack
	// has had a chance to land.
	var lastDeliveryNs atomic.Int64
	lastDeliveryNs.Store(time.Now().UnixNano())

	// Drain delivery reports off producer.Events() so the Counter reflects
	// broker-acked messages, not local enqueues. The drainer exits when the
	// producer is closed (which closes the Events channel).
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for ev := range producer.Events() {
			m, isMsg := ev.(*kafka.Message)
			if !isMsg {
				continue // ignore non-delivery events (kafka.Error, Stats, etc.)
			}
			if m.TopicPartition.Error != nil {
				// Delivery failed — do NOT count this message.
				continue
			}
			if counter != nil {
				counter.n.Add(1)
				counter.bytes.Add(int64(len(m.Value)))
			}
			lastDeliveryNs.Store(time.Now().UnixNano())
		}
	}()

	// Cleanup runs on return: close the producer (drains and closes Events),
	// then wait for the drainer goroutine to finish so we don't leak it.
	defer func() {
		producer.Close()
		<-drainDone
	}()

	topic := cfg.Topic
	for i := 0; cfg.MessageCount < 0 || i < cfg.MessageCount; i++ {
		select {
		case <-ctx.Done():
			signalExit()
			// Short flush on user-cancel — most messages are already in-flight,
			// and a long flush makes the UI feel unresponsive. 2s is plenty for
			// the broker to ack the small backlog of buffered batches.
			producer.Flush(2 * 1000)
			return ctx.Err()
		default:
		}

		// Broker-health watchdog: if no successful delivery in brokerDeadTimeout,
		// give up. Skip the check until we've actually attempted enough produces
		// for a missing ack to be meaningful — otherwise we'd false-positive on
		// the first ~100ms of startup before the first ack lands.
		if i > 100 {
			lastNs := lastDeliveryNs.Load()
			if time.Since(time.Unix(0, lastNs)) > brokerDeadTimeout {
				signalExit()
				producer.Flush(500) // brief drain — broker is gone, nothing will ack
				return ErrBrokerUnreachable
			}
		}

		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				signalExit()
				// ctx cancelled while waiting — drain and exit.
				producer.Flush(2 * 1000)
				return err
			}
		}

		t := NewTransaction()
		payload, err := json.Marshal(&t)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}

		if err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, nil); err != nil {
			// Produce failures (typically ErrQueueFull when broker is slow/down)
			// — back off briefly so the watchdog above gets a chance to fire.
			time.Sleep(10 * time.Millisecond)
			continue
		}
	}

	// Natural loop end: hit MessageCount.
	signalExit()
	flushTO := cfg.FlushTimeout
	if flushTO <= 0 {
		flushTO = defaultFlushTimeout
	}
	producer.Flush(int(flushTO / time.Millisecond))
	return nil
}
