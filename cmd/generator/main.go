// Standalone CLI that bootstraps the pipeline (Kibana dashboards + ES sink
// connector) then runs the steady-rate transaction generator. This is the
// same one-shot behavior the old client/ binary had, now built from the
// reusable internal/ packages so the API server can do the same operations
// in-process.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/momijigz/realtime-data-processing-pipeline/internal/bootstrap"
	"github.com/momijigz/realtime-data-processing-pipeline/internal/generator"
)

func main() {
	limit := flag.Int("limit", 1000000, "number of messages to produce (-1 = unbounded)")
	rate := flag.Float64("rate", 0, "target msg/s (0 = unlimited / full speed)")
	flushTO := flag.Duration("flush", 15*time.Second, "drain timeout at natural end of run (e.g. 2s, 500ms)")
	lingerMs := flag.Int("linger-ms", -1, "queue.buffering.max.ms (-1 = use librdkafka default)")
	batchSize := flag.Int("batch-size", -1, "batch.size in bytes (-1 = use librdkafka default)")
	compression := flag.String("compression", "", "none/gzip/snappy/lz4/zstd (empty = librdkafka default)")
	bootstrapServers := flag.String("brokers", envOr("KAFKA_BROKERS", "kafka:29092"), "kafka bootstrap servers")
	topic := flag.String("topic", "transactions", "kafka topic")
	kibanaURL := flag.String("kibana", envOr("KIBANA_URL", "http://kibana:5601"), "kibana base URL")
	connectURL := flag.String("connect", envOr("CONNECT_URL", "http://kafka-connect:8083"), "kafka-connect base URL")
	esURL := flag.String("es", envOr("ES_URL", "http://elastic:9200"), "elasticsearch base URL")
	dashboards := flag.String("dashboards", "/app/exports.ndjson", "path to kibana saved-objects ndjson")
	flag.Parse()

	fmt.Println("**** Pipeline Generator ****")

	if err := bootstrap.UploadKibanaDashboards(*kibanaURL, *dashboards); err != nil {
		log.Fatalf("kibana bootstrap: %v", err)
	}
	fmt.Println("kibana dashboards imported")

	if err := bootstrap.CreateESConnector(*connectURL, *topic, *esURL); err != nil {
		log.Fatalf("connect bootstrap: %v", err)
	}
	fmt.Println("elasticsearch sink connector created")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	counter := &generator.Counter{}
	go reportThroughput(ctx, counter)

	cfg := generator.SteadyConfig{
		BootstrapServers: *bootstrapServers,
		Topic:            *topic,
		MessageCount:     *limit,
		TargetRate:       *rate,
		FlushTimeout:     *flushTO,
		CompressionType:  *compression,
	}
	if *lingerMs >= 0 {
		cfg.LingerMs = lingerMs
	}
	if *batchSize >= 0 {
		cfg.BatchSize = batchSize
	}
	start := time.Now()
	if err := generator.RunSteady(ctx, cfg, counter); err != nil && ctx.Err() == nil {
		log.Fatalf("generator: %v", err)
	}
	fmt.Printf("done: %d messages in %s\n", counter.Value(), time.Since(start))
}

func reportThroughput(ctx context.Context, c *generator.Counter) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(start).Seconds()
			fmt.Printf("throughput: %.2f msg/s\n", float64(c.Value())/elapsed)
		}
	}
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
