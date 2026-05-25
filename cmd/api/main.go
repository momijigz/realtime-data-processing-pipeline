// Control-plane API for the realtime pipeline lab.
//
// Thin entry point: parses env, constructs the RunManager, registers routes,
// starts Gin. Domain logic lives under internal/. Bootstrap runs once in the
// background on startup so the user doesn't need a separate manual step.
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/momijigz/realtime-data-processing-pipeline/internal/bootstrap"
	"github.com/momijigz/realtime-data-processing-pipeline/internal/probes"
	"github.com/momijigz/realtime-data-processing-pipeline/internal/runs"
	apihttp "github.com/momijigz/realtime-data-processing-pipeline/internal/transport/http"
)

func main() {
	cfg := loadConfig()

	// Kick bootstrap off in the background. It's idempotent (CreateESConnector
	// tolerates 409 Conflict) and waits internally for Kibana/Connect to be
	// HTTP-ready. If it fails (e.g. the supporting services never come up),
	// the API still serves — the user just won't see dashboards or a sink.
	go runBootstrap(cfg)

	// Background health poller. Probes services every 5s; the StackStatus
	// handler serves the cached snapshot.
	poller := probes.New(probes.Config{
		KafkaBrokers: cfg.KafkaBrokers,
		KibanaURL:    cfg.KibanaURL,
		ConnectURL:   cfg.ConnectURL,
		ESURL:        cfg.ESURL,
		LogstashURL:  cfg.LogstashURL,
	})
	go poller.Run(context.Background())

	srv := &apihttp.Server{
		Runs:         runs.New(),
		Probes:       poller,
		KafkaBrokers: cfg.KafkaBrokers,
		KafkaTopic:   cfg.KafkaTopic,
		DefaultLimit: cfg.DefaultLimit,
		DefaultRate:  cfg.DefaultRate,
		DefaultFlush: cfg.DefaultFlush,
	}

	r := gin.Default()
	api := r.Group("/api")
	{
		api.GET("/health", srv.Health)
		api.GET("/stack/status", srv.StackStatus)
		api.POST("/producer/start", srv.ProducerStart)
		api.POST("/producer/stop", srv.ProducerStop)
		api.GET("/producer/status", srv.ProducerStatus)
		api.GET("/metrics/throughput", srv.Throughput)
	}

	log.Printf("api listening on %s", cfg.ListenAddr)
	if err := r.Run(cfg.ListenAddr); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	ListenAddr     string
	KafkaBrokers   string
	KafkaTopic     string
	KibanaURL      string
	ConnectURL     string
	ESURL          string
	LogstashURL    string
	DashboardsPath string
	DefaultLimit   int
	DefaultRate    float64
	DefaultFlush   time.Duration
}

func loadConfig() config {
	return config{
		ListenAddr:     ":" + envOr("PORT", "8090"),
		KafkaBrokers:   envOr("KAFKA_BROKERS", "kafka:29092"),
		KafkaTopic:     envOr("KAFKA_TOPIC", "transactions"),
		KibanaURL:      envOr("KIBANA_URL", "http://kibana:5601"),
		ConnectURL:     envOr("CONNECT_URL", "http://kafka-connect:8083"),
		ESURL:          envOr("ES_URL", "http://elastic:9200"),
		LogstashURL:    envOr("LOGSTASH_URL", "http://logstash:9600"),
		DashboardsPath: envOr("KIBANA_DASHBOARDS", "/app/exports.ndjson"),
		DefaultLimit:   envIntOr("DEFAULT_LIMIT", 1000000),
		DefaultRate:    envFloatOr("DEFAULT_RATE", 0), // 0 = unlimited
		DefaultFlush:   time.Duration(envIntOr("DEFAULT_FLUSH_MS", 15000)) * time.Millisecond,
	}
}

func runBootstrap(cfg config) {
	log.Println("bootstrap: starting in background")
	if err := bootstrap.UploadKibanaDashboards(cfg.KibanaURL, cfg.DashboardsPath); err != nil {
		log.Printf("bootstrap kibana: %v", err)
		// Don't abort — Connect bootstrap is independent.
	} else {
		log.Println("bootstrap kibana: dashboards imported")
	}
	if err := bootstrap.CreateESConnector(cfg.ConnectURL, cfg.KafkaTopic, cfg.ESURL); err != nil {
		log.Printf("bootstrap connect: %v", err)
	} else {
		log.Println("bootstrap connect: sink connector created (or already existed)")
	}
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

func envIntOr(k string, fallback int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envFloatOr(k string, fallback float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
