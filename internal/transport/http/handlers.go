// Package http contains HTTP transport — Gin handler functions for the
// control-plane API. Handlers should be thin: parse input, call into
// internal/* domain packages, format output. No business logic.
package http

import (
	nethttp "net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/momijigz/realtime-data-processing-pipeline/internal/generator"
	"github.com/momijigz/realtime-data-processing-pipeline/internal/probes"
	"github.com/momijigz/realtime-data-processing-pipeline/internal/runs"
)

// Server is the dependency-holding struct that backs every handler. Routes
// are registered as methods on Server in main.go.
type Server struct {
	Runs         *runs.Manager
	Probes       *probes.Poller
	KafkaBrokers string
	KafkaTopic   string
	DefaultLimit int
	DefaultRate  float64
	DefaultFlush time.Duration
}

func (s *Server) Health(c *gin.Context) {
	c.JSON(nethttp.StatusOK, gin.H{"status": "ok"})
}

// StackStatus serves the latest cached snapshot from the background probe
// poller. Status values: "healthy" | "warn" | "down".
func (s *Server) StackStatus(c *gin.Context) {
	c.JSON(nethttp.StatusOK, s.Probes.Get())
}

// StartRequest is the JSON body accepted by POST /api/producer/start.
// Fields are optional — omitted/zero values fall back to server defaults
// (rate=0 means unlimited, which is the natural "no override" semantic).
type StartRequest struct {
	TargetRate     *float64 `json:"targetRate,omitempty"`      // msg/s; nil = use default; 0 = unlimited
	Limit          *int     `json:"limit,omitempty"`           // total messages; nil = use default; -1 = unbounded
	FlushTimeoutMs *int     `json:"flushTimeoutMs,omitempty"`  // ms to wait for in-flight drain at end of run; nil = use default
	LingerMs       *int     `json:"lingerMs,omitempty"`        // queue.buffering.max.ms producer tuning
	BatchSize      *int     `json:"batchSize,omitempty"`       // batch.size producer tuning (bytes)
	Compression    string   `json:"compressionType,omitempty"` // none/gzip/snappy/lz4/zstd; "" = librdkafka default
}

// validCompressions is the set librdkafka accepts; anything else fails at
// producer init with a cryptic error, so we 400 here instead.
var validCompressions = map[string]bool{
	"":       true, // empty = "use default" (no override)
	"none":   true,
	"gzip":   true,
	"snappy": true,
	"lz4":    true,
	"zstd":   true,
}

// ProducerStart kicks off a steady-rate generator run with the server's
// defaults (overridable by the request body). Returns 409 if a run is active.
func (s *Server) ProducerStart(c *gin.Context) {
	var req StartRequest
	// Body is optional — ignore decode errors when the body is empty.
	_ = c.ShouldBindJSON(&req)

	if !validCompressions[req.Compression] {
		c.JSON(nethttp.StatusBadRequest, gin.H{
			"error": "compressionType must be one of: none, gzip, snappy, lz4, zstd",
		})
		return
	}

	cfg := generator.SteadyConfig{
		BootstrapServers: s.KafkaBrokers,
		Topic:            s.KafkaTopic,
		MessageCount:     s.DefaultLimit,
		TargetRate:       s.DefaultRate,
		FlushTimeout:     s.DefaultFlush,
		LingerMs:         req.LingerMs,
		BatchSize:        req.BatchSize,
		CompressionType:  req.Compression,
	}
	if req.TargetRate != nil {
		cfg.TargetRate = *req.TargetRate
	}
	if req.Limit != nil {
		cfg.MessageCount = *req.Limit
	}
	if req.FlushTimeoutMs != nil {
		cfg.FlushTimeout = time.Duration(*req.FlushTimeoutMs) * time.Millisecond
	}

	run, err := s.Runs.StartSteady(cfg)
	if err == runs.ErrRunInProgress {
		c.JSON(nethttp.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		c.JSON(nethttp.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(nethttp.StatusOK, run)
}

// ProducerStop cancels the active run. Returns 404 if there is no active run.
func (s *Server) ProducerStop(c *gin.Context) {
	run, err := s.Runs.Stop()
	if err == runs.ErrNoActiveRun {
		c.JSON(nethttp.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		c.JSON(nethttp.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(nethttp.StatusOK, run)
}

// ProducerStatus returns the current (or last) run snapshot, or null if no
// run has ever been started.
func (s *Server) ProducerStatus(c *gin.Context) {
	c.JSON(nethttp.StatusOK, gin.H{"run": s.Runs.Snapshot()})
}

// Throughput returns the active run's msg/s — useful for the UI poll loop.
func (s *Server) Throughput(c *gin.Context) {
	c.JSON(nethttp.StatusOK, gin.H{
		"producedPerSec": s.Runs.Throughput(),
		// consumedPerSec stays mocked until we wire up consumer-group lag tracking.
		"consumedPerSec": 0,
	})
}
