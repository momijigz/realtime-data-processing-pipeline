# Changelog

All notable changes to this project since the last commit (`8dbc6ad`,
"Adding nodejs based server"). Entries are grouped by date (YYYY-MM-DD) of
the commit that introduced them. Newest entries on top.

The project was reframed from an e-commerce demo into a real-time pipeline
scale lab: a control-plane API, a React UI, in-process generators, and a
production-shaped repo layout.

## 2026-05-23

### Repo restructure

- Collapsed two Go modules (`api/go.mod`, `client/go.mod`) into a single root
  `go.mod` at `github.com/momijigz/realtime-data-processing-pipeline`. Go
  1.25.0.
- Adopted the standard `cmd/` + `internal/` Go layout:
  - `cmd/api/` — control-plane HTTP server (Gin)
  - `cmd/generator/` — standalone CLI generator (replaces the old `client/`)
  - `internal/generator/` — generator domain code (Transaction, RunSteady,
    Counter, rate limiter, broker watchdog)
  - `internal/bootstrap/` — Kibana dashboard import + ES sink connector creation
  - `internal/runs/` — RunManager (single-active-run invariant, state machine)
  - `internal/probes/` — background health probes for stack services
  - `internal/transport/http/` — Gin handlers, request types
- Moved ops files into `deploy/`:
  - `deploy/docker-compose.yml`
  - `deploy/logstash/pipeline/logstash.conf`
  - `deploy/connect-plugins/` (large jar tree)
  - All `docker compose` commands now require `-f deploy/docker-compose.yml`
- Static fixtures moved to `assets/kibana/exports.ndjson`
- Deleted the orphaned `server/` Node.js scaffold (dead code, not wired into
  anything)
- Added `internal-docs/` with `recommendations.md` (pivot notes) and
  `microservices-generator.md` (plan for the next generator type)

### New control-plane API (`cmd/api/`)

A long-running HTTP server (Gin) on port 8090 that controls the generator
in-process and serves stack telemetry. Replaces the old "client runs once on
docker compose up" model.

- `GET  /api/health` — liveness probe
- `GET  /api/stack/status` — per-service status from probe poller
  (`healthy`/`warn`/`down`)
- `POST /api/producer/start` — start a generator run with JSON body of knobs;
  returns 409 if a run is already active
- `POST /api/producer/stop` — cancel the active run (returns immediately with
  `stopping` state; goroutine drains in background)
- `GET  /api/producer/status` — current or last-completed run snapshot
- `GET  /api/metrics/throughput` — produced/consumed msg/s (returns 0 when no
  run is running, by design — no taper)

### Generator (in-process, was standalone)

The generator code now lives in `internal/generator/` and runs in-process from
the API via a `RunManager`. The standalone CLI in `cmd/generator/` is retained
for benchmarks but does not autostart with the stack (lives behind `profiles:
[manual]` in docker-compose).

**Configurable knobs on `POST /api/producer/start`:**

| Field | Purpose |
|---|---|
| `targetRate` | msg/s; 0 = unlimited |
| `limit` | message count cap; -1 = unbounded |
| `flushTimeoutMs` | drain timeout at natural end-of-run |
| `lingerMs` | librdkafka `queue.buffering.max.ms` |
| `batchSize` | librdkafka `batch.size` (bytes) |
| `compressionType` | none / gzip / snappy / lz4 / zstd |

**Run lifecycle states:** `running` → `flushing` → `finished` / `stopped` /
`failed`, plus `stopping` (user-cancelled, draining).

### Real Kafka tracking, not local-enqueue counting

The Counter previously incremented after `Produce()` (local enqueue) — when
Kafka went down, the local queue would silently absorb messages while the UI
showed climbing throughput. Now the Counter increments only on **successful
delivery reports** from `producer.Events()`. With Kafka down, the count
freezes immediately and throughput stops climbing.

### Broker-failure detection

Added `ErrBrokerUnreachable` watchdog: if no successful delivery report
arrives within 5 seconds while the loop is still producing, the run exits with
an error and `RunManager` flips state to `failed` with a clear error message.
Verified by stopping Kafka mid-run — state transitions to `failed` within ~6s.

### Background probe poller (`internal/probes/`)

5-second probe interval, 2-second per-probe timeout, runs all probes in
parallel:

- **Kafka** — admin-client `GetMetadata` (any successful reply = healthy)
- **Elasticsearch** — `GET /_cluster/health` (`green`=healthy, `yellow`=warn,
  `red`=down)
- **Kibana** — `GET /api/status` (`available`=healthy, `degraded`=warn)
- **Kafka Connect** — `GET /` (2xx = healthy)
- **Logstash** — `GET :9600/` (2xx = healthy)

The handler serves the cached snapshot; no per-request probing.

### Auto-bootstrap on api startup

`POST /api/producer/start` no longer bootstraps anything. On API server boot,
a background goroutine runs `bootstrap.UploadKibanaDashboards` and
`bootstrap.CreateESConnector` exactly once. Both are idempotent (Connect
tolerates 409 Conflict). The API serves immediately; bootstrap completes in
the background.

### React UI (`ui/`)

New single-page application that talks to the Go API. Vite + React + TypeScript
+ Tailwind v4. Vite dev server proxies `/api/*` to the api container so the
browser never makes cross-origin calls.

**Three-column layout:**
- Left: Controls (sliders, action buttons)
- Center: KPI row (Throughput / Messages / Bytes) + Logs panel
- Right: Producer card (run state + throughput sub-section) + Stack Status
  card with colored dots

**Controls:**
- **Three stepped sliders** for the most-tuned knobs:
  - Target Rate: 10 / 100 / 1k / 10k / 100k / 1M / ∞ msg/s
  - Message Limit: 1k / 10k / 100k / 1M / 10M / ∞
  - Flush Timeout: 5s / 10s / 15s
- **Numeric inputs** for the tier-1 batching knobs:
  - linger.ms
  - batch.size (bytes)
  - compression.type (dropdown: default / none / gzip / snappy / lz4 / zstd)
- **Start / Stop buttons** with state-dependent labels
  (Start / Running… / Stopping… / Flushing…)

**Stack Status dots** (size reduced to 6px after first iteration):
- 🟢 emerald = `healthy`
- 🟡 amber = `warn`
- 🔴 red = `down`

**Toasts** (top-right, sticky until recovery or manual dismiss):
- Fire on healthy → down transitions for any crucial service
- Auto-clear when the service recovers
- Manual × to dismiss

**Disabled Start guard:**
- Start button disables when any of {kafka, elasticsearch, kibana,
  kafkaConnect, logstash} is `down`
- Red explanatory text appears below the disabled button
- Click-time toast fires as a defensive backstop in case polling lag lets the
  button be clicked during a transition

**Logs panel:** UI-event-driven (clicks, polls, errors, run lifecycle).
Auto-scrolls, capped at 500 entries.

### Docker compose changes (`deploy/docker-compose.yml`)

- Removed the dead `version: "3.8"` directive
- Fixed Kafka to work with `apache/kafka:3.9.0`:
  - Switched env prefix from `KAFKA_CFG_*` (Bitnami convention) to `KAFKA_*`
  - Added `CLUSTER_ID`
  - Removed broken custom `command:` block that referenced
    `/kafka/bin/kafka-storage.sh` (wrong path for this image — binaries are at
    `/opt/kafka/bin/`)
  - Fixed healthcheck binary path
  - Moved log dir to `/var/lib/kafka/data`
- Added two new services:
  - `api` on port 8090, depends on kafka/kibana/kafka-connect/elasticsearch
  - `ui` on port 5173, depends on api
- Generator behavior:
  - Removed from default startup (now lives behind `profiles: [manual]`)
  - Renamed `client` service → `generator`
  - Restart policy: `restart: always` → `restart: on-failure` (the generator
    is a one-shot, not a long-running service)
  - Removed dead `ports: ["3005:3005"]` mapping (client never listens)

### Generator (`internal/generator/`) — code hygiene

- Removed three `rand.Seed(time.Now().UnixNano())` calls — Go 1.20+ auto-seeds
  the global RNG; re-seeding in a hot loop was producing degraded randomness
  by occasionally landing in the same nanosecond bucket
- Replaced deprecated `ioutil.ReadAll` with `io.ReadAll`; dropped `io/ioutil`
  import
- Removed double `producer.Close()` call that was panicking on shutdown (the
  `defer producer.Close()` + explicit close caused "close of closed channel")
- Shortened cancel-path flush from 15s → 2s so the Stop button feels snappy

### Bootstrap reliability

The old generator's "Kibana liveness check" used `http.NewRequest` which
*builds* a request but never sends it — `err` was only non-nil on malformed
URLs, so the check trivially passed. The real liveness check is now
`waitForReady` polling `GET /api/status` until Kibana returns
`status.overall.level: "available"`, with a 5-minute timeout polling every 2s.
Same pattern for Kafka Connect (`GET /connectors`).

### New module / dependencies

- `github.com/gin-gonic/gin v1.10.0` — HTTP framework
- `golang.org/x/time v0.15.0` — `rate.Limiter` for target-rate enforcement
- `github.com/confluentinc/confluent-kafka-go v1.9.2` — moved from `client/`
  module
- `github.com/brianvoe/gofakeit/v6 v6.28.0` — moved from `client/` module

### Verification done

- Cold-start: `docker compose up` brings whole stack online with `restarts=0`
  across all services
- Manual UI flow: Start → producer hits target rate within ±2%, Stop →
  immediate `stopping` state, throughput card drops to 0
- Broker failure: stopping Kafka mid-run flips state to `failed` within 6s
- Compression: zstd vs none on 10k messages → ~6.7× smaller broker storage
- Probe accuracy: each service reports `healthy` when up; ES legitimately
  reports `warn` (yellow, single-node, expected) without blocking Start;
  killing logstash flips its dot red and fires the toast within 5–7s

### Known-good state

The stack now boots clean, the UI controls real behavior, telemetry is
honest, and stopping any single service produces the correct UI signal
without taper or false counting.
