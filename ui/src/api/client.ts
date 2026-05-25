// Thin typed wrapper around the Go API. All paths are relative so they go
// through the Vite proxy in dev and a future reverse proxy in prod.

export type ApiError = { error: string; status: number }

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init)
  const body = await res.json().catch(() => ({}))
  if (!res.ok) {
    const err: ApiError = { error: body.error ?? res.statusText, status: res.status }
    throw err
  }
  return body as T
}

export type StackStatus = {
  kafka: string
  elasticsearch: string
  kibana: string
  kafkaConnect: string
  logstash: string
}

export type RunState = 'running' | 'flushing' | 'stopping' | 'stopped' | 'finished' | 'failed'

export type Run = {
  ID: string
  State: RunState
  StartedAt: string
  FinishedAt: string // RFC3339, "0001-01-01T00:00:00Z" when zero
  Sent: number
  BytesSent: number
  Err: string
}

export type Throughput = {
  producedPerSec: number
  consumedPerSec: number
}

export type CompressionType = '' | 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd'

export type StartOptions = {
  targetRate?: number      // msg/s; 0 = unlimited; undefined = server default
  limit?: number           // total messages; -1 = unbounded; undefined = server default
  flushTimeoutMs?: number  // drain timeout at natural end of run, in ms; undefined = server default (15000)
  lingerMs?: number        // queue.buffering.max.ms; undefined = librdkafka default
  batchSize?: number       // batch.size in bytes; undefined = librdkafka default
  compressionType?: CompressionType // undefined or '' = librdkafka default
}

export const api = {
  health: () => request<{ status: string }>('/api/health'),
  stackStatus: () => request<StackStatus>('/api/stack/status'),
  producerStart: (opts?: StartOptions) =>
    request<Run>('/api/producer/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(opts ?? {}),
    }),
  producerStop: () => request<Run>('/api/producer/stop', { method: 'POST' }),
  producerStatus: () => request<{ run: Run | null }>('/api/producer/status'),
  throughput: () => request<Throughput>('/api/metrics/throughput'),
}
