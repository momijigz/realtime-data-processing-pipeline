import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { api, type StackStatus, type Throughput, type Run, type CompressionType } from './api/client'

// Services whose absence breaks a useful run. If any of these are "down"
// (per the probe poller) we block Start and show a toast. Note: "warn"
// (e.g. ES yellow) is fine — single-node clusters live there.
const CRUCIAL_SERVICES: (keyof StackStatus)[] = [
  'kafka',
  'elasticsearch',
  'kibana',
  'kafkaConnect',
  'logstash',
]

export function App() {
  const [stack, setStack] = useState<StackStatus | null>(null)
  const [throughput, setThroughput] = useState<Throughput | null>(null)
  const [run, setRun] = useState<Run | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [logs, setLogs] = useState<LogEntry[]>([])

  // Inputs for the next run. Sliders carry indices into the *_STOPS arrays
  // below; lookup happens just before send. Index defaults are chosen as
  // sensible scale-lab starting points.
  const [rateIdx, setRateIdx] = useState(2)   // 1,000 msg/s
  const [limitIdx, setLimitIdx] = useState(1) // 10,000 messages
  const [flushIdx, setFlushIdx] = useState(0) // 5s
  const [lingerInput, setLingerInput] = useState('')
  const [batchSizeInput, setBatchSizeInput] = useState('')
  const [compressionInput, setCompressionInput] = useState<CompressionType>('')

  // Toast stack — top-right notifications. Identified by `key` so we can
  // dedupe transition events and dismiss on recovery.
  const [toasts, setToasts] = useState<Toast[]>([])
  const pushToast = useCallback((t: Toast) => {
    setToasts((cur) => (cur.some((x) => x.key === t.key) ? cur : [...cur, t]))
  }, [])
  const dismissToast = useCallback((key: string) => {
    setToasts((cur) => cur.filter((t) => t.key !== key))
  }, [])

  // Track the previous stack snapshot so we can fire toasts only on
  // healthy→down transitions (not every poll while down).
  const prevStackRef = useRef<StackStatus | null>(null)

  const appendLog = useCallback((kind: LogEntry['kind'], message: string) => {
    setLogs((prev) => {
      const next = [...prev, { ts: new Date(), kind, message }]
      // Cap at 500 entries so the panel doesn't grow unbounded.
      return next.length > 500 ? next.slice(-500) : next
    })
  }, [])

  const loadStack = useCallback(async () => {
    try {
      const s = await api.stackStatus()
      setStack(s)
      setError(null)
      appendLog('info', 'refreshed stack status')
    } catch (e: any) {
      const msg = e?.error ?? String(e)
      setError(msg)
      appendLog('error', `stack status failed: ${msg}`)
    }
  }, [appendLog])

  const refreshRun = useCallback(async () => {
    try {
      const r = await api.producerStatus()
      setRun(r.run)
    } catch (e: any) {
      setError(e?.error ?? String(e))
    }
  }, [])

  useEffect(() => {
    loadStack()
    refreshRun()
    appendLog('info', 'UI initialized — polling every 2s')
    const t = setInterval(async () => {
      try {
        const [tp, rs, ss] = await Promise.all([
          api.throughput(),
          api.producerStatus(),
          api.stackStatus(),
        ])
        setThroughput(tp)
        setRun(rs.run)
        setStack(ss)
      } catch (e: any) {
        setError(e?.error ?? String(e))
      }
    }, 2000)
    return () => clearInterval(t)
  }, [loadStack, refreshRun, appendLog])

  // "Active" = the goroutine is still doing something (producing or draining).
  // Inputs and Start stay disabled until it's fully done.
  const runIsActive =
    run?.State === 'running' || run?.State === 'flushing' || run?.State === 'stopping'

  // Crucial services currently reporting "down". Memoized so identity stays
  // stable across renders when nothing has changed.
  const downServices = useMemo<string[]>(() => {
    if (!stack) return []
    return CRUCIAL_SERVICES.filter((s) => mapStatus(stack[s]) === 'down')
  }, [stack])

  // Fire a toast on healthy→down transitions; remove a service's toast on
  // recovery. Comparing against prevStackRef avoids spamming every poll tick.
  useEffect(() => {
    if (!stack) return
    const prev = prevStackRef.current
    const newlyDown: string[] = []
    const newlyUp: string[] = []
    for (const s of CRUCIAL_SERVICES) {
      const now = mapStatus(stack[s])
      const before = prev ? mapStatus(prev[s]) : 'healthy'
      if (now === 'down' && before !== 'down') newlyDown.push(s)
      if (now !== 'down' && before === 'down') newlyUp.push(s)
    }
    if (newlyDown.length || newlyUp.length) {
      setToasts((cur) => {
        // Drop toasts for recovered services.
        let next = cur.filter((t) => !newlyUp.includes(t.service))
        // Add toasts for newly down services (dedupe by service).
        for (const svc of newlyDown) {
          if (next.some((t) => t.service === svc)) continue
          next = [
            ...next,
            {
              key: `${svc}-${Date.now()}`,
              service: svc,
              message: `${svc} is unreachable — pipeline runs will fail until it recovers.`,
            },
          ]
        }
        return next
      })
      for (const svc of newlyDown) appendLog('error', `${svc} went down`)
      for (const svc of newlyUp) appendLog('info', `${svc} recovered`)
    }
    prevStackRef.current = stack
  }, [stack, appendLog])

  const disabledReason =
    downServices.length > 0 ? `${downServices.join(', ')} unreachable` : null

  const handleStart = async () => {
    // Defensive: button should already be disabled when crucial services are
    // down. This catches polling-lag edge cases (service went down between
    // render and click) and surfaces a clear reason instead of letting the
    // generator crash 5 seconds later.
    if (downServices.length > 0) {
      pushToast({
        key: `blocked-${Date.now()}`,
        service: 'blocked',
        message: `Cannot start — ${downServices.join(', ')} unreachable`,
      })
      return
    }
    setBusy(true)
    setError(null)
    const opts: {
      targetRate?: number
      limit?: number
      flushTimeoutMs?: number
      lingerMs?: number
      batchSize?: number
      compressionType?: CompressionType
    } = {
      targetRate: RATE_STOPS[rateIdx].value,
      limit: LIMIT_STOPS[limitIdx].value,
      flushTimeoutMs: FLUSH_STOPS[flushIdx].value,
    }
    if (lingerInput.trim()) {
      const n = Number(lingerInput)
      if (!Number.isFinite(n) || n < 0) {
        setError('linger.ms must be a non-negative integer')
        setBusy(false)
        return
      }
      opts.lingerMs = Math.trunc(n)
    }
    if (batchSizeInput.trim()) {
      const n = Number(batchSizeInput)
      if (!Number.isFinite(n) || n < 0) {
        setError('batch.size must be a non-negative integer (bytes)')
        setBusy(false)
        return
      }
      opts.batchSize = Math.trunc(n)
    }
    if (compressionInput) {
      opts.compressionType = compressionInput
    }
    appendLog(
      'action',
      `clicked Start (rate=${RATE_STOPS[rateIdx].label}, limit=${LIMIT_STOPS[limitIdx].label}, flush=${FLUSH_STOPS[flushIdx].label}, linger=${opts.lingerMs ?? 'default'}, batch=${opts.batchSize ?? 'default'}, compression=${opts.compressionType || 'default'})`,
    )
    try {
      const r = await api.producerStart(opts)
      setRun(r)
      appendLog('info', `run started: ${r.ID}`)
    } catch (e: any) {
      const msg = e?.error ?? String(e)
      setError(msg)
      appendLog('error', `start failed: ${msg}`)
    } finally {
      setBusy(false)
    }
  }

  const handleStop = async () => {
    setBusy(true)
    setError(null)
    appendLog('action', 'clicked Stop')
    try {
      const r = await api.producerStop()
      setRun(r)
      appendLog('info', `run stopping: ${r.ID} (sent ${r.Sent.toLocaleString()})`)
    } catch (e: any) {
      const msg = e?.error ?? String(e)
      setError(msg)
      appendLog('error', `stop failed: ${msg}`)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-6 font-sans">
      <Toaster toasts={toasts} onDismiss={dismissToast} />
      <div className="max-w-[1600px] mx-auto space-y-4">
        <header>
          <h1 className="text-2xl font-semibold">Realtime Pipeline Lab</h1>
          <p className="text-slate-400 text-sm">
            Trigger a steady-rate Kafka producer from the API. Defaults: 1M messages, 10µs gap.
          </p>
        </header>

        {error && (
          <div className="bg-red-950 border border-red-800 text-red-200 p-3 rounded text-sm">
            {error}
          </div>
        )}

        <div className="grid grid-cols-[280px_1fr_320px] gap-4 min-w-[1024px]">
          <ControlsCard
            busy={busy}
            run={run}
            runIsActive={runIsActive}
            disabledReason={disabledReason}
            onStart={handleStart}
            onStop={handleStop}
            rateIdx={rateIdx}
            limitIdx={limitIdx}
            flushIdx={flushIdx}
            linger={lingerInput}
            batchSize={batchSizeInput}
            compression={compressionInput}
            onRateIdx={setRateIdx}
            onLimitIdx={setLimitIdx}
            onFlushIdx={setFlushIdx}
            onLinger={setLingerInput}
            onBatchSize={setBatchSizeInput}
            onCompression={setCompressionInput}
          />
          <div className="flex flex-col gap-4">
            <KpiRow throughput={throughput} run={run} />
            <LogsCard logs={logs} onClear={() => setLogs([])} />
          </div>
          <div className="flex flex-col gap-4">
            <RunCard run={run} throughput={throughput} />
            <StatusCard stack={stack} onRefresh={loadStack} />
          </div>
        </div>
      </div>
    </div>
  )
}

// ---------- Controls ----------

// ---------- Slider stops ----------

type Stop = { value: number; label: string }

// Discrete stops for each slider. value is what gets sent to the API:
//   - rate: 0 = unlimited
//   - limit: -1 = unbounded
//   - flush: ms
const RATE_STOPS: Stop[] = [
  { value: 10, label: '10' },
  { value: 100, label: '100' },
  { value: 1000, label: '1k' },
  { value: 10000, label: '10k' },
  { value: 100000, label: '100k' },
  { value: 1000000, label: '1M' },
  { value: 0, label: '∞' },
]
const LIMIT_STOPS: Stop[] = [
  { value: 1000, label: '1k' },
  { value: 10000, label: '10k' },
  { value: 100000, label: '100k' },
  { value: 1000000, label: '1M' },
  { value: 10000000, label: '10M' },
  { value: -1, label: '∞' },
]
const FLUSH_STOPS: Stop[] = [
  { value: 5000, label: '5s' },
  { value: 10000, label: '10s' },
  { value: 15000, label: '15s' },
]

function ControlsCard({
  busy,
  run,
  runIsActive,
  disabledReason,
  onStart,
  onStop,
  rateIdx,
  limitIdx,
  flushIdx,
  linger,
  batchSize,
  compression,
  onRateIdx,
  onLimitIdx,
  onFlushIdx,
  onLinger,
  onBatchSize,
  onCompression,
}: {
  busy: boolean
  run: Run | null
  runIsActive: boolean
  disabledReason: string | null
  onStart: () => void
  onStop: () => void
  rateIdx: number
  limitIdx: number
  flushIdx: number
  linger: string
  batchSize: string
  compression: CompressionType
  onRateIdx: (n: number) => void
  onLimitIdx: (n: number) => void
  onFlushIdx: (n: number) => void
  onLinger: (v: string) => void
  onBatchSize: (v: string) => void
  onCompression: (v: CompressionType) => void
}) {
  const startBlocked = !!disabledReason
  return (
    <Card title="Controls">
      <div className="space-y-4">
        <Section label="Producer">
          <Field label="Target Rate (msg/s)">
            <Slider
              stops={RATE_STOPS}
              index={rateIdx}
              onChange={onRateIdx}
              disabled={runIsActive}
            />
          </Field>
          <Field label="Message Limit">
            <Slider
              stops={LIMIT_STOPS}
              index={limitIdx}
              onChange={onLimitIdx}
              disabled={runIsActive}
            />
          </Field>
          <Field label="Flush Timeout">
            <Slider
              stops={FLUSH_STOPS}
              index={flushIdx}
              onChange={onFlushIdx}
              disabled={runIsActive}
            />
          </Field>
        </Section>

        <Section label="Batching">
          <Field label="linger.ms">
            <input
              type="number"
              inputMode="numeric"
              min={0}
              placeholder="librdkafka default (5)"
              disabled={runIsActive}
              value={linger}
              onChange={(e) => onLinger(e.target.value)}
              className={inputCls}
            />
          </Field>
          <Field label="batch.size (bytes)">
            <input
              type="number"
              inputMode="numeric"
              min={0}
              placeholder="librdkafka default (~16KB)"
              disabled={runIsActive}
              value={batchSize}
              onChange={(e) => onBatchSize(e.target.value)}
              className={inputCls}
            />
          </Field>
          <Field label="compression.type">
            <select
              disabled={runIsActive}
              value={compression}
              onChange={(e) => onCompression(e.target.value as CompressionType)}
              className={inputCls}
            >
              <option value="">default (none)</option>
              <option value="none">none</option>
              <option value="gzip">gzip</option>
              <option value="snappy">snappy</option>
              <option value="lz4">lz4</option>
              <option value="zstd">zstd</option>
            </select>
          </Field>
        </Section>

        <div className="space-y-2 pt-1">
          <button
            onClick={onStart}
            disabled={busy || runIsActive || startBlocked}
            title={startBlocked ? disabledReason! : undefined}
            className={`${btn} w-full ${runIsActive || startBlocked ? 'opacity-40 cursor-not-allowed' : ''}`}
          >
            {run?.State === 'stopping'
              ? 'Stopping…'
              : run?.State === 'flushing'
              ? 'Flushing…'
              : run?.State === 'running'
              ? 'Running…'
              : 'Start'}
          </button>
          {startBlocked && (
            <p className="text-[10px] text-red-400 leading-tight">{disabledReason}</p>
          )}
          <button
            onClick={onStop}
            disabled={busy || !runIsActive}
            className={`${btn} w-full ${!runIsActive ? 'opacity-40 cursor-not-allowed' : ''}`}
          >
            Stop
          </button>
        </div>
      </div>
    </Card>
  )
}

function Section({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-2">
      <div className="text-[10px] text-slate-500 uppercase tracking-wider font-semibold border-b border-slate-800 pb-1">
        {label}
      </div>
      <div className="space-y-2">{children}</div>
    </div>
  )
}

// Slider over a fixed list of discrete stops. Linear native range under the
// hood (cleaner than canvas), index 0..stops.length-1. The currently-selected
// stop's label floats above the track; all stop labels render below as ticks.
function Slider({
  stops,
  index,
  onChange,
  disabled,
}: {
  stops: Stop[]
  index: number
  onChange: (n: number) => void
  disabled?: boolean
}) {
  const max = stops.length - 1
  const current = stops[Math.min(Math.max(index, 0), max)]
  return (
    <div className={disabled ? 'opacity-50' : ''}>
      <div className="flex justify-between items-baseline mb-1">
        <span className="font-mono text-sm text-slate-100">{current.label}</span>
        <span className="text-[10px] text-slate-600">
          {index + 1} of {stops.length}
        </span>
      </div>
      <input
        type="range"
        min={0}
        max={max}
        step={1}
        value={index}
        disabled={disabled}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-full accent-emerald-500 cursor-pointer disabled:cursor-not-allowed"
      />
      <div className="flex justify-between mt-1 text-[9px] text-slate-600 font-mono">
        {stops.map((s, i) => (
          <span key={i} className={i === index ? 'text-slate-300' : ''}>
            {s.label}
          </span>
        ))}
      </div>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="text-[10px] text-slate-500 uppercase tracking-wide block mb-1">
        {label}
      </label>
      {children}
    </div>
  )
}

const inputCls =
  'w-full bg-slate-950 border border-slate-700 rounded px-2 py-1.5 text-sm font-mono text-slate-100 placeholder:text-slate-600 focus:outline-none focus:border-slate-500 disabled:opacity-50 disabled:cursor-not-allowed'

function RunCard({ run, throughput }: { run: Run | null; throughput: Throughput | null }) {
  return (
    <Card title="Producer">
      {run ? (
        <div className="text-xs space-y-1.5">
          <div className="flex justify-between gap-2">
            <span className="text-slate-400">ID</span>
            <span className="font-mono truncate">{run.ID}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-slate-400">State</span>
            <StateBadge state={run.State} />
          </div>
          <div className="flex justify-between">
            <span className="text-slate-400">Sent</span>
            <span className="font-mono">{run.Sent.toLocaleString()}</span>
          </div>
          {run.Err && (
            <div className="flex justify-between">
              <span className="text-slate-400">Error</span>
              <span className="text-red-400 truncate">{run.Err}</span>
            </div>
          )}
        </div>
      ) : (
        <p className="text-slate-500 text-sm">no run yet</p>
      )}

      <div className="mt-4 pt-3 border-t border-slate-800">
        <div className="text-slate-500 uppercase tracking-wide text-[10px] mb-2">Throughput</div>
        {throughput ? (
          <ul className="text-xs space-y-1">
            <li className="flex justify-between">
              <span className="text-slate-400">Produced/sec</span>
              <span className="font-mono text-slate-100">{throughput.producedPerSec.toFixed(0)}</span>
            </li>
            <li className="flex justify-between">
              <span className="text-slate-400">Consumed/sec</span>
              <span className="font-mono text-slate-500">— (todo)</span>
            </li>
          </ul>
        ) : (
          <p className="text-slate-500 text-xs">waiting for first poll…</p>
        )}
      </div>
    </Card>
  )
}

// ---------- KPI row (above logs) ----------

function KpiRow({ throughput, run }: { throughput: Throughput | null; run: Run | null }) {
  const tput = throughput?.producedPerSec ?? 0
  const sent = run?.Sent ?? 0
  const bytes = run?.BytesSent ?? 0

  return (
    <div className="grid grid-cols-3 gap-3">
      <KpiCard label="Throughput" value={tput.toFixed(0)} unit="msg/s" />
      <KpiCard label="Messages Sent" value={sent.toLocaleString()} unit="total" />
      <KpiCard label="Bytes Sent" value={formatBytes(bytes)} unit="total" />
    </div>
  )
}

function KpiCard({ label, value, unit }: { label: string; value: string; unit: string }) {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded p-4">
      <div className="text-[10px] text-slate-500 uppercase tracking-wide">{label}</div>
      <div className="mt-1 flex items-baseline gap-1.5">
        <span className="text-2xl font-mono text-slate-100">{value}</span>
        <span className="text-xs text-slate-500">{unit}</span>
      </div>
    </div>
  )
}

// formatBytes scales raw bytes into a human-readable string (B / KB / MB / GB).
// 1024-based — matches what disk/network tools typically show.
function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let v = n / 1024
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`
}

// ---------- Logs (mocked content from UI events) ----------

type LogEntry = { ts: Date; kind: 'info' | 'action' | 'error'; message: string }

function LogsCard({ logs, onClear }: { logs: LogEntry[]; onClear: () => void }) {
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    el.scrollTop = el.scrollHeight
  }, [logs])

  return (
    <Card
      title="Logs"
      subtitle="mocked from UI events"
      action={
        <button onClick={onClear} className={`${btn} text-xs`}>
          Clear
        </button>
      }
    >
      <div
        ref={scrollRef}
        className="bg-black/40 border border-slate-800 rounded p-2 font-mono text-xs h-[480px] overflow-y-auto"
      >
        {logs.length === 0 ? (
          <p className="text-slate-600">no entries yet</p>
        ) : (
          logs.map((l, i) => (
            <div key={i} className="leading-relaxed">
              <span className="text-slate-500">{fmtTime(l.ts)}</span>{' '}
              <span className={logColor[l.kind]}>{l.kind.padEnd(6)}</span>{' '}
              <span className="text-slate-200">{l.message}</span>
            </div>
          ))
        )}
      </div>
    </Card>
  )
}

const logColor: Record<LogEntry['kind'], string> = {
  info: 'text-sky-400',
  action: 'text-emerald-400',
  error: 'text-red-400',
}

function fmtTime(d: Date): string {
  return d.toTimeString().slice(0, 8)
}

// ---------- Status + Throughput ----------

function StatusCard({
  stack,
  onRefresh,
}: {
  stack: StackStatus | null
  onRefresh: () => void
}) {
  return (
    <Card
      title="Stack Status"
      action={<button onClick={onRefresh} className={`${btn} text-xs`}>Refresh</button>}
      className="flex-1"
    >
      {stack ? (
        <ul className="text-xs">
          {Object.entries(stack).map(([k, v]) => (
            <li key={k} className="flex justify-between items-center border-b border-slate-800 py-0.5 leading-tight">
              <span className="text-slate-300">{k}</span>
              <StatusDot status={mapStatus(v)} title={v} />
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-slate-500 text-xs">loading…</p>
      )}
    </Card>
  )
}

// Maps the API's string status to a semantic dot color. The probe poller
// returns exactly "healthy" | "warn" | "down" but we still normalize defensively.
type DotStatus = 'healthy' | 'warn' | 'down'
function mapStatus(s: string): DotStatus {
  const lower = s.toLowerCase()
  if (lower === 'healthy' || lower === 'ok' || lower === 'available' || lower === 'green') return 'healthy'
  if (lower === 'warn' || lower === 'yellow' || lower.startsWith('start') || lower === 'degraded') return 'warn'
  return 'down'
}

function StatusDot({ status, title }: { status: DotStatus; title?: string }) {
  const style: Record<DotStatus, string> = {
    healthy: 'bg-emerald-400 shadow-[0_0_6px_rgba(52,211,153,0.6)]',
    warn: 'bg-amber-400 shadow-[0_0_6px_rgba(251,191,36,0.6)]',
    down: 'bg-red-500 shadow-[0_0_6px_rgba(239,68,68,0.6)]',
  }
  return <span title={title ?? status} className={`inline-block w-1.5 h-1.5 rounded-full ${style[status]}`} />
}

// ---------- Run state badge (reused in Controls card) ----------

function StateBadge({ state }: { state: Run['State'] }) {
  const color: Record<Run['State'], string> = {
    running: 'text-emerald-400',
    flushing: 'text-sky-300',
    stopping: 'text-amber-300',
    finished: 'text-blue-400',
    stopped: 'text-amber-400',
    failed: 'text-red-400',
  }
  return <span className={`font-mono ${color[state]}`}>{state}</span>
}

// ---------- Toaster ----------

type Toast = { key: string; service: string; message: string }

function Toaster({ toasts, onDismiss }: { toasts: Toast[]; onDismiss: (key: string) => void }) {
  if (toasts.length === 0) return null
  return (
    <div className="fixed top-4 right-4 z-50 space-y-2 w-80 max-w-[calc(100vw-2rem)]">
      {toasts.map((t) => (
        <div
          key={t.key}
          className="bg-red-950/95 border border-red-800 text-red-100 rounded shadow-lg p-3 flex items-start gap-2"
          role="alert"
        >
          <span className="text-red-400 mt-0.5 select-none">●</span>
          <div className="flex-1 text-xs leading-relaxed">{t.message}</div>
          <button
            onClick={() => onDismiss(t.key)}
            aria-label="Dismiss"
            className="text-red-300 hover:text-red-100 text-sm leading-none"
          >
            ×
          </button>
        </div>
      ))}
    </div>
  )
}

// ---------- Card primitive ----------

function Card({
  title,
  subtitle,
  action,
  className = '',
  children,
}: {
  title: string
  subtitle?: string
  action?: React.ReactNode
  className?: string
  children: React.ReactNode
}) {
  return (
    <section className={`bg-slate-900 border border-slate-800 rounded p-4 ${className}`}>
      <div className="flex items-center justify-between mb-3">
        <div>
          <h2 className="font-medium">{title}</h2>
          {subtitle && <p className="text-[10px] text-slate-500 uppercase tracking-wide">{subtitle}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  )
}

const btn =
  'px-3 py-1.5 rounded bg-slate-800 hover:bg-slate-700 active:bg-slate-600 text-sm border border-slate-700 disabled:hover:bg-slate-800'
