// Single entry point for data.
// Live mode: the app (`uv run mini-cortex`, app/mini_cortex/server.py) is reachable at /api, so connectors, protocol frames, intake,
// packets, memory decisions and chat are real (app/mini_cortex). Mock mode: everything comes from mock.ts.
// Lenses are still mock in both modes.
import { fmtDayTime } from '../fmt'
import * as mock from './mock'
import type {
  ChatEvent,
  ChatMessage,
  ConnectorInfo,
  DataMode,
  DoctorCheck,
  Lens,
  LlmDraft,
  LlmModel,
  LlmSettings,
  MemoryLogEntry,
  Packet,
  PacketSources,
  PipelineProgress,
  PipelineStats,
  RawItem,
  RawSource,
  Settings,
  Thread,
  WireFrame,
} from './types'

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms))
const clone = <T>(v: T): T => structuredClone(v)

let modePromise: Promise<DataMode> | null = null

function detectMode(): Promise<DataMode> {
  modePromise ??= (async () => {
    try {
      const r = await fetch('/api/health', { signal: AbortSignal.timeout(1500) })
      return r.ok && (await r.json()).mode === 'live' ? 'live' : 'mock'
    } catch {
      return 'mock'
    }
  })()
  return modePromise
}

async function get<T>(path: string): Promise<T> {
  const r = await fetch(path)
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`)
  return r.json()
}

/** An API error with the server's suggested fix (doctor-style), when it gave one. */
export class ApiError extends Error {
  fix: string
  constructor(message: string, fix = '') {
    super(message)
    this.fix = fix
  }
}

async function send<T>(method: 'POST' | 'PUT', path: string, body: unknown): Promise<T> {
  const r = await fetch(path, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
  const data = await r.json().catch(() => ({}))
  if (!r.ok) {
    const d = data.detail
    if (d && typeof d === 'object' && 'error' in d) throw new ApiError(d.error, d.fix)
    throw new ApiError(typeof d === 'string' ? d : `${path}: HTTP ${r.status}`)
  }
  return data
}

let mockLlm = clone(mock.llmSettings)

export interface LiveHandlers {
  frame?: (f: WireFrame) => void
  intake?: (e: MemoryLogEntry) => void
  connectors?: (c: ConnectorInfo[]) => void
  /** Packetize / dedup decisions as they are made. */
  memory?: (e: MemoryLogEntry) => void
  stats?: (s: PipelineStats) => void
  progress?: (p: PipelineProgress) => void
}

/** Server-sent events from a POST body (EventSource only does GET). */
async function* sse(r: Response): AsyncGenerator<{ event: string; data: string }> {
  const reader = r.body!.pipeThrough(new TextDecoderStream()).getReader()
  let buf = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return
    buf += value
    let i
    while ((i = buf.indexOf('\n\n')) >= 0) {
      const block = buf.slice(0, i)
      buf = buf.slice(i + 2)
      const event = block.match(/^event: (.*)$/m)?.[1] ?? 'message'
      const data = block
        .split('\n')
        .filter((l) => l.startsWith('data: '))
        .map((l) => l.slice(6))
        .join('\n')
      if (data) yield { event, data }
    }
  }
}

export const api = {
  mode: detectMode,

  /** Live events from the app. Returns an unsubscribe function; a no-op in mock mode. */
  subscribe(h: LiveHandlers): () => void {
    let es: EventSource | null = null
    let closed = false
    detectMode().then((m) => {
      if (m !== 'live' || closed) return
      es = new EventSource('/api/events')
      if (h.frame) es.addEventListener('frame', (e) => h.frame!(JSON.parse((e as MessageEvent).data)))
      if (h.intake) es.addEventListener('intake', (e) => h.intake!(JSON.parse((e as MessageEvent).data)))
      if (h.connectors) es.addEventListener('connectors', (e) => h.connectors!(JSON.parse((e as MessageEvent).data)))
      if (h.memory) es.addEventListener('memory', (e) => h.memory!(JSON.parse((e as MessageEvent).data)))
      if (h.stats) es.addEventListener('stats', (e) => h.stats!(JSON.parse((e as MessageEvent).data)))
      if (h.progress) es.addEventListener('progress', (e) => h.progress!(JSON.parse((e as MessageEvent).data)))
    })
    return () => {
      closed = true
      es?.close()
    }
  },

  async stats(): Promise<PipelineStats> {
    if ((await detectMode()) === 'live') return get('/api/stats')
    return clone(mock.stats)
  },
  /** Packetize every pending item (progress arrives as `progress` events). */
  async process(): Promise<{ started?: boolean; busy?: boolean; pending?: number }> {
    if ((await detectMode()) === 'live') return send('POST', '/api/pipeline/process', {})
    return { started: false }
  },
  /** Forget all packets and decisions; every item goes back to pending. */
  async rebuild(): Promise<PipelineStats> {
    return send('POST', '/api/pipeline/rebuild', {})
  },
  async memoryLog(): Promise<MemoryLogEntry[]> {
    if ((await detectMode()) === 'live') return get('/api/memory-log')
    return clone(mock.memoryLog)
  },
  async packets(): Promise<Packet[]> {
    if ((await detectMode()) === 'live') return get('/api/packets')
    return clone(mock.packets)
  },
  /** One original item (any connector). Demo mode has no raw text. */
  async rawItem(connector: string, sourceId: string): Promise<RawSource | null> {
    if ((await detectMode()) !== 'live') return null
    const q = new URLSearchParams({ connector, source_id: sourceId })
    return get(`/api/raw?${q}`)
  },
  /** A packet and the original messages behind it. Demo packets have no raw text. */
  async packetSources(id: string): Promise<PacketSources | null> {
    if ((await detectMode()) === 'live') return get(`/api/packets/${encodeURIComponent(id)}/sources`)
    const p = mock.packets.find((x) => x.id === id)
    return p ? { packet: clone(p), sources: p.sources.map((s) => ({ ...s, text: null })) } : null
  },
  async connectors(): Promise<{ hub: string; connectors: ConnectorInfo[] }> {
    if ((await detectMode()) === 'live') return get('/api/connectors')
    return { hub: mock.hubAddress, connectors: clone(mock.connectors) }
  },
  async frames(): Promise<WireFrame[]> {
    if ((await detectMode()) === 'live') return get('/api/frames')
    return clone(mock.frames)
  },
  async items(limit = 200): Promise<RawItem[]> {
    if ((await detectMode()) === 'live') return get(`/api/items?limit=${limit}`)
    return []
  },
  async lenses(): Promise<Lens[]> {
    return clone(mock.lenses)
  },
  async settings(): Promise<Settings> {
    if ((await detectMode()) === 'live') return get('/api/settings')
    return clone(mock.defaultSettings)
  },
  /** Live: saves θ, buffer and packet types to memory_types.yaml; existing packets change only after a rebuild. */
  async applySettings(s: Settings): Promise<{ queued: number; saved?: boolean }> {
    if ((await detectMode()) === 'live') {
      await send('PUT', '/api/settings', s)
      return { queued: 0, saved: true }
    }
    await wait(400)
    return { queued: mock.stats.intake }
  },

  // ---------- models & keys ----------

  async llm(): Promise<LlmSettings> {
    if ((await detectMode()) === 'live') return get('/api/llm/settings')
    return clone(mockLlm)
  },
  /** Saves to the local .env. Leave `api_key` null to keep the stored key. */
  async saveLlm(d: LlmDraft): Promise<LlmSettings> {
    if ((await detectMode()) === 'live') return send('PUT', '/api/llm/settings', d)
    await wait(300)
    const { api_key, ...rest } = d
    mockLlm = {
      ...mockLlm,
      ...rest,
      key_set: mockLlm.key_set || !!api_key,
      key_hint: api_key ? `••••${api_key.slice(-4)}` : mockLlm.key_hint,
    }
    return clone(mockLlm)
  },
  /** The provider's model list, for unsaved settings (so the dropdowns fill before Save). */
  async llmModels(d: LlmDraft): Promise<LlmModel[]> {
    if ((await detectMode()) === 'live') return (await send<{ models: LlmModel[] }>('POST', '/api/llm/models', d)).models
    await wait(350)
    return clone(mock.llmModels)
  },
  /** The `doctor` checks: chat, structured output, tool calling, embeddings. */
  async testLlm(d: LlmDraft): Promise<{ ok: boolean; checks: DoctorCheck[] }> {
    if ((await detectMode()) === 'live') return send('POST', '/api/llm/test', d)
    await wait(900)
    return { ok: true, checks: clone(mock.doctorChecks) }
  },

  async threads(): Promise<Thread[]> {
    if ((await detectMode()) === 'live') {
      const ts = await get<{ id: string; title: string; updated: string }[]>('/api/threads')
      return ts.map((t) => ({ ...t, updated: fmtDayTime(t.updated) }))
    }
    return clone(mock.threads)
  },
  async messages(threadId: string): Promise<ChatMessage[]> {
    if ((await detectMode()) === 'live') return get(`/api/threads/${encodeURIComponent(threadId)}/messages`)
    return clone(mock.initialMessages[threadId] ?? [])
  },

  /**
   * Ask a connector for a time window (protocol `fetch`). Live: the hub runs it and the frames arrive as events.
   * Mock: returns a fake frame for the caller to show.
   */
  async fetchWindow(req: {
    connector: string
    stream: string
    since: string
    until: string
  }): Promise<{ items?: number; frame?: WireFrame }> {
    if ((await detectMode()) === 'live') {
      const r = await fetch('/api/fetch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req),
      })
      const body = await r.json()
      if (!r.ok) throw new Error(body.detail ?? `HTTP ${r.status}`)
      return body
    }
    await wait(150)
    return {
      frame: {
        id: `f-${Date.now()}`,
        at: new Date().toISOString(),
        dir: 'out',
        method: 'fetch',
        summary: `${req.stream} · ${req.since.slice(0, 10)} → ${req.until.slice(0, 10)} · limit 50`,
        payload: {
          jsonrpc: '2.0',
          id: Math.floor(Math.random() * 1000),
          method: 'fetch',
          params: { stream: req.stream.replace(/^#/, ''), since: req.since, until: req.until, cursor: null, limit: 50 },
        },
      },
    }
  },

  /** Streams one assistant turn as events. Live: the chat agent over SSE; `threadId` is created on first use. */
  async *ask(question: string, threadId?: string): AsyncGenerator<ChatEvent> {
    if ((await detectMode()) === 'live') {
      const r = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: question, thread_id: threadId }),
      })
      if (!r.ok || !r.body) {
        yield { type: 'error', message: `Chat failed: HTTP ${r.status}` }
        yield { type: 'done' }
        return
      }
      for await (const { data } of sse(r)) yield JSON.parse(data) as ChatEvent
      return
    }
    const c = mock.canned.find((x) => x.match.test(question)) ?? {
      ...mock.fallback,
      steps: [{ ...mock.fallback.steps[0], args: `"${question.slice(0, 48)}"` }],
    }
    for (const step of c.steps) {
      await wait(450)
      yield { type: 'step', step }
    }
    await wait(250)
    yield { type: 'citations', citations: c.citations }
    for (const word of c.text.split(/(?<=\s)/)) {
      await wait(28)
      yield { type: 'token', text: word }
    }
    if (c.superseded) yield { type: 'superseded', items: c.superseded }
    yield { type: 'done' }
  },
}
