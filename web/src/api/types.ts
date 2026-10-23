// Shapes shared by the UI and the backend (app/mini_cortex). Item shapes follow PROTOCOL.md.

export type PacketType =
  | 'fact'
  | 'event'
  | 'decision'
  | 'person'
  | 'preference'
  | 'reference'
  | 'warning'
  | 'commitment'
  | 'summary'

export interface SourceRef {
  source_id: string
  connector: string
  stream: string
  timestamp: string
  author?: string
  label?: string
  /** Link to the original message (live sources that have one). */
  url?: string | null
}

export interface Packet {
  id: string
  type: PacketType
  title: string
  content: string
  entities: string[]
  sources: SourceRef[]
  source_timestamp: string
  created_at: string
  updated_at: string
  deprecated: boolean
  superseded_by: string | null
  access_count: number
}

/** One original item from a connector, as stored at intake. */
export interface RawSource {
  connector: string
  stream: string
  source_id: string
  text: string | null
  url: string | null
  author: string | null
  timestamp: string
  reply_to?: string | null
  edited?: boolean
  status: 'pending' | 'packetized' | 'error'
}

/** A packet with the original messages it was extracted from (to check the model's work). */
export interface PacketSources {
  packet: Pick<Packet, 'id' | 'type' | 'title' | 'content' | 'entities' | 'source_timestamp' | 'deprecated'>
  sources: (SourceRef & { text: string | null; reply_to?: string | null; edited?: boolean })[]
}

export type Stage = 'intake' | 'packetize' | 'dedup'
export type Action =
  | 'FETCH'
  | 'PUSH'
  | 'PACKETIZE'
  | 'ADD'
  | 'MERGE'
  | 'SKIP'
  | 'SUPERSEDE'
  | 'SUMMARIZE'
  | 'ARCHIVE'

export interface Neighbour {
  packet_id: string
  title: string
  cosine: number
}

export interface MemoryLogEntry {
  id: string
  at: string
  stage: Stage
  action: Action
  title: string
  reason?: string | null
  packet_ids: string[]
  source?: SourceRef
  detail?: {
    neighbours?: Neighbour[]
    target_id?: string
    content_before?: string
    content_after?: string
    produced?: string[]
    items?: number
    page?: string | null
    /** Intake only: how many of the items were new or changed (the rest were duplicates). */
    new?: number | null
    /** Intake only: the first few items, for a quick look without opening Sources. */
    preview?: { author?: string | null; channel?: string | null; timestamp: string; text: string }[]
  }
}

export interface PipelineProgress {
  state: 'idle' | 'running' | 'done' | 'failed'
  done: number
  total: number
  error: string | null
  trigger: 'manual' | 'live' | null
}

export interface PipelineStats {
  intake: number
  streams: number
  /** Raw items that were packetized (live), or packets produced (demo). */
  packetized: number
  /** Windows with nothing durable (live), or items (demo). */
  nothing_durable: number
  merged: number
  skipped_known: number
  packets: number
  types: number
  /** Live only. */
  pending?: number
  errors?: number
  progress?: PipelineProgress
}

export type Capability = 'fetch' | 'push'

export interface StreamInfo {
  name: string
  supports: Capability[]
  items: number
  /** Known totals (mock / sources that report them). Live connectors don't, so `state` is used instead. */
  backfill: { done: number; total: number } | null
  kind?: string
  description?: string
  state?: 'idle' | 'running' | 'done' | 'failed'
  last_fetch?: { purpose: string; since: string; until: string; fetched?: number } | null
  /** Items per channel/thread inside the stream (e.g. Slack channels in the `channels` stream). */
  channels?: { name: string; items: number }[]
}

/** An IngestItem as stored by the hub (PROTOCOL.md §5). */
export interface RawItem {
  connector: string
  stream: string
  item: {
    source_id: string
    timestamp: string
    content: string
    author?: { name: string; id?: string }
    thread_id?: string
    reply_to?: string
    url?: string
    updated_at?: string
    metadata?: Record<string, unknown>
  }
}

export type DataMode = 'live' | 'mock'

export interface ConnectorInfo {
  name: string
  label: string
  version: string
  status: 'live' | 'idle' | 'disconnected'
  detail: string
  connected_since?: string
  streams: StreamInfo[]
}

export interface WireFrame {
  id: string
  at: string
  connector?: string
  dir: 'in' | 'out'
  method: string
  summary: string
  payload: unknown
}

export interface Citation {
  n: number
  kind: 'packet' | 'raw'
  title: string
  type?: PacketType
  where: string
  when: string
  packet_id?: string
}

export interface ToolStep {
  tool: 'search_knowledge' | 'fetch_from_source'
  args: string
  result: string
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  steps?: ToolStep[]
  citations?: Citation[]
  superseded?: { title: string; note: string }[]
  streaming?: boolean
}

export interface Thread {
  id: string
  title: string
  updated: string
}

export type ChatEvent =
  | { type: 'step'; step: ToolStep }
  | { type: 'token'; text: string }
  | { type: 'answer'; text: string }
  | { type: 'citations'; citations: Citation[] }
  | { type: 'superseded'; items: { title: string; note: string }[] }
  | { type: 'thread'; thread_id: string }
  | { type: 'error'; message: string }
  | { type: 'done' }

export interface TypeDef {
  name: PacketType
  definition: string
  enabled: boolean
}

export interface Lens {
  id: string
  name: string
  intent: string
}

export interface Settings {
  lens: string
  types: TypeDef[]
  theta: number
  buffer: { messages: number; seconds: number }
}

// ---------- models & keys (Tune → Models; backend: app/mini_cortex/llm_routes.py) ----------

export interface LlmPreset {
  id: string
  label: string
  base_url: string
  needs_key: boolean
  key_env: string
  key_help: string
  chat_model: string
  extract_model: string
  embed_model: string
  embed_dim: number
}

/** What the server reports. The key itself never comes back: only whether one is stored and its last 4 chars. */
export interface LlmSettings {
  provider: string
  base_url: string
  project_id: string
  key_set: boolean
  key_hint: string
  chat_model: string
  extract_model: string
  embed_model: string
  embed_dim: number
  ca_bundle: string
  missing: string[]
  env_file: string
  overridden: string[]
  presets: LlmPreset[]
}

/** What the UI sends. `api_key: null` keeps the stored key. */
export interface LlmDraft {
  provider: string
  base_url: string
  project_id: string
  api_key: string | null
  chat_model: string
  extract_model: string
  embed_model: string
  embed_dim: number
  ca_bundle: string
}

export interface LlmModel {
  id: string
  kind: 'chat' | 'embed'
}

export interface DoctorCheck {
  name: string
  status: 'ok' | 'warn' | 'fail' | 'skip'
  detail: string
  fix: string
  ms: number | null
}
