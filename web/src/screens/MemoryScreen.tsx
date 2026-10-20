import { Fragment, useEffect, useMemo, useState } from 'react'
import { api } from '../api/client'
import type {
  Action,
  ConnectorInfo,
  DataMode,
  MemoryLogEntry,
  Neighbour,
  Packet,
  PacketType,
  PipelineStats,
  RawSource,
  SourceRef,
  Stage,
} from '../api/types'
import { TypeChip, Verdict } from '../components/atoms'
import { SourceMessages } from '../components/SourceMessages'
import { fmtDay, fmtDayTime, fmtTime, streamLabel } from '../fmt'

type View = 'activity' | 'packets' | 'dream'

export function MemoryScreen({
  theta,
  mode,
  connectors,
}: {
  theta: number
  mode: DataMode
  connectors: ConnectorInfo[]
}) {
  const [view, setView] = useState<View>('activity')
  const [stats, setStats] = useState<PipelineStats | null>(null)
  const [log, setLog] = useState<MemoryLogEntry[]>([])
  const [packets, setPackets] = useState<Packet[]>([])

  useEffect(() => {
    const loadPackets = () => api.packets().then(setPackets)
    let timer: ReturnType<typeof setTimeout> | undefined
    // Packets change in bursts during a run: refetch at most every 800 ms.
    const refreshPackets = () => {
      clearTimeout(timer)
      timer = setTimeout(loadPackets, 800)
    }
    const add = (e: MemoryLogEntry) => setLog((l) => [e, ...l].slice(0, 500))
    api.stats().then(setStats)
    api.memoryLog().then(setLog)
    loadPackets()
    const unsubscribe = api.subscribe({
      intake: add,
      memory: (e) => {
        add(e)
        if (e.stage === 'dedup') refreshPackets()
      },
      stats: setStats,
    })
    return () => {
      unsubscribe()
      clearTimeout(timer)
    }
  }, [])

  const byId = useMemo(() => new Map(packets.map((p) => [p.id, p])), [packets])
  const live = mode === 'live'
  const sources = connectors.filter((c) => c.status === 'live').length

  const rebuild = async () => {
    if (!confirm('Forget all packets and memory decisions, and mark every item as pending again?')) return
    setStats(await api.rebuild())
    setPackets([])
    setLog(await api.memoryLog())
  }

  return (
    <div className="cols mem">
      {live
        ? stats && <LiveStrip s={stats} sources={sources} onProcess={() => api.process()} onRebuild={rebuild} />
        : stats && <PipelineStrip s={stats} />}
      {view === 'activity' && (
        <Activity log={log} byId={byId} theta={theta} view={view} setView={setView} />
      )}
      {view === 'packets' && <Packets packets={packets} log={log} view={view} setView={setView} live={live} />}
      {view === 'dream' && (
        <div className="col mem-full">
          <div className="col-head">
            <ViewSwitch view={view} setView={setView} />
          </div>
          <div className="empty">
            <h3>Dream is a bonus step</h3>
            <p>
              Press Sleep to consolidate memory offline: merge late duplicates, mark superseded decisions and summarise
              clusters. Every change will be listed here as a diff.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}

function PipelineStrip({ s }: { s: PipelineStats }) {
  return (
    <div className="pipe">
      <div>
        <small>Intake</small>
        <strong>{s.intake}</strong>
        <span>
          raw items · <b>{s.streams} sources</b>
        </span>
      </div>
      <div>
        <small>Packetize</small>
        <strong>{s.packetized}</strong>
        <span>
          packets · <b>{s.nothing_durable} items</b> had nothing durable
        </span>
      </div>
      <div>
        <small>Dedup</small>
        <strong>{s.merged}</strong>
        <span>
          merged · <b>{s.skipped_known}</b> skipped as known
        </span>
      </div>
      <div>
        <small>Memory</small>
        <strong>{s.packets}</strong>
        <span>
          packets · <b>{s.types} types</b>
        </span>
      </div>
    </div>
  )
}

function LiveStrip({
  s,
  sources,
  onProcess,
  onRebuild,
}: {
  s: PipelineStats
  sources: number
  onProcess: () => void
  onRebuild: () => void
}) {
  const p = s.progress
  const running = p?.state === 'running'
  const pending = (s.pending ?? 0) + (s.errors ?? 0) // failed items are retried by Process
  return (
    <div className="pipe">
      <div>
        <small>Intake</small>
        <strong>{s.intake}</strong>
        <span>
          raw items · <b>{sources} live {sources === 1 ? 'source' : 'sources'}</b>
        </span>
        <div className="pipe-run">
          {running ? (
            <>
              <span className="bar" style={{ ['--pct' as string]: `${(100 * p.done) / Math.max(p.total, 1)}%` }} />
              <span>
                {p.trigger === 'live' ? 'Live flush' : 'Processing'} · {p.done}/{p.total} windows
              </span>
            </>
          ) : (
            <button type="button" className="btn primary" disabled={!pending} onClick={onProcess}>
              {pending ? `Process ${pending} pending` : 'All processed'}
            </button>
          )}
        </div>
        {p?.state === 'failed' && <span className="err">{p.error}</span>}
        {!!s.errors && !running && <span className="err">{s.errors} items failed last time; Process retries them</span>}
      </div>
      <div>
        <small>Packetize</small>
        <strong>{s.packetized}</strong>
        <span>
          items read · <b>{s.nothing_durable} windows</b> had nothing durable
        </span>
      </div>
      <div>
        <small>Dedup</small>
        <strong>{s.merged}</strong>
        <span>
          merged · <b>{s.skipped_known}</b> skipped as known
        </span>
      </div>
      <div>
        <small>Memory</small>
        <strong>{s.packets}</strong>
        <span>
          packets · <b>{s.types} types</b>
        </span>
        <div className="pipe-run">
          <button type="button" className="link small" disabled={running} onClick={onRebuild}>
            Rebuild memory
          </button>
        </div>
      </div>
    </div>
  )
}

function ViewSwitch({ view, setView }: { view: View; setView: (v: View) => void }) {
  const items: [View, string][] = [
    ['activity', 'Activity'],
    ['packets', 'Packets'],
    ['dream', 'Dream ☾'],
  ]
  return (
    <div className="seg" role="group" aria-label="Memory view">
      {items.map(([id, label]) => (
        <button type="button" key={id} aria-pressed={view === id} onClick={() => setView(id)}>
          {label}
        </button>
      ))}
    </div>
  )
}

const STAGES: (Stage | 'all')[] = ['all', 'intake', 'packetize', 'dedup']
const ACTIONS: (Action | 'all')[] = ['all', 'FETCH', 'PUSH', 'PACKETIZE', 'ADD', 'MERGE', 'SKIP']

function Activity({
  log,
  byId,
  theta,
  view,
  setView,
}: {
  log: MemoryLogEntry[]
  byId: Map<string, Packet>
  theta: number
  view: View
  setView: (v: View) => void
}) {
  const [stage, setStage] = useState<Stage | 'all'>('all')
  const [action, setAction] = useState<Action | 'all'>('all')
  const [sel, setSel] = useState<string | null>(null)

  const rows = log.filter((e) => (stage === 'all' || e.stage === stage) && (action === 'all' || e.action === action))
  const selected = log.find((e) => e.id === sel) ?? rows[0]

  return (
    <>
      <div className="col">
        <div className="col-head">
          <ViewSwitch view={view} setView={setView} />
          <div className="spacer" />
          <select id="stage-filter" className="select" value={stage} onChange={(e) => setStage(e.target.value as Stage)}>
            {STAGES.map((s) => (
              <option key={s} value={s}>
                {s === 'all' ? 'All stages' : s}
              </option>
            ))}
          </select>
          <select id="action-filter" className="select" value={action} onChange={(e) => setAction(e.target.value as Action)}>
            {ACTIONS.map((a) => (
              <option key={a} value={a}>
                {a === 'all' ? 'All verdicts' : a}
              </option>
            ))}
          </select>
        </div>
        <div className="log scroll">
          {rows.length === 0 && (
            <div className="empty">
              <p>
                {log.length === 0
                  ? 'Nothing has arrived yet. Connect a source or run a backfill on Sources.'
                  : 'No activity matches these filters.'}
              </p>
            </div>
          )}
          {rows.map((e) => (
            <button
              type="button"
              key={e.id}
              className={`r${selected?.id === e.id ? ' sel' : ''}`}
              onClick={() => setSel(e.id)}
            >
              <time>{fmtTime(e.at)}</time>
              <span>
                <Verdict action={e.action} />
              </span>
              <b>{e.title}</b>
              {e.reason && <p>{e.reason}</p>}
              {!e.reason && e.detail?.page && <p className="src">{e.detail.page}</p>}
            </button>
          ))}
        </div>
      </div>
      <div className="col">
        <div className="detail scroll">{selected && <EntryDetail e={selected} byId={byId} theta={theta} />}</div>
      </div>
    </>
  )
}

function EntryDetail({ e, byId, theta }: { e: MemoryLogEntry; byId: Map<string, Packet>; theta: number }) {
  const target = e.detail?.target_id ? byId.get(e.detail.target_id) : byId.get(e.packet_ids[0])
  const showPacket = ['ADD', 'MERGE', 'SKIP'].includes(e.action) && e.stage === 'dedup' && target

  return (
    <>
      <div className="kv">
        <Verdict action={e.action} />
        {showPacket && <TypeChip type={target.type} />}
        <span className="label">{e.stage}</span>
        <div className="spacer" />
        {showPacket && <span className="mono faint">{target.id}</span>}
      </div>
      <h3>{showPacket ? target.title : e.title}</h3>
      {showPacket && target.entities.length > 0 && (
        <div className="kv">
          {target.entities.map((x) => (
            <span key={x} className="ent">
              {x}
            </span>
          ))}
        </div>
      )}

      {e.detail?.neighbours && (
        <div className="block">
          <small>Nearest packets · same type</small>
          <Neighbours list={e.detail.neighbours} theta={theta} target={e.detail.target_id} />
        </div>
      )}

      {e.reason && (
        <div className="block">
          <small>{e.stage === 'intake' ? 'Item' : 'Model verdict'}</small>
          <p className="verdict">
            {e.action === 'MERGE' && e.detail?.target_id && <b>MERGE into {e.detail.target_id}. </b>}
            {e.reason}
          </p>
        </div>
      )}

      {e.detail?.content_before && e.detail.content_after?.startsWith(e.detail.content_before) ? (
        <div className="block">
          <small>Merged content</small>
          <p className="merged">
            {e.detail.content_before}
            <ins>{e.detail.content_after.slice(e.detail.content_before.length)}</ins>
          </p>
        </div>
      ) : (
        e.detail?.content_before && (
          <div className="block">
            <small>Before</small>
            <p className="merged before">{e.detail.content_before}</p>
            <small>After merge</small>
            <p className="merged">
              <ins>{e.detail.content_after}</ins>
            </p>
          </div>
        )
      )}

      {e.detail?.produced && (
        <div className="block">
          <small>Packets produced · {e.detail.produced.length}</small>
          <div className="plist">
            {e.detail.produced.map((id) => {
              const p = byId.get(id)
              return p ? (
                <div key={id}>
                  <TypeChip type={p.type} />
                  <span>{p.title}</span>
                </div>
              ) : null
            })}
          </div>
        </div>
      )}

      {e.detail?.items !== undefined && (
        <div className="block">
          <small>Intake</small>
          <p className="muted">
            {e.detail.items} {e.detail.items === 1 ? 'item' : 'items'} via {e.action.toLowerCase()}
            {e.detail.page ? ` · ${e.detail.page}` : ''}
            {e.detail.new != null ? ` · ${e.detail.new} new or changed, ${e.detail.items - e.detail.new} already known` : ''}
            . Items are stored once per source_id, then queued for packetizing.
          </p>
        </div>
      )}

      {e.detail?.preview && e.detail.preview.length > 0 && (
        <div className="block">
          <small>
            Items{e.detail.items && e.detail.items > e.detail.preview.length ? ` · first ${e.detail.preview.length}` : ''}
          </small>
          <div className="previews">
            {e.detail.preview.map((p, i) => (
              <div key={i}>
                <span className="it-meta">
                  <b>{p.author ?? 'unknown'}</b>
                  {p.channel && <span>#{p.channel}</span>}
                  <time>{fmtDayTime(p.timestamp)}</time>
                </span>
                <span className="it-text">{p.text}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {showPacket ? (
        <Sources list={target.sources} />
      ) : (
        e.source && !e.detail?.preview?.length && <Sources list={[e.source]} title="Source" />
      )}
    </>
  )
}

function Neighbours({ list, theta, target }: { list: Neighbour[]; theta: number; target?: string }) {
  const sorted = [...list].sort((a, b) => b.cosine - a.cosine)
  const above = sorted.filter((n) => n.cosine >= theta).length
  return (
    <>
      <div className="nb">
        {sorted.map((n, i) => (
          <Fragment key={n.packet_id}>
            {i === above && <div className="thr">── θ {theta.toFixed(2)} ──────────────────────</div>}
            <div className={n.cosine < theta ? 'below' : undefined}>
              <span className="cos">{n.cosine.toFixed(2)}</span>
              <span>{n.title}</span>
              <span className="pick">{n.packet_id === target ? 'target' : ''}</span>
            </div>
          </Fragment>
        ))}
        {above === sorted.length && <div className="thr">── θ {theta.toFixed(2)} ──────────────────────</div>}
      </div>
      <p className="muted small">
        {above === 0
          ? 'Nothing above θ, so the packet is added without asking the model.'
          : `${above} above θ ${above === 1 ? 'goes' : 'go'} to the model for a verdict.`}
      </p>
    </>
  )
}

/** Compact source lines; click one to read the original message, or jump to it in the source. */
function Sources({ list, title = 'Sources' }: { list: SourceRef[]; title?: string }) {
  const [open, setOpen] = useState<string | null>(null)
  return (
    <div className="block">
      <small>
        {title} · {list.length}
      </small>
      <div className="srcs">
        {list.map((s) => (
          <Fragment key={s.source_id}>
            <div className="src-row">
              <button
                type="button"
                className="src-toggle"
                aria-expanded={open === s.source_id}
                onClick={() => setOpen(open === s.source_id ? null : s.source_id)}
                title="Show the original message"
              >
                <span className="chev">{open === s.source_id ? '▾' : '▸'}</span>
                <span>
                  {s.connector} {s.label ? `· ${s.label}` : streamLabel(s.connector, s.stream)}
                </span>
                <code>
                  {fmtDayTime(s.timestamp)}
                  {s.author ? ` · ${s.author}` : ''}
                </code>
              </button>
              {s.url && (
                <a href={s.url} target="_blank" rel="noreferrer" title={`Open in ${s.connector}`}>
                  Open ↗
                </a>
              )}
            </div>
            {open === s.source_id && <RawPreview connector={s.connector} sourceId={s.source_id} />}
          </Fragment>
        ))}
      </div>
    </div>
  )
}

function RawPreview({ connector, sourceId }: { connector: string; sourceId: string }) {
  const [raw, setRaw] = useState<RawSource | null | undefined>(undefined)
  useEffect(() => {
    let alive = true
    api
      .rawItem(connector, sourceId)
      .then((r) => alive && setRaw(r))
      .catch(() => alive && setRaw(null))
    return () => {
      alive = false
    }
  }, [connector, sourceId])
  if (raw === undefined) return <p className="muted small">Loading…</p>
  if (!raw?.text) return <p className="muted small">No original text (demo data, or the item is gone).</p>
  return (
    <div className="raw-msg">
      <div className="raw-head">
        <b>{raw.author ?? 'unknown'}</b>
        <span>
          {fmtDayTime(raw.timestamp)}
          {raw.reply_to ? ' · reply in thread' : ''}
          {raw.edited ? ' · edited' : ''}
        </span>
        <div className="spacer" />
        {raw.url && (
          <a href={raw.url} target="_blank" rel="noreferrer">
            Open in {raw.connector} ↗
          </a>
        )}
      </div>
      <p className="raw-text">{raw.text}</p>
      <code className="faint">{raw.source_id}</code>
    </div>
  )
}

function Packets({
  packets,
  log,
  view,
  setView,
  live,
}: {
  packets: Packet[]
  log: MemoryLogEntry[]
  view: View
  setView: (v: View) => void
  live: boolean
}) {
  const [type, setType] = useState<PacketType | 'all'>('all')
  const [q, setQ] = useState('')
  const [sel, setSel] = useState<string | null>(null)

  const types = useMemo(() => {
    const counts = new Map<PacketType, number>()
    packets.forEach((p) => counts.set(p.type, (counts.get(p.type) ?? 0) + 1))
    return [...counts.entries()]
  }, [packets])

  const needle = q.trim().toLowerCase()
  const rows = packets.filter(
    (p) =>
      (type === 'all' || p.type === type) &&
      (!needle || `${p.title} ${p.content} ${p.entities.join(' ')}`.toLowerCase().includes(needle)),
  )
  const selected = packets.find((p) => p.id === sel) ?? rows[0]
  const history = selected ? log.filter((e) => e.packet_ids.includes(selected.id)) : []

  return (
    <>
      <div className="col">
        <div className="col-head">
          <ViewSwitch view={view} setView={setView} />
          <div className="spacer" />
          <input
            id="packet-search"
            className="search"
            type="search"
            placeholder="Filter packets…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
        <div className="typebar">
          <button type="button" aria-pressed={type === 'all'} onClick={() => setType('all')}>
            All <span>{packets.length}</span>
          </button>
          {types.map(([t, n]) => (
            <button type="button" key={t} aria-pressed={type === t} onClick={() => setType(t)}>
              <TypeChip type={t} /> <span>{n}</span>
            </button>
          ))}
        </div>
        <div className="plist-main scroll">
          {rows.map((p) => (
            <button
              type="button"
              key={p.id}
              className={`prow${selected?.id === p.id ? ' sel' : ''}${p.deprecated ? ' dep' : ''}`}
              onClick={() => setSel(p.id)}
            >
              <TypeChip type={p.type} />
              <b>{p.title}</b>
              <span className="meta">
                {p.sources.length > 1 && <span className="ent">{p.sources.length} sources</span>}
                {p.deprecated && <Verdict action="SUPERSEDE" />}
                {fmtDay(p.source_timestamp)}
              </span>
            </button>
          ))}
          {rows.length === 0 && (
            <div className="empty">
              <p>
                {packets.length === 0 && live
                  ? 'No packets yet. Press Process at the top to turn the pending items into packets.'
                  : 'No packets match.'}
              </p>
            </div>
          )}
        </div>
      </div>
      <div className="col">
        <div className="detail scroll">
          {selected && (
            <>
              <div className="kv">
                <TypeChip type={selected.type} />
                {selected.deprecated && <Verdict action="SUPERSEDE" />}
                <div className="spacer" />
                <span className="mono faint">{selected.id}</span>
              </div>
              <h3>{selected.title}</h3>
              {selected.entities.length > 0 && (
                <div className="kv">
                  {selected.entities.map((x) => (
                    <span key={x} className="ent">
                      {x}
                    </span>
                  ))}
                </div>
              )}
              <p className="merged">{selected.content}</p>
              {selected.superseded_by && (
                <p className="verdict">
                  Superseded by <b>{selected.superseded_by}</b>. Chat ignores this packet when answering.
                </p>
              )}
              {live ? <SourceMessages packetId={selected.id} /> : <Sources list={selected.sources} />}
              <div className="block">
                <small>Stats</small>
                <p className="muted small">
                  Event time {fmtDayTime(selected.source_timestamp)} · stored {fmtDayTime(selected.created_at)} · used in{' '}
                  {selected.access_count} {selected.access_count === 1 ? 'answer' : 'answers'}
                </p>
              </div>
              {history.length > 0 && (
                <div className="block">
                  <small>History</small>
                  <div className="plist">
                    {history.map((h) => (
                      <div key={h.id}>
                        <Verdict action={h.action} />
                        <span>{h.reason ?? h.title}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </>
  )
}
