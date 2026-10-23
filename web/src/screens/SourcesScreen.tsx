import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { api } from '../api/client'
import type { ConnectorInfo, DataMode, RawItem, StreamInfo, WireFrame } from '../api/types'
import { fmtDayTime, fmtTime } from '../fmt'

type Pane = 'wire' | 'items'
type Selection = { kind: 'frame'; id: string } | { kind: 'item'; id: string } | null

export function SourcesScreen({
  connectors,
  hub,
  mode,
  notify,
}: {
  connectors: ConnectorInfo[]
  hub: string
  mode: DataMode
  notify: (msg: string) => void
}) {
  const [frames, setFrames] = useState<WireFrame[]>([])
  const [items, setItems] = useState<RawItem[]>([])
  const [pane, setPane] = useState<Pane>('wire')
  const [sel, setSel] = useState<Selection>(null)
  const [form, setForm] = useState(false)

  useEffect(() => {
    api.frames().then((f) => {
      setFrames(f)
      const push = [...f].reverse().find((x) => x.method === 'push')
      if (push) setSel({ kind: 'frame', id: push.id })
    })
    api.items().then(setItems)
    return api.subscribe({ frame: (f) => setFrames((fs) => [...fs.slice(-499), f]) })
  }, [])

  // Items are cheap to re-read; refresh whenever a connector's counts change.
  const total = connectors.reduce((n, c) => n + c.streams.reduce((m, s) => m + s.items, 0), 0)
  useEffect(() => {
    if (mode === 'live') api.items().then(setItems)
  }, [mode, total])

  const newestFirst = useMemo(() => [...frames].reverse(), [frames])
  const selectedFrame = sel?.kind === 'frame' ? frames.find((f) => f.id === sel.id) : undefined
  const selectedItem = sel?.kind === 'item' ? items.find((i) => i.item.source_id === sel.id) : undefined
  const fetchable = connectors.flatMap((c) =>
    c.status === 'live'
      ? c.streams.filter((s) => s.supports.includes('fetch')).map((s) => ({ connector: c.name, stream: s.name }))
      : [],
  )

  return (
    <div className="cols src">
      <div className="col">
        <div className="col-head">
          <h4>Connectors</h4>
          <div className="spacer" />
          <button
            type="button"
            className="btn"
            aria-expanded={form}
            disabled={fetchable.length === 0}
            onClick={() => setForm((v) => !v)}
          >
            Backfill a period…
          </button>
        </div>
        <div className="conns scroll">
          {form && (
            <BackfillForm
              targets={fetchable}
              onCancel={() => setForm(false)}
              onSubmit={async (target, since, until) => {
                try {
                  const r = await api.fetchWindow({ ...target, since, until })
                  if (r.frame) setFrames((fs) => [...fs, r.frame!])
                  setForm(false)
                  notify(
                    r.items !== undefined
                      ? `${target.connector} returned ${r.items} ${r.items === 1 ? 'item' : 'items'}`
                      : `fetch sent to ${target.connector}`,
                  )
                } catch (e) {
                  notify(`Fetch failed: ${(e as Error).message}`)
                }
              }}
            />
          )}
          {connectors.length === 0 && (
            <div className="conn-empty">
              <h3>No sources wired yet</h3>
              <p>
                Wire your Slack (workshop step 1): set <code>MINI_CORTEX_CONNECTORS=slack</code> and your tokens in
                <code>.env</code>, then restart the app. No Slack? The offline story:
                <code>MINI_CORTEX_CONNECTORS=replay</code>
              </p>
            </div>
          )}
          {connectors.map((c) => (
            <div key={c.name} className={`conn${c.status !== 'live' ? ' idle' : ''}`}>
              <header>
                <span className={`dot${c.status === 'live' ? ' live pulse' : ''}`} />
                <h5>{c.label}</h5>
                <span className="sub">
                  {c.status === 'disconnected' ? 'disconnected · ' : ''}
                  {c.detail}
                  {c.connected_since && ` · connected ${fmtTime(c.connected_since).slice(0, 5)}`}
                  {c.version && ` · v${c.version}`}
                </span>
              </header>
              {c.streams.length > 0 && (
                <div className="streams">
                  {c.streams.map((s) => (
                    <StreamRow key={s.name} s={s} />
                  ))}
                </div>
              )}
            </div>
          ))}
          <div className="wait">
            <span className="dot" />
            Listening for connectors on <code>{hub}</code>
          </div>
        </div>
      </div>

      <div className="col wire-col">
        <div className="col-head">
          <div className="seg" role="group" aria-label="Right pane">
            <button type="button" aria-pressed={pane === 'wire'} onClick={() => setPane('wire')}>
              Wire <span className="count">{frames.length}</span>
            </button>
            <button type="button" aria-pressed={pane === 'items'} onClick={() => setPane('items')}>
              Items <span className="count">{mode === 'live' ? total : '—'}</span>
            </button>
          </div>
          <div className="spacer" />
          <span className="chip">JSON-RPC 2.0</span>
        </div>
        <div className="wire scroll">
          {pane === 'wire' &&
            newestFirst.map((f) => (
              <button
                type="button"
                key={f.id}
                className={`f${sel?.kind === 'frame' && sel.id === f.id ? ' sel' : ''}`}
                onClick={() => setSel({ kind: 'frame', id: f.id })}
              >
                <time>{fmtTime(f.at)}</time>
                <span className={`dir${f.dir === 'in' ? ' in' : ''}`} aria-label={f.dir === 'in' ? 'from connector' : 'to connector'}>
                  {f.dir === 'in' ? '←' : '→'}
                </span>
                <span className={`m${f.method === 'error' ? ' err' : ''}`}>{f.method}</span>
                <span className="s">
                  {f.connector && f.connector !== '?' && connectors.length > 1 ? `${f.connector} · ` : ''}
                  {f.summary}
                </span>
              </button>
            ))}
          {pane === 'wire' && frames.length === 0 && <div className="empty small">No frames yet.</div>}
          {pane === 'items' &&
            items.map((r) => (
              <button
                type="button"
                key={r.item.source_id}
                className={`it${sel?.kind === 'item' && sel.id === r.item.source_id ? ' sel' : ''}`}
                onClick={() => setSel({ kind: 'item', id: r.item.source_id })}
              >
                <span className="it-meta">
                  <b>{r.item.author?.name ?? 'unknown'}</b>
                  <span>{r.item.thread_id ?? r.stream}</span>
                  <time>{fmtDayTime(r.item.timestamp)}</time>
                </span>
                <span className="it-text">{r.item.content}</span>
              </button>
            ))}
          {pane === 'items' && items.length === 0 && (
            <div className="empty small">
              {mode === 'live' ? 'No items yet. Run a backfill or post in a channel.' : 'Items are shown in live mode only.'}
            </div>
          )}
        </div>
        <div className="json-pane">
          {selectedFrame && <Json value={selectedFrame.payload} />}
          {selectedItem && <Json value={selectedItem.item} />}
          {!selectedFrame && !selectedItem && <div className="empty small">Select a frame or item to see its JSON.</div>}
        </div>
      </div>
    </div>
  )
}

function StreamRow({ s }: { s: StreamInfo }) {
  const [open, setOpen] = useState(false)
  let pct = 0
  let label: ReactNode = s.items
  let state = s.state ?? 'idle'
  if (s.backfill) {
    pct = Math.round((s.backfill.done / s.backfill.total) * 100)
    state = s.backfill.done >= s.backfill.total ? 'done' : 'running'
    if (state === 'running') label = `${s.backfill.done} / ${s.backfill.total}`
  } else if (state === 'done') pct = 100
  const channels = s.channels ?? []
  return (
    <>
      <div>
        <span className="sname">
          {channels.length > 0 ? (
            <button type="button" className="disclose" aria-expanded={open} onClick={() => setOpen((v) => !v)}>
              {open ? '▾' : '▸'} {s.name}
            </button>
          ) : (
            s.name
          )}
        </span>
        <span className="sup">{s.supports.join(' · ')}</span>
        <span
          className={`bar ${state}`}
          role="progressbar"
          aria-label={`${s.name}: ${state}`}
          aria-valuenow={state === 'running' && !s.backfill ? undefined : pct}
          aria-valuemin={0}
          aria-valuemax={100}
          title={
            s.last_fetch
              ? `${s.last_fetch.purpose} ${s.last_fetch.since.slice(0, 10)} → ${s.last_fetch.until.slice(0, 10)}: ${state}`
              : state === 'idle'
                ? 'Nothing fetched yet'
                : state
          }
        >
          <i style={{ width: state === 'running' && !s.backfill ? undefined : `${pct}%` }} />
        </span>
        <span className="n">{label}</span>
      </div>
      {open &&
        channels.map((ch) => (
          <div key={ch.name} className="sub-row">
            <span>#{ch.name}</span>
            <span />
            <span />
            <span className="n">{ch.items}</span>
          </div>
        ))}
    </>
  )
}

function BackfillForm({
  targets,
  onSubmit,
  onCancel,
}: {
  targets: { connector: string; stream: string }[]
  onSubmit: (target: { connector: string; stream: string }, since: string, until: string) => Promise<void>
  onCancel: () => void
}) {
  const today = new Date().toISOString().slice(0, 10)
  const weekAgo = new Date(Date.now() - 7 * 86400_000).toISOString().slice(0, 10)
  const [target, setTarget] = useState(0)
  const [since, setSince] = useState(weekAgo)
  const [until, setUntil] = useState(today)
  const [busy, setBusy] = useState(false)
  // `until` is exclusive in the protocol, so the chosen end day is included by fetching up to the next midnight.
  const untilIso = new Date(Date.parse(`${until}T00:00:00Z`) + 86400_000).toISOString()
  return (
    <form
      className="backfill"
      onSubmit={async (e) => {
        e.preventDefault()
        setBusy(true)
        await onSubmit(targets[target], `${since}T00:00:00Z`, untilIso)
        setBusy(false)
      }}
    >
      <label>
        <span>Stream</span>
        <select id="bf-stream" className="select" value={target} onChange={(e) => setTarget(Number(e.target.value))}>
          {targets.map((t, i) => (
            <option key={`${t.connector}/${t.stream}`} value={i}>
              {t.connector} · {t.stream}
            </option>
          ))}
        </select>
      </label>
      <label>
        <span>From</span>
        <input id="bf-since" type="date" value={since} onChange={(e) => setSince(e.target.value)} />
      </label>
      <label>
        <span>To (inclusive)</span>
        <input id="bf-until" type="date" value={until} onChange={(e) => setUntil(e.target.value)} />
      </label>
      <div className="actions">
        <button type="button" className="btn" onClick={onCancel}>
          Cancel
        </button>
        <button type="submit" className="btn primary" disabled={busy || targets.length === 0 || since > until}>
          {busy ? 'Fetching…' : 'Send fetch'}
        </button>
      </div>
    </form>
  )
}

/** Small JSON pretty-printer with token colouring; no dependency needed. */
function Json({ value }: { value: unknown }) {
  const text = JSON.stringify(value, null, 2)
  const parts: ReactNode[] = []
  const re = /("(?:\\.|[^"\\])*")(\s*:)?|\b(-?\d+(?:\.\d+)?|true|false|null)\b/g
  let last = 0
  let m: RegExpExecArray | null
  let i = 0
  while ((m = re.exec(text))) {
    parts.push(text.slice(last, m.index))
    if (m[1] && m[2]) parts.push(<span key={i++} className="k">{m[1]}</span>, m[2])
    else if (m[1]) parts.push(<span key={i++} className="str">{m[1]}</span>)
    else parts.push(<span key={i++} className="num">{m[3]}</span>)
    last = re.lastIndex
  }
  parts.push(text.slice(last))
  return <pre className="json">{parts}</pre>
}
