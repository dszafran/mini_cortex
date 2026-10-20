import { useEffect, useState } from 'react'
import { api } from '../api/client'
import type { PacketSources } from '../api/types'
import { fmtDayTime } from '../fmt'

/**
 * The original messages a packet was extracted from, with a link back to the source (e.g. Slack). This is how a
 * person checks the model's work: compare the packet with what was actually said.
 */
export function SourceMessages({ packetId, showPacket = false }: { packetId: string; showPacket?: boolean }) {
  const [data, setData] = useState<PacketSources | null | undefined>(undefined)

  useEffect(() => {
    let alive = true
    api
      .packetSources(packetId)
      .then((d) => alive && setData(d))
      .catch(() => alive && setData(null))
    return () => {
      alive = false
    }
  }, [packetId])

  if (data === undefined) return <p className="muted small">Loading the original messages…</p>
  if (data === null) return <p className="muted small">This packet is no longer in memory.</p>

  return (
    <div className="block">
      {showPacket && <p className="merged">{data.packet.content}</p>}
      <small>
        Original {data.sources.length === 1 ? 'message' : 'messages'} · {data.sources.length}
      </small>
      <div className="raw-msgs">
        {data.sources.map((s) => (
          <div key={s.source_id} className="raw-msg">
            <div className="raw-head">
              <b>{s.author ?? 'unknown'}</b>
              <span>
                {s.label ?? s.stream} · {fmtDayTime(s.timestamp)}
                {s.reply_to ? ' · reply in thread' : ''}
                {s.edited ? ' · edited' : ''}
              </span>
              <div className="spacer" />
              {s.url && (
                <a href={s.url} target="_blank" rel="noreferrer" title="Open the original message">
                  Open in {s.connector} ↗
                </a>
              )}
            </div>
            {s.text != null ? (
              <Clamped text={s.text} />
            ) : (
              <p className="muted small">Demo packet: no original text.</p>
            )}
            <code className="faint">{s.source_id}</code>
          </div>
        ))}
      </div>
    </div>
  )
}

function Clamped({ text }: { text: string }) {
  const [open, setOpen] = useState(false)
  const long = text.length > 420
  return (
    <>
      <p className={`raw-text${long && !open ? ' clamp' : ''}`}>{text}</p>
      {long && (
        <button type="button" className="link small" onClick={() => setOpen(!open)}>
          {open ? 'Show less' : 'Show all'}
        </button>
      )}
    </>
  )
}
