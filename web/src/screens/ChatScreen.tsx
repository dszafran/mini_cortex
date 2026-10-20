import { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api/client'
import type { ChatMessage, Thread } from '../api/types'
import { RichText, TypeChip } from '../components/atoms'
import { SourceMessages } from '../components/SourceMessages'
import { FetchIcon, SearchIcon, SendIcon } from '../components/icons'

let seq = 0
const uid = (p: string) => `${p}-${Date.now()}-${seq++}`

export function ChatScreen() {
  const [threads, setThreads] = useState<Thread[]>([])
  const [active, setActive] = useState<string>('t1')
  const [byThread, setByThread] = useState<Record<string, ChatMessage[]>>({})
  const [focus, setFocus] = useState<{ msg: string; n: number | null } | null>(null)
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const scroller = useRef<HTMLDivElement>(null)

  useEffect(() => {
    api.threads().then((ts) => {
      setThreads(ts)
      // Live: open the latest thread, or start a fresh one when there is none yet.
      if (ts.length && !ts.some((t) => t.id === 't1')) setActive(ts[0].id)
      else if (!ts.length) newThread()
    })
  }, [])

  useEffect(() => {
    if (byThread[active]) return
    api.messages(active).then((m) => setByThread((s) => ({ ...s, [active]: m })))
  }, [active, byThread])

  const messages = useMemo(() => byThread[active] ?? [], [byThread, active])

  useEffect(() => {
    scroller.current?.scrollTo({ top: scroller.current.scrollHeight })
  }, [messages])

  // The Cited panel follows the focused answer, else the latest one.
  const citedMsg = useMemo(() => {
    const answers = messages.filter((m) => m.role === 'assistant')
    return answers.find((m) => m.id === focus?.msg) ?? answers[answers.length - 1]
  }, [messages, focus])

  const patch = (thread: string, id: string, fn: (m: ChatMessage) => ChatMessage) =>
    setByThread((s) => ({ ...s, [thread]: (s[thread] ?? []).map((m) => (m.id === id ? fn(m) : m)) }))

  async function send() {
    const q = input.trim()
    if (!q || busy) return
    const thread = active
    const answerId = uid('a')
    setInput('')
    setBusy(true)
    setFocus(null)
    setByThread((s) => ({
      ...s,
      [thread]: [
        ...(s[thread] ?? []),
        { id: uid('q'), role: 'user', content: q },
        { id: answerId, role: 'assistant', content: '', steps: [], citations: [], streaming: true },
      ],
    }))
    setThreads((ts) =>
      ts.map((t) => (t.id === thread ? { ...t, title: t.title === 'New chat' ? q : t.title, updated: 'just now' } : t)),
    )
    try {
      for await (const ev of api.ask(q, thread)) {
        if (ev.type === 'step') patch(thread, answerId, (m) => ({ ...m, steps: [...(m.steps ?? []), ev.step] }))
        else if (ev.type === 'token') patch(thread, answerId, (m) => ({ ...m, content: m.content + ev.text }))
        else if (ev.type === 'citations') patch(thread, answerId, (m) => ({ ...m, citations: ev.citations }))
        else if (ev.type === 'superseded') patch(thread, answerId, (m) => ({ ...m, superseded: ev.items }))
        else if (ev.type === 'error')
          patch(thread, answerId, (m) => ({ ...m, content: `${m.content}\n\n**Error:** ${ev.message}` }))
        else if (ev.type === 'done') patch(thread, answerId, (m) => ({ ...m, streaming: false }))
      }
    } finally {
      patch(thread, answerId, (m) => ({ ...m, streaming: false }))
      setBusy(false)
    }
  }

  function newThread() {
    const id = uid('t')
    setThreads((ts) => [{ id, title: 'New chat', updated: 'just now' }, ...ts])
    setByThread((s) => ({ ...s, [id]: [] }))
    setActive(id)
    setFocus(null)
  }

  return (
    <div className="cols chat">
      <div className="col threads-col">
        <div className="col-head">
          <h4>Threads</h4>
          <div className="spacer" />
          <button type="button" className="chip" onClick={newThread}>
            + New
          </button>
        </div>
        <div className="threads scroll">
          {threads.map((t) => (
            <button
              type="button"
              key={t.id}
              className={t.id === active ? 'on' : undefined}
              onClick={() => {
                setActive(t.id)
                setFocus(null)
              }}
            >
              <span>{t.title}</span>
              <small>{t.updated}</small>
            </button>
          ))}
        </div>
      </div>

      <div className="col">
        <div className="convo scroll" ref={scroller}>
          {messages.length === 0 && (
            <div className="empty">
              <h3>Ask your memory</h3>
              <p>Try "Why was night mode cancelled?" or "What do I owe Alpenvox, and by when?"</p>
            </div>
          )}
          {messages.map((m) =>
            m.role === 'user' ? (
              <div key={m.id} className="q">
                {m.content}
              </div>
            ) : (
              <div key={m.id} className="a">
                {!!m.steps?.length && (
                  <div className="trace">
                    {m.steps.map((s, i) => (
                      <div key={i} className="step">
                        {s.tool === 'search_knowledge' ? <SearchIcon /> : <FetchIcon />}
                        <span className="step-line" title={`${s.tool} ${s.args} · ${s.result}`}>
                          {s.tool} <em>{s.args}</em> · {s.result}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
                {m.streaming && !m.content && !m.steps?.length && <div className="thinking">Thinking…</div>}
                {m.content && (
                  <div className="answer">
                    <RichText
                      text={m.content}
                      rawCites={m.citations?.filter((c) => c.kind === 'raw').map((c) => c.n)}
                      onCite={(n) => setFocus({ msg: m.id, n })}
                    />
                    {m.streaming && <span className="caret" />}
                  </div>
                )}
                {m.superseded?.map((s, i) => (
                  <div key={i} className="note">
                    <span className="v" style={{ ['--c' as string]: 'var(--warn)' }}>
                      SUPERSEDED
                    </span>
                    <span>
                      Ignored the older plan: <s>{s.title}</s>, {s.note}.
                    </span>
                  </div>
                ))}
              </div>
            ),
          )}
        </div>
        <form
          className="composer"
          onSubmit={(e) => {
            e.preventDefault()
            send()
          }}
        >
          <textarea
            id="chat-input"
            rows={1}
            value={input}
            placeholder="Ask about your memory…"
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault()
                send()
              }
            }}
          />
          <div className="row">
            <span>Searches memory</span>
            <div className="spacer" />
            <button type="submit" className="btn primary" disabled={busy || !input.trim()}>
              <SendIcon /> Send
            </button>
          </div>
        </form>
      </div>

      <div className="col">
        <div className="col-head">
          <h4>Cited</h4>
          <div className="spacer" />
          <span className="hint-raw">click to see the original messages</span>
        </div>
        <div className="cited scroll">
          {!citedMsg?.citations?.length ? (
            <div className="empty">
              <p>Sources for the answer appear here.</p>
            </div>
          ) : (
            <ul>
              {citedMsg.citations.map((c) => {
                const open = focus?.msg === citedMsg.id && focus.n === c.n
                return (
                  <li key={c.n} className={open ? 'hl' : undefined}>
                    <button
                      type="button"
                      className="cite-head"
                      aria-expanded={open}
                      onClick={() => setFocus(open ? null : { msg: citedMsg.id, n: c.n })}
                      title={open ? 'Hide the original messages' : 'Show the packet and its original messages'}
                    >
                      <span className={`cite${c.kind === 'raw' ? ' raw' : ''}`}>{c.n}</span>
                      <b>{c.title}</b>
                      <span className="meta">
                        <TypeChip type={c.kind === 'raw' ? 'raw' : c.type!} />
                        <span>
                          {c.where} · {c.when}
                        </span>
                      </span>
                    </button>
                    {open && c.packet_id && <SourceMessages packetId={c.packet_id} showPacket />}
                  </li>
                )
              })}
            </ul>
          )}
        </div>
      </div>
    </div>
  )
}
