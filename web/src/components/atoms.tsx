import type { CSSProperties, ReactNode } from 'react'
import type { Action, PacketType } from '../api/types'

const verdictColor: Record<Action, string> = {
  ADD: 'var(--ok)',
  MERGE: 'var(--accent)',
  SKIP: 'var(--muted)',
  SUPERSEDE: 'var(--warn)',
  SUMMARIZE: 'var(--t-person)',
  ARCHIVE: 'var(--faint)',
  PACKETIZE: 'var(--t-event)',
  FETCH: 'var(--faint)',
  PUSH: 'var(--faint)',
}

export function Verdict({ action }: { action: Action }) {
  return (
    <span className="v" style={{ '--c': verdictColor[action] } as CSSProperties}>
      {action}
    </span>
  )
}

export function TypeChip({ type }: { type: PacketType | 'raw' }) {
  const c = type === 'raw' ? 'var(--faint)' : `var(--t-${type})`
  return (
    <span className="t" style={{ '--c': c } as CSSProperties}>
      {type}
    </span>
  )
}

/** Renders **bold** and [n] citation markers inside model text. */
export function RichText({
  text,
  rawCites = [],
  onCite,
}: {
  text: string
  rawCites?: number[]
  onCite?: (n: number) => void
}) {
  // Citations attach to the preceding word: "lighthouse [1]." renders as "lighthouse¹."
  const paragraphs = text.replace(/\s+(\[\d+\])/g, '$1').split(/\n{2,}/)
  return (
    <>
      {paragraphs.map((para, i) => (
        <p key={i}>
          {para.split(/(\*\*[^*]+\*\*|\[\d+\])/).map((part, j): ReactNode => {
            const bold = part.match(/^\*\*(.+)\*\*$/)
            if (bold) return <b key={j}>{bold[1]}</b>
            const cite = part.match(/^\[(\d+)\]$/)
            if (cite) {
              const n = Number(cite[1])
              return (
                <button
                  key={j}
                  type="button"
                  className={`cite${rawCites.includes(n) ? ' raw' : ''}`}
                  onClick={() => onCite?.(n)}
                  aria-label={`Source ${n}`}
                >
                  {n}
                </button>
              )
            }
            return part
          })}
        </p>
      ))}
    </>
  )
}
