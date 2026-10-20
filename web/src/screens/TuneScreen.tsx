import { useState } from 'react'
import type { Lens, LlmSettings, Settings } from '../api/types'
import { TypeChip } from '../components/atoms'
import { ModelsSection } from './ModelsSection'

const THETA_MIN = 0.7
const THETA_MAX = 0.95

export function TuneScreen({
  settings,
  lenses,
  onApply,
  llm,
  onLlmSaved,
}: {
  settings: Settings
  lenses: Lens[]
  onApply: (s: Settings) => Promise<void>
  llm: LlmSettings | null
  onLlmSaved: (s: LlmSettings) => void
}) {
  const [draft, setDraft] = useState<Settings>(settings)
  const [saving, setSaving] = useState(false)
  const dirty = JSON.stringify(draft) !== JSON.stringify(settings)
  const pct = ((draft.theta - THETA_MIN) / (THETA_MAX - THETA_MIN)) * 100

  return (
    <div className="tune">
      {llm && <ModelsSection llm={llm} onSaved={onLlmSaved} />}

      <section className="wide">
        <h3>Lens</h3>
        <p>Who is this memory for? The same data produces different packets under each lens.</p>
        <div className="lenses" role="radiogroup" aria-label="Lens">
          {lenses.map((l) => (
            <button
              type="button"
              role="radio"
              aria-checked={draft.lens === l.id}
              key={l.id}
              className={`lens${draft.lens === l.id ? ' on' : ''}`}
              onClick={() => setDraft({ ...draft, lens: l.id })}
            >
              <b>{l.name}</b>
              <span>{l.intent}</span>
            </button>
          ))}
        </div>
      </section>

      <section>
        <h3>Packet types</h3>
        <p>
          From <code>memory_types.yaml</code>. Switch types off, then re-ingest and compare.
        </p>
        <div className="types">
          {draft.types.map((t, i) => (
            <label key={t.name} className={t.enabled ? undefined : 'off'}>
              <input
                id={`type-${t.name}`}
                type="checkbox"
                checked={t.enabled}
                onChange={() =>
                  setDraft({
                    ...draft,
                    types: draft.types.map((x, j) => (j === i ? { ...x, enabled: !x.enabled } : x)),
                  })
                }
              />
              <TypeChip type={t.name} />
              <span>{t.definition}</span>
            </label>
          ))}
        </div>
      </section>

      <section>
        <h3>Dedup threshold θ</h3>
        <p>Neighbours at or above θ go to the model for a verdict. Below θ, a packet is added without a check.</p>
        <div className="slider" style={{ ['--pct' as string]: `${pct}%` }}>
          <input
            id="theta"
            type="range"
            min={THETA_MIN}
            max={THETA_MAX}
            step={0.01}
            value={draft.theta}
            onChange={(e) => setDraft({ ...draft, theta: Number(e.target.value) })}
            aria-valuetext={`θ ${draft.theta.toFixed(2)}`}
          />
          <div className="scale">
            <span>{THETA_MIN.toFixed(2)}</span>
            <span className="cur">θ = {draft.theta.toFixed(2)}</span>
            <span>{THETA_MAX.toFixed(2)}</span>
          </div>
        </div>
        <p className="hint">
          {draft.theta > 0.89
            ? 'At this θ, CR-812 and CR-815 (cosine 0.89) skip the model and are both added. Lower θ sends more pairs to the model, which costs more calls and risks false merges.'
            : 'CR-812 and CR-815 score 0.89, so the model decides whether they merge. Apply, then open the CR-815 row on Memory to read its reasoning.'}
        </p>

        <h3 className="gap">Pipeline</h3>
        <div className="fields">
          <div>
            <span>Chat buffer</span>
            <b>
              flush at {draft.buffer.messages} messages or {draft.buffer.seconds} s
            </b>
          </div>
        </div>

        <div className="actions">
          <button
            type="button"
            className="btn primary"
            disabled={!dirty || saving}
            onClick={async () => {
              setSaving(true)
              await onApply(draft)
              setSaving(false)
            }}
          >
            {saving ? 'Applying…' : 'Apply'}
          </button>
          <button type="button" className="btn" disabled={!dirty || saving} onClick={() => setDraft(settings)}>
            Reset
          </button>
          {dirty && <span className="muted small">Unapplied changes</span>}
        </div>
      </section>
    </div>
  )
}
