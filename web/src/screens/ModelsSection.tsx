import { useEffect, useState } from 'react'
import { ApiError, api } from '../api/client'
import type { DoctorCheck, LlmDraft, LlmModel, LlmSettings } from '../api/types'

const toDraft = (s: LlmSettings): LlmDraft => ({
  provider: s.provider,
  base_url: s.provider === 'scaleway' ? '' : s.base_url,
  project_id: s.project_id,
  api_key: null,
  chat_model: s.chat_model,
  extract_model: s.extract_model,
  embed_model: s.embed_model,
  embed_dim: s.embed_dim,
  ca_bundle: s.ca_bundle,
})

const ROLES = [
  { key: 'chat_model', label: 'Chat', hint: 'Answers in chat. Needs tool calling.', kind: 'chat' },
  { key: 'extract_model', label: 'Extract', hint: 'Packetize and dedup verdicts. Needs structured output; high volume.', kind: 'chat' },
  { key: 'embed_model', label: 'Embeddings', hint: 'Vectors for search and dedup.', kind: 'embed' },
] as const

const MARK: Record<DoctorCheck['status'], string> = { ok: '✓', warn: '!', fail: '✕', skip: '–' }

/** Tune → Models: one OpenAI-compatible endpoint, a key, and a model per role. Saved to the local .env. */
export function ModelsSection({ llm, onSaved }: { llm: LlmSettings; onSaved: (s: LlmSettings) => void }) {
  const [draft, setDraft] = useState<LlmDraft>(() => toDraft(llm))
  const [models, setModels] = useState<LlmModel[] | null>(null)
  const [listError, setListError] = useState<ApiError | null>(null)
  const [busy, setBusy] = useState<'models' | 'test' | 'save' | null>(null)
  const [checks, setChecks] = useState<DoctorCheck[] | null>(null)
  const [showKey, setShowKey] = useState(false)

  const preset = llm.presets.find((p) => p.id === draft.provider) ?? llm.presets[0]
  const sameProvider = draft.provider === llm.provider
  // The saved key only follows the same provider and host (the server enforces this too).
  const hostOf = (u: string) => {
    try {
      return new URL(u).host
    } catch {
      return u
    }
  }
  const keyStored =
    sameProvider && llm.key_set && (draft.provider === 'scaleway' || hostOf(draft.base_url) === hostOf(llm.base_url))
  const canCall = !!(draft.api_key || keyStored || !preset.needs_key) && (draft.provider === 'scaleway' || !!draft.base_url)
  const dirty = JSON.stringify(draft) !== JSON.stringify(toDraft(llm))
  const connected = llm.missing.length === 0

  const loadModels = async (d: LlmDraft) => {
    setBusy('models')
    setListError(null)
    try {
      setModels(await api.llmModels(d))
    } catch (e) {
      setModels(null)
      setListError(e instanceof ApiError ? e : new ApiError(String(e)))
    } finally {
      setBusy(null)
    }
  }

  // Fill the dropdowns on open when a key is already stored.
  useEffect(() => {
    if (llm.key_set || !preset.needs_key) void loadModels(toDraft(llm))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const setProvider = (id: string) => {
    if (id === draft.provider) return
    const p = llm.presets.find((x) => x.id === id)!
    const back = id === llm.provider ? toDraft(llm) : null
    setDraft(
      back ?? {
        provider: id,
        base_url: p.base_url,
        project_id: '',
        api_key: null,
        chat_model: p.chat_model,
        extract_model: p.extract_model,
        embed_model: p.embed_model,
        embed_dim: p.embed_dim,
        ca_bundle: draft.ca_bundle,
      },
    )
    setModels(null)
    setListError(null)
    setChecks(null)
  }

  const test = async () => {
    setBusy('test')
    setChecks(null)
    try {
      setChecks((await api.testLlm(draft)).checks)
    } catch (e) {
      setChecks([{ name: 'test', status: 'fail', detail: String(e), fix: '', ms: null }])
    } finally {
      setBusy(null)
    }
  }

  const save = async () => {
    setBusy('save')
    try {
      const s = await api.saveLlm(draft)
      onSaved(s)
      setDraft(toDraft(s))
      setShowKey(false)
    } finally {
      setBusy(null)
    }
  }

  const status = dirty ? 'Unsaved changes' : connected ? 'Configured' : `Missing: ${llm.missing.join(', ')}`

  return (
    <section className="wide models">
      <div className="models-head">
        <h3>Models</h3>
        <span className={`pill ${dirty ? 'warn' : connected ? 'ok' : 'bad'}`}>{status}</span>
      </div>
      <p>
        One OpenAI-compatible endpoint serves every role. Settings are saved to <code>{llm.env_file}</code> on this
        machine; the key is sent only to the provider and is never shown again.
      </p>

      <div className="models-grid">
        <div className="block">
          <div className="seg" role="radiogroup" aria-label="Provider">
            {llm.presets.map((p) => (
              <button
                type="button"
                role="radio"
                key={p.id}
                aria-checked={draft.provider === p.id}
                className={draft.provider === p.id ? 'on' : undefined}
                onClick={() => setProvider(p.id)}
              >
                {p.label}
              </button>
            ))}
          </div>

          {draft.provider === 'scaleway' ? (
            <label className="field">
              <span>Project ID</span>
              <input
                className="mono"
                value={draft.project_id}
                placeholder="UUID from the Scaleway console · empty = default project"
                spellCheck={false}
                onChange={(e) => setDraft({ ...draft, project_id: e.target.value })}
              />
              {/[/:]/.test(draft.project_id) ? (
                <small className="warn-note">
                  That looks like a URL, not a Scaleway project ID. Paste the project ID (a UUID) from the Scaleway console.
                </small>
              ) : (
                <small>
                  Base URL:{' '}
                  <code>https://api.scaleway.ai/{draft.project_id.trim() ? `${draft.project_id.trim()}/` : ''}v1</code>
                </small>
              )}
            </label>
          ) : (
            <label className="field">
              <span>Base URL</span>
              <input
                className="mono"
                value={draft.base_url}
                placeholder="https://…/v1"
                spellCheck={false}
                onChange={(e) => setDraft({ ...draft, base_url: e.target.value })}
              />
            </label>
          )}

          <label className="field">
            <span>
              API key <code className="faint">{preset.key_env}</code>
            </span>
            <div className="key">
              <input
                className="mono"
                type={showKey ? 'text' : 'password'}
                autoComplete="off"
                spellCheck={false}
                value={draft.api_key ?? ''}
                placeholder={keyStored ? `${llm.key_hint} saved · leave empty to keep` : preset.needs_key ? 'paste your key' : 'not needed'}
                onChange={(e) => setDraft({ ...draft, api_key: e.target.value || null })}
              />
              <button type="button" className="btn" onClick={() => setShowKey(!showKey)} disabled={!draft.api_key}>
                {showKey ? 'Hide' : 'Show'}
              </button>
            </div>
            <small>{preset.key_help}</small>
          </label>

          <div className="actions">
            <button type="button" className="btn" disabled={!canCall || busy !== null} onClick={() => loadModels(draft)}>
              {busy === 'models' ? 'Loading…' : models ? `Reload models (${models.length})` : 'Load models'}
            </button>
          </div>
          {listError && (
            <p className="check-fix bad">
              {listError.message}
              {listError.fix && <span>{listError.fix}</span>}
            </p>
          )}
        </div>

        <div className="block">
          {ROLES.map((r) => (
            <label className="field" key={r.key}>
              <span>{r.label}</span>
              <ModelPicker
                value={draft[r.key]}
                models={models}
                prefer={r.kind}
                onChange={(v) => setDraft({ ...draft, [r.key]: v })}
              />
              <small>{r.hint}</small>
            </label>
          ))}
          <label className="field">
            <span>Embedding size</span>
            <input
              className="mono dim"
              type="number"
              min={0}
              step={1}
              value={draft.embed_dim || ''}
              placeholder="model default"
              onChange={(e) => setDraft({ ...draft, embed_dim: Number(e.target.value) || 0 })}
            />
            <small>
              Matryoshka models (qwen3-embedding-8b) can return fewer dimensions: 1024 is plenty. Changing it means
              re-embedding.
            </small>
          </label>
        </div>
      </div>

      {llm.overridden.length > 0 && (
        <p className="hint">
          Set in your shell environment, which wins over <code>.env</code>: <code>{llm.overridden.join(', ')}</code>.
          Unset them to manage these values here.
        </p>
      )}

      <div className="actions">
        <button type="button" className="btn" disabled={!canCall || busy !== null} onClick={test}>
          {busy === 'test' ? 'Testing…' : 'Test connection'}
        </button>
        <button type="button" className="btn primary" disabled={!dirty || busy !== null} onClick={save}>
          {busy === 'save' ? 'Saving…' : 'Save'}
        </button>
        <button
          type="button"
          className="btn"
          disabled={!dirty || busy !== null}
          onClick={() => {
            setDraft(toDraft(llm))
            setChecks(null)
          }}
        >
          Reset
        </button>
        <span className="muted small">
          Test uses the values above without saving. Same checks as <code>uv run doctor</code>.
        </span>
      </div>

      {checks && (
        <ul className="checks" aria-live="polite">
          {checks.map((c) => (
            <li key={c.name} className={c.status}>
              <b aria-label={c.status}>{MARK[c.status]}</b>
              <span>{c.name}</span>
              <code>{c.detail}</code>
              <i>{c.ms != null ? `${c.ms} ms` : ''}</i>
              {c.fix && c.status !== 'ok' && <p className="check-fix">{c.fix}</p>}
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}

/** A dropdown once the provider's list is loaded (preferred kind first); a text box until then. */
function ModelPicker({
  value,
  models,
  prefer,
  onChange,
}: {
  value: string
  models: LlmModel[] | null
  prefer: 'chat' | 'embed'
  onChange: (v: string) => void
}) {
  if (!models)
    return (
      <input
        className="mono"
        value={value}
        placeholder="model id"
        spellCheck={false}
        onChange={(e) => onChange(e.target.value)}
      />
    )
  const fits = models.filter((m) => m.kind === prefer)
  const rest = models.filter((m) => m.kind !== prefer)
  const listed = models.some((m) => m.id === value)
  return (
    <select className="mono" value={value} onChange={(e) => onChange(e.target.value)}>
      {!listed && <option value={value}>{value ? `${value} (not offered here)` : 'pick a model'}</option>}
      <optgroup label={prefer === 'embed' ? 'Embedding models' : 'Chat models'}>
        {fits.map((m) => (
          <option key={m.id}>{m.id}</option>
        ))}
      </optgroup>
      {rest.length > 0 && (
        <optgroup label="Other">
          {rest.map((m) => (
            <option key={m.id}>{m.id}</option>
          ))}
        </optgroup>
      )}
    </select>
  )
}
