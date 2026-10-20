import { useCallback, useEffect, useState, type ReactNode } from 'react'
import { api } from './api/client'
import type { ConnectorInfo, DataMode, Lens, LlmSettings, Settings } from './api/types'
import { ChatIcon, MemoryIcon, MoonIcon, SourcesIcon, SunIcon, TuneIcon } from './components/icons'
import { ChatScreen } from './screens/ChatScreen'
import { MemoryScreen } from './screens/MemoryScreen'
import { SourcesScreen } from './screens/SourcesScreen'
import { TuneScreen } from './screens/TuneScreen'

type Screen = 'chat' | 'memory' | 'sources' | 'tune'
const SCREENS: Screen[] = ['chat', 'memory', 'sources', 'tune']

const fromHash = (): Screen => {
  const h = location.hash.slice(1) as Screen
  return SCREENS.includes(h) ? h : 'chat'
}

type Theme = 'dark' | 'light'
const readTheme = (): Theme => (document.documentElement.dataset.theme === 'light' ? 'light' : 'dark')

export default function App() {
  const [screen, setScreen] = useState<Screen>(fromHash)
  const [theme, setTheme] = useState<Theme>(readTheme)
  const [settings, setSettings] = useState<Settings | null>(null)
  const [lenses, setLenses] = useState<Lens[]>([])
  const [connectors, setConnectors] = useState<ConnectorInfo[]>([])
  const [hub, setHub] = useState('')
  const [toast, setToast] = useState<string | null>(null)
  const [mode, setMode] = useState<DataMode | null>(null)
  const [llm, setLlm] = useState<LlmSettings | null>(null)

  useEffect(() => {
    api.mode().then(setMode)
    api.settings().then(setSettings)
    api.llm().then(setLlm)
    api.lenses().then(setLenses)
    api.connectors().then((r) => {
      setConnectors(r.connectors)
      setHub(r.hub)
    })
    const unsubscribe = api.subscribe({ connectors: setConnectors })
    const onHash = () => setScreen(fromHash())
    window.addEventListener('hashchange', onHash)
    return () => {
      unsubscribe()
      window.removeEventListener('hashchange', onHash)
    }
  }, [])

  useEffect(() => {
    if (!toast) return
    const t = setTimeout(() => setToast(null), 2600)
    return () => clearTimeout(t)
  }, [toast])

  const go = useCallback((s: Screen) => {
    history.replaceState(null, '', `#${s}`)
    setScreen(s)
  }, [])

  const toggleTheme = () => {
    const next: Theme = theme === 'dark' ? 'light' : 'dark'
    if (next === 'light') document.documentElement.dataset.theme = 'light'
    else delete document.documentElement.dataset.theme
    try {
      localStorage.setItem('mc-theme', next)
    } catch {
      /* storage unavailable: the theme still applies for this session */
    }
    setTheme(next)
  }

  const lensName = lenses.find((l) => l.id === settings?.lens)?.name ?? '…'

  const nav: { id: Screen; label: string; icon: ReactNode }[] = [
    { id: 'chat', label: 'Chat', icon: <ChatIcon /> },
    { id: 'memory', label: 'Memory', icon: <MemoryIcon /> },
    { id: 'sources', label: 'Sources', icon: <SourcesIcon /> },
  ]

  return (
    <div className="app">
      <header className="topbar">
        <div className="wordmark">
          <i />
          mini-cortex
        </div>
        <button type="button" className="chip" onClick={() => go('tune')} title="Change the lens on Tune">
          Lens <b>{lensName}</b>
        </button>
        {connectors.map((c) => (
          <button type="button" key={c.name} className="chip" onClick={() => go('sources')}>
            <span className={`dot${c.status === 'live' ? ' live pulse' : ''}`} />
            {c.name}
          </button>
        ))}
        {mode === 'live' && connectors.length === 0 && (
          <button type="button" className="chip" onClick={() => go('sources')}>
            <span className="dot" />
            no connectors yet
          </button>
        )}
        <div className="spacer" />
        {mode && (
          <span
            className={`chip mode ${mode}`}
            title={
              mode === 'live'
                ? 'Connected to the mini-cortex app: connectors, protocol frames, memory and chat are real.'
                : 'No mini-cortex app found at /api: everything is demo data.'
            }
          >
            {mode === 'live' ? 'live hub' : 'demo data'}
          </span>
        )}
        {llm &&
          (llm.missing.length === 0 ? (
            <button type="button" className="chip mono" onClick={() => go('tune')} title="Models and keys, on Tune">
              {llm.chat_model} · {llm.embed_model}
              {llm.embed_dim ? `/${llm.embed_dim}` : ''}
            </button>
          ) : (
            <button
              type="button"
              className="chip connect"
              onClick={() => go('tune')}
              title={`Missing: ${llm.missing.join(', ')}`}
            >
              <span className="dot" />
              Connect a model
            </button>
          ))}
        <button
          type="button"
          className="icon-btn"
          onClick={toggleTheme}
          aria-label={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
          title={theme === 'dark' ? 'Light theme' : 'Dark theme'}
        >
          {theme === 'dark' ? <SunIcon /> : <MoonIcon />}
        </button>
        <button
          type="button"
          className="btn ghost"
          title="Bonus: the dream phase consolidates memory offline"
          onClick={() => setToast('Sleep (dream phase) is a bonus step and is not built yet.')}
        >
          ☾ Sleep
        </button>
      </header>

      <nav className="rail" aria-label="Screens">
        {nav.map((n) => (
          <button
            type="button"
            key={n.id}
            aria-current={screen === n.id ? 'page' : undefined}
            onClick={() => go(n.id)}
          >
            {n.icon}
            {n.label}
          </button>
        ))}
        <button
          type="button"
          className="bottom"
          aria-current={screen === 'tune' ? 'page' : undefined}
          onClick={() => go('tune')}
        >
          <TuneIcon />
          Tune
        </button>
      </nav>

      <main className="screen">
        {screen === 'chat' && <ChatScreen />}
        {screen === 'memory' && settings && mode && (
          <MemoryScreen theta={settings.theta} mode={mode} connectors={connectors} />
        )}
        {screen === 'sources' && mode && (
          <SourcesScreen connectors={connectors} hub={hub} mode={mode} notify={setToast} />
        )}
        {screen === 'tune' && settings && (
          <TuneScreen
            settings={settings}
            lenses={lenses}
            llm={llm}
            onLlmSaved={(s) => {
              setLlm(s)
              setToast(s.missing.length ? `Saved. Still missing: ${s.missing.join(', ')}.` : 'Model settings saved to .env.')
            }}
            onApply={async (s) => {
              const r = await api.applySettings(s)
              setSettings(s)
              setToast(
                r.saved
                  ? 'Saved. New items use it now; Rebuild memory on Memory to redo existing packets.'
                  : `Settings applied. Re-ingesting ${r.queued} items.`,
              )
            }}
          />
        )}
      </main>

      {toast && (
        <div className="toast" role="status">
          {toast}
        </div>
      )}
    </div>
  )
}
