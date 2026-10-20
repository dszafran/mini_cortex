import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '@fontsource-variable/bricolage-grotesque'
import '@fontsource-variable/hanken-grotesk'
import '@fontsource-variable/jetbrains-mono'
import './styles/tokens.css'
import './styles/app.css'
import './styles/screens.css'
import App from './App.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
