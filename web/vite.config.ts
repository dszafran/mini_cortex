import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// In dev, /api goes to the app (`uv run mini-cortex` in ../app). Without it the UI falls back to mock data.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': { target: 'http://127.0.0.1:8787', changeOrigin: true },
    },
  },
})
