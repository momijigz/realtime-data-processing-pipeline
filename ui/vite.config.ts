import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Dev server runs on :5173. /api/* is proxied to the Go API container so the
// browser never makes cross-origin calls — same URL shape in dev and prod.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.VITE_API_TARGET || 'http://api:8090',
        changeOrigin: true,
      },
    },
  },
})
