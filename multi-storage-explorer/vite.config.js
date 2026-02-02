import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5175,
    strictPort: false,
  },
  build: {
    // Build output goes to the Python package static directory
    outDir: path.resolve(__dirname, '../multi-storage-client/src/multistorageclient/explorer/static'),
    emptyOutDir: true,
  },
})


