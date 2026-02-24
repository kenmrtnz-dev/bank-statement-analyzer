import { resolve } from "node:path";
import { defineConfig } from "vite";

export default defineConfig({
  build: {
    outDir: resolve(__dirname, "../app/static/dist"),
    emptyOutDir: false,
    sourcemap: false,
    rollupOptions: {
      input: {
        app: resolve(__dirname, "src/main.js"),
      },
      output: {
        entryFileNames: "app.bundle.js",
        chunkFileNames: "chunks/[name]-[hash].js",
        assetFileNames: "assets/[name]-[hash][extname]",
      },
    },
  },
});
