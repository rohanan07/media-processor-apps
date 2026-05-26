# --- Stage 1: Build & Dependencies ---
FROM node:20-slim AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

# --- Stage 2: Final Runtime Environment ---
FROM node:20-slim
WORKDIR /app

# Install ffmpeg system-wide for video thumbnail extraction
RUN apt-get update && \
    apt-get install -y ffmpeg --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy optimized dependencies and worker code from builder
COPY --from=builder /app/node_modules ./node_modules
COPY . .

ENV NODE_ENV=production

CMD ["node", "worker.js"]