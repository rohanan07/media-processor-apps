# --- Stage 1: Build & Dependencies ---
FROM node:20-alpine AS builder
WORKDIR /app

# Copy package configurations first to leverage Docker layer caching
COPY package*.json ./
RUN npm ci --only=production

# --- Stage 2: Final Runtime Environment ---
FROM node:20-alpine
WORKDIR /app

# Copy optimized node_modules and application code from the builder stage
COPY --from=builder /app/node_modules ./node_modules
COPY . .

EXPOSE 3000

ENV NODE_ENV=production

CMD ["node", "server.js"]