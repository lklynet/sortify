FROM node:22-bookworm-slim AS frontend-build

WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM node:22-bookworm-slim AS backend-build

WORKDIR /app/backend
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*
COPY backend/package*.json ./
RUN npm ci
COPY backend/ ./
RUN npm run build && npm prune --omit=dev

FROM node:22-bookworm-slim

WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3001
ENV SORTIFY_DB_PATH=/data/sortify.db
COPY --from=backend-build /app/backend/package*.json ./backend/
COPY --from=backend-build /app/backend/node_modules ./backend/node_modules
COPY --from=backend-build /app/backend/dist ./backend/dist
COPY --from=frontend-build /app/frontend/dist ./frontend/dist
EXPOSE 3001
CMD ["node", "backend/dist/server.js"]
