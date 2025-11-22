# Dockerfile
# Multi-stage build for smaller runtime image
FROM node:22-alpine AS base
WORKDIR /app
RUN apk add --no-cache tini bash curl tzdata
ENV NODE_ENV=production

# Only copy package manifests first for better caching
COPY package*.json ./
RUN npm ci --omit=dev

# Now copy the rest of the app
COPY . .

# Ensure scripts are executable (if you add bash shims later)
# RUN chmod +x ./bin/*.sh

# Healthcheck script (optional)
RUN printf '#!/bin/sh\nnode -e "process.exit(0)"\n' > /usr/local/bin/healthcheck && chmod +x /usr/local/bin/healthcheck

# Default command overridden by docker-compose
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "api/server.js"]
