FROM node:22-bookworm

WORKDIR /app/frontend

COPY frontend/package.json ./
RUN npm install

COPY frontend /app/frontend
COPY infra/docker/start-frontend-dev.sh /usr/local/bin/start-frontend-dev.sh
RUN chmod +x /usr/local/bin/start-frontend-dev.sh
