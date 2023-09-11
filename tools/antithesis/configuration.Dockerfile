FROM scratch

COPY config/docker-compose.yaml docker-compose.yaml
COPY config/.env .env
