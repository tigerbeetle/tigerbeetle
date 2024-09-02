FROM scratch

COPY tools/antithesis/config/docker-compose.yaml docker-compose.yaml
COPY tools/antithesis/config/.env .env
