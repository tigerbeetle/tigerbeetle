FROM scratch

COPY tools/system_test/config/docker-compose.yaml docker-compose.yaml
COPY tools/system_test/config/.env .env
COPY tools/system_test/config/volumes/database /volumes/database
