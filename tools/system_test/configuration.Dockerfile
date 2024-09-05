FROM debian:stable-slim

RUN mkdir -p /volumes/database

FROM scratch

COPY tools/system_test/config/docker-compose.yaml docker-compose.yaml
COPY tools/system_test/config/.env .env
COPY --from=0 /volumes/database /volumes/database
