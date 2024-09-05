ARG TAG

FROM debian:stable-slim

RUN mkdir -p /volumes/database
RUN echo "TAG=${TAG}" > /.env

FROM scratch

COPY src/testing/systest/config/docker-compose.yaml docker-compose.yaml
COPY --from=0 /.env .env
COPY --from=0 /volumes/database /volumes/database
