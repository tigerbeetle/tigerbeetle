FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY zig-out/bin/antithesis_api ./antithesis_api
COPY tools/antithesis/lib/libvoidstar.so /usr/local/lib

ENTRYPOINT ["./antithesis_api"]
