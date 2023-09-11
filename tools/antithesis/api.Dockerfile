FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY tools/antithesis/lib/libvoidstar.so /lib
COPY zig-out/bin/antithesis_api ./antithesis_api


ENTRYPOINT ["./antithesis_api"]
