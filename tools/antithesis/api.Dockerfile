FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY zig-out/bin/antithesis_api ./antithesis_api

# The `[o]` is a hack for optionally copying libvoidstar.so.
COPY tools/antithesis/lib/libvoidstar.s[o] /usr/local/lib

ENTRYPOINT ["./antithesis_api"]
