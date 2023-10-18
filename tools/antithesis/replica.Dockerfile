FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY zig-out/bin/tigerbeetle ./tigerbeetle
COPY tools/antithesis/scripts/run-tigerbeetle.sh ./run-tigerbeetle.sh

# The `[o]` is a hack for optionally copying libvoidstar.so.
COPY tools/antithesis/lib/libvoidstar.s[o] /usr/local/lib

ENTRYPOINT ["./run-tigerbeetle.sh"]
