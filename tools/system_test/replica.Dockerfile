FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY zig-out/bin/tigerbeetle ./tigerbeetle
COPY tools/system_test/scripts/run-tigerbeetle.sh ./run-tigerbeetle.sh

# The `[o]` is a hack for optionally copying libvoidstar.so.
# COPY tools/system_test/lib/libvoidstar.s[o] /usr/local/lib

ENTRYPOINT ["./run-tigerbeetle.sh"]
