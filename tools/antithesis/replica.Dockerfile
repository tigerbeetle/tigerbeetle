FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY zig-out/bin/tigerbeetle ./tigerbeetle
COPY tools/antithesis/scripts/run-tigerbeetle.sh ./run-tigerbeetle.sh
COPY tools/antithesis/lib/libvoidstar.so /usr/local/lib

ENTRYPOINT ["./run-tigerbeetle.sh"]
