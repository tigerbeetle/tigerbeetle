FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY lib/libvoidstar.so         /lib
COPY zig-out/bin/tigerbeetle    ./tigerbeetle
COPY scripts/run-tigerbeetle.sh ./run-tigerbeetle.sh

ENTRYPOINT ["./run-tigerbeetle.sh"]
