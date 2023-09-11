FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY zig-out/bin/tigerbeetle ./tigerbeetle
COPY tools/antithesis/scripts/run-tigerbeetle.sh ./run-tigerbeetle.sh

ENTRYPOINT ["./run-tigerbeetle.sh"]
