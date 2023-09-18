FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY zig-out/bin/antithesis_workload ./antithesis_workload
COPY tools/antithesis/lib/libvoidstar.so /usr/local/lib


ENTRYPOINT ["./antithesis_workload"]
