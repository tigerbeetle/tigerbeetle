FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY tools/antithesis/lib/libvoidstar.so /lib
COPY zig-out/bin/antithesis_workload ./antithesis_workload


ENTRYPOINT ["./antithesis_workload"]
