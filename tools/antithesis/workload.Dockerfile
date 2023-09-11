FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY zig-out/bin/antithesis_workload ./antithesis_workload


ENTRYPOINT ["./antithesis_workload"]
