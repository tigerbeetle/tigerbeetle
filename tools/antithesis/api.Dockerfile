FROM debian:stable-slim
WORKDIR /opt/tigerbeetle

COPY lib/libvoidstar.so /lib
COPY zig-out/bin/api    ./api


ENTRYPOINT ["./api"]
