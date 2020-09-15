FROM ubuntu:20.10
WORKDIR /tmp/zig

RUN  apt-get update \
  && apt-get install -y wget xz-utils
RUN wget -q https://ziglang.org/builds/zig-linux-x86_64-0.6.0+fd4783906.tar.xz && \
        tar -xf zig-linux-x86_64-0.6.0+fd4783906.tar.xz && \
        mv "zig-linux-x86_64-0.6.0+fd4783906" /usr/local/lib/zig && \
        ln -s /usr/local/lib/zig/zig /usr/local/bin/zig && \
        rm zig-linux-x86_64-0.6.0+fd4783906.tar.xz

WORKDIR /opt/alpha-beetle

COPY ./io_uring.zig .
COPY ./server.zig .

RUN zig build-exe server.zig

CMD ["./server"]