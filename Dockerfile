FROM ubuntu:20.10 as build

RUN  apt-get update \
  && apt-get install -y wget xz-utils

WORKDIR /opt/beta-beetle

COPY src ./src
COPY scripts ./scripts
COPY build.zig ./build.zig

ENV PATH="${PATH}:/opt/beta-beetle/zig"
RUN ./scripts/install.sh

FROM ubuntu:20.10
WORKDIR /opt/beta-beetle

COPY --from=build /opt/beta-beetle/tigerbeetle ./tigerbeetle

ENTRYPOINT ["./tigerbeetle"]
