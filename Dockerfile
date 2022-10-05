FROM ubuntu:22.04 AS build

RUN  apt-get update \
  && apt-get install -y wget xz-utils

WORKDIR /opt/beta-beetle

COPY src ./src
COPY scripts ./scripts
COPY build.zig ./build.zig

ENV PATH="${PATH}:/opt/beta-beetle/zig"

# Allows passing in --build-arg DEBUG=true during builds (needed by Github CI for generating debug image)
ARG DEBUG=false
RUN ./scripts/install.sh

FROM ubuntu:22.04 AS release
WORKDIR /opt/beta-beetle

COPY --from=build /opt/beta-beetle/tigerbeetle ./tigerbeetle

ENTRYPOINT ["./tigerbeetle"]
