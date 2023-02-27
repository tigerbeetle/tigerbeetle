# Since we support arm64, take advantage of Docker and Zig's cross
# compilation, rather than running under qemu.
# https://www.docker.com/blog/faster-multi-platform-builds-dockerfile-cross-compilation-guide/
FROM --platform=$BUILDPLATFORM ubuntu:22.04 AS build

RUN  apt-get update \
  && apt-get install -y wget xz-utils

WORKDIR /opt/beta-beetle

# Copy install_zig by itself, to try and keep cache hits as high as possible.
RUN mkdir scripts
COPY scripts/install_zig.sh ./scripts

ENV PATH="${PATH}:/opt/beta-beetle/zig"

# Allows passing in --build-arg DEBUG=true during builds (needed by Github CI for generating debug image)
ARG DEBUG=false

# With our multiplatform builds, Docker will only repeat steps after the ARG TARGETPLATFORM.
# So, explicitly install zig before then, to not need to do it twice.
RUN ./scripts/install_zig.sh

COPY scripts ./scripts
COPY src ./src
COPY build.zig ./build.zig

ARG TARGETPLATFORM
RUN ./scripts/install.sh

FROM alpine:3.17 AS release
WORKDIR /

COPY --from=build /opt/beta-beetle/tigerbeetle ./tigerbeetle

ENTRYPOINT ["/tigerbeetle"]
