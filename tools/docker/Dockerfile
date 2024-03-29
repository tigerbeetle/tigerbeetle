# We _could_ use `FROM scratch` here, but let's use alpine to ease debugging in case someone needs
# to poke inside the container.
FROM alpine:3.19

# TigerBeetle doesn't install its own signal handlers, and PID 1 doesn't have a default SIGTERM
# signal handler. (See https://github.com/krallin/tini#why-tini).
# Using "tini" as PID 1 ensures that signals work as expected, so e.g. "docker stop" will not hang.
RUN apk add --no-cache tini

# NB: We are doing something unconventional here. To ensure that docker images contain the exact
# same TigerBeetle binary we publish as our release, we don't build inside a container, and copy a
# pre-existing binary instead.
ARG TARGETARCH
COPY tigerbeetle-${TARGETARCH} /tigerbeetle

ENTRYPOINT ["tini", "--", "/tigerbeetle"]
