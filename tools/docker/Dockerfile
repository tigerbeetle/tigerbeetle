# We _could_ use `FROM scratch` here, but let's use alpine to ease debugging in case someone needs
# to poke inside the container.
FROM alpine:3.17

# NB: We are doing something unconventional here. To ensure that docker images contain the exact
# same TigerBeetle binary we publish as our release, we don't build inside a container, and copy a
# pre-existing binary instead.
ARG TARGETARCH
COPY tigerbeetle-${TARGETARCH} /tigerbeetle

ENTRYPOINT ["/tigerbeetle"]
