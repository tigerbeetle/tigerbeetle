---
sidebar_position: 2
---

# Single-node cluster with a single binary

Install TigerBeetle by grabbing the latest release from
GitHub.

## Prebuilt Linux binary

```bash
$ curl -LO https://github.com/tigerbeetledb/tigerbeetle/releases/download/2022-11-16-weekly/tigerbeetle-Linux-x64-2022-11-16-weekly.zip
$ unzip tigerbeetle-Linux-x64-2022-11-16-weekly.zip
$ sudo cp tigerbeetle /usr/local/bin/tigerbeetle
$ tigerbeetle version --verbose | head -n6
TigerBeetle version experimental

git_commit="b47292aaf2492e6b56a977009b85f7fca6e66775"

zig_version=0.9.1
mode=Mode.ReleaseSafe
```

### Debugging panics

If you run into panics, you can get more information by using the
debug binary. To grab this binary, add `--debug` before the `.zip`
extension:

```bash
$ curl -LO https://github.com/tigerbeetledb/tigerbeetle/releases/download/2022-11-16-weekly/tigerbeetle-Linux-x64-2022-11-16-weekly--debug.zip
```

## Prebuilt macOS binary

```bash
$ curl -LO https://github.com/tigerbeetledb/tigerbeetle/releases/download/2022-11-16-weekly/tigerbeetle-macOS-x64-2022-11-16-weekly.zip
$ unzip tigerbeetle-macOS-x64-2022-11-16-weekly.zip
$ sudo cp tigerbeetle /usr/local/bin/tigerbeetle
$ tigerbeetle version --verbose | head -n6
TigerBeetle version experimental

git_commit="b47292aaf2492e6b56a977009b85f7fca6e66775"

zig_version=0.9.1
mode=Mode.ReleaseSafe
```

### Debugging panics

If you run into panics, you can get more information by using the
debug binary. To grab this binary, add `--debug` before the `.zip`
extension:

```bash
$ curl -LO https://github.com/tigerbeetledb/tigerbeetle/releases/download/2022-11-16-weekly/tigerbeetle-macOS-x64-2022-11-16-weekly--debug.zip
```

## Building from source

Or to build from source, clone the repo, checkout a release, and run
the install script.

You will need POSIX userland, curl or wget, tar, and xz.

```bash
$ git clone https://github.com/tigerbeetledb/tigerbeetle.git
$ git checkout 2022-11-16-weekly # Or latest tag
$ cd tigerbeetle
$ scripts/install.sh
```

Don't worry, this will only make changes within the `tigerbeetle`
directory. No global changes. The result will place the compiled
`tigerbeetle` binary into the current directory.

### Debugging panics

If you run into panics, you can get more information by using the
debug binary:

```
$ DEBUG=true ./scripts/install.sh
```

## Running TigerBeetle

Now create the TigerBeetle data file.

```bash
$ ./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

And start the server.

```bash
$ ./tigerbeetle start --addresses=3000 0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 127.0.0.1:3000
```

Now you can connect to the running server with any client. For a quick
start, try [creating accounts and transfers in the Node
CLI](../usage/node-cli).
