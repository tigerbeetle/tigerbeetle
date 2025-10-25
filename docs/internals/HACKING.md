# Hacking on TigerBeetle

**Prerequisites:** TigerBeetle makes use of certain fairly new technologies, such as
[io_uring](https://kernel.dk/io_uring.pdf) or advanced CPU instructions for cryptography. As such,
it requires a fairly modern kernel (≥ 5.6) and CPU. While at the moment only Linux is supported for
production deployments, TigerBeetle also works on Windows and MacOS.

## Building

```console
git clone https://github.com/tigerbeetle/tigerbeetle.git
cd tigerbeetle
./zig/download.ps1 # Yes, .ps1 even on Linux.
./zig/zig build -Drelease
./tigerbeetle version
```

See the [Quick Start](/docs/start.md) for how to use a freshly-built TigerBeetle and
[docs.tigerbeetle.com](https://docs.tigerbeetle.com) for the rest of user-facing documentation.

## Testing

All database tests:

```console
./zig/zig build test
```

A specific test:

```console
./zig/zig build test -- parse_addresses
```

Fuzzing ([/src/fuzz_tests.zig](/src/fuzz_tests.zig)):

```console
./zig/zig build fuzz -- smoke
./zig/zig build fuzz -- lsm_tree
```

Continuous Integration entry point (see [ci.yml](/.github/workflows/ci.yml)):

```console
./zig/zig build ci
```

## Simulation

The bulk of testing happens via our deterministic simulator:

```console
./zig/zig build vopr
```

To run the VOPR using a specific seed (this produces a fully deterministic, reproducible outcome):

```console
./zig/zig build vopr -- 123
```

See [./testing.md](./testing.md) for the explanation of the output format.

## CFO

In addition to the standard GitHub CI infrastructure that is used for tests and merge queue, we
employ a cluster of machines for continuous fuzzing, via the Continuous Fuzzing Orchestrator
([/src/scripts/cfo.zig](/src/scripts/cfo.zig)). You can see the results on devhub:

<https://devhub.tigerbeetle.com>

To direct CFO's eye of Sauron towards your PR, apply one of `fuzz` labels, e.g.,
[`fuzz vopr`](https://github.com/tigerbeetle/tigerbeetle/labels/fuzz%20vopr).

## Clients

Each client is built using language-specific tooling (`npm`, `maven`, `dotnet`, and `go`), and links
to a native library built with Zig. The general pattern is

```console
./zig/zig build clients:lang
cd src/clients/lang
lang_package_manager test
```

See `src/clients/$LANG/ci.zig` scripts for exact commands we use on CI to build clients.

### Testing Client Libraries

Each language client is tested by a mixture of unit-tests written using language-specific test
frameworks, and integration tests which run sample projects against a real `tigerbeetle` process.
Everything is orchestrated by [ci.zig](/src/scripts/ci.zig) script:

```console
./zig/zig build scripts -- ci --language=go
```

## Other Useful Commands

Build & immediately run TigerBeetle:

```console
./zig/zig build run -- format ...
```

Quickly check if the code compiles without spending time to generate the binary:

```console
./zig/zig build check
```

Reformat the code according to style:

```
./zig/zig fmt .
```

Run lint checks:

```
./zig/zig build test -- tidy
```

Run macro benchmark:

```
./zig/zig build -Drelease run -- benchmark
```

See comments at the top of
[/src/tigerbeetle/benchmark_load.zig](/src/tigerbeetle/benchmark_load.zig)
for details of benchmarking.

## Pull Requests

When submitting pull request, _assign_ a single person to be its reviewer. Unpacking:

* GitHub supports both "assign" and "request review". The difference between them is that "request"
  is "edge triggered" (it is cleared after a round of review), while "assign" is "level triggered"
  (it won't go away until the PR is merged or closed). We use "assign", because the reviewer is
  co-responsible for making sure that the PR doesn't stall, and is eventually completed.

* Only a single person is assigned to any particular pull request, to avoid diffusion of
  responsibility and the bystander effect.

* Pull request author chooses the reviewer. The author has the most context about who is the best
  person to request review from. When picking a reviewer, think about sharing knowledge, balancing
  review load, and maximizing correctness of the code.

After pull request is approved, the author makes the final call to merge by clicking "merge when
ready" button on GitHub. To reduce the number of round-trips, "merge when ready" can be engaged
before the review is completed: a PR will then be merged automatically once an approving review is
submitted.

To synchronize the state of the pull request, rebase the pull request on top of main branch. You
don't need to proactively synchronize pull requests with main: merge queue runs the tests on the
merge commit from the PR branch into main.
