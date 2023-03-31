# Clients

## Documentation

Documentation for clients (i.e. client `README.md`s) are generated
from [docs_generate.zig](./docs_generate.zig).

Each client implements the `Docs` struct from
[docs_types.zig](./docs_types.zig).

The template for the README is in code in
[docs_generate.zig](./docs_generate.zig).

Existing `Docs` struct implementations are in:

* [go/docs.zig](./go/docs.zig), which generates [go/README.md](./go/README.md)
* [node/docs.zig](./node/docs.zig), which generates [node/README.md](./node/README.md)

### Run

To run the generator:

```console
$ ../../zig/zig run docs_generate.zig # ../../zig/zig to get the standard TigerBeetle Zig binary
```

### Skip validation

Validation involves Docker at the moment and this can take a few
minutes. You can skip validation when you are adding things to docs
that can't break (such as more documentation, not code).

```console
$ ../../zig/zig run docs_generate.zig -- --no-validate
```

### Just one language

To run the generator only for a certain language (defined by `.markdown_name`):

```console
$ ../../zig/zig run docs_generate.zig -- --only javascript
```

```console
$ ../../zig/zig run docs_generate.zig -- --only javascript,go
```

Docs are only regenerated/modified when there would be a diff so the
mtime of each README changes only as needed.

### Format files

To format all Zig files:

```console
../../zig/zig fmt .
```

## run_with_tb

This directory contains `run_with_tb.zig` that proxies commands passed
to it. Specifically, it:

* Creates a random temporary directory and formats TigerBeetle's data file there
* Spins up a TigerBeetle server on an open port
* Sets the port in the `TB_PORT` environment variable
* Calls the commands passed on the CLI
* Turns off the server and deletes the temporary directory

Example:

```bash
$ ./zig/zig build-exe src/clients/run_with_tb.zig
$ ./run_with_tb bash -c "npm install tigerbeetle-node && node $(pwd)/myscript.js"
```

Or:

```bash
$ ./zig/zig run ./src/clients/run_with_tb.zig -- bash -c "cd $(pwd)/src/clients/go/samples/two-phase && go run main.go"
```

Or (on Ventura you must go through `./scripts/build.sh`):

```
$ ./scripts/build.sh run_with_tb -- bash -c "cd $(pwd)/src/clients/go/samples/two-phase && go run main.go"
```

NOTE: All relative file paths in the proxied commands should be
resolved to absolute file paths since the runner doesn't necessarily
run in the same directory.

### Set working directory with `R_CWD`

Since you often need to `cd` to a directory to run a command, there's
a shorthand via the `R_CWD` environment variable.

```bash
$ R_CWD=$(pwd)/src/clients/go/samples/two-phase ./zig/zig run ./src/clients/run_with_tb.zig -- go run main.go
```
