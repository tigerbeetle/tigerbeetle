# Clients

## Documentation

Documentation for clients (i.e. client `README.md`s) are generated
from [docs_generate.zig](./docs_generate.zig).

Each client implements the `Docs` struct from
[docs_types.zig](./docs_types.zig).

The template for the README is in code in
[docs_generate.zig](./docs_generate.zig).

Existing `Docs` struct implementations are in:

* [dotnet/docs.zig](./dotnet/docs.zig), which generates [dotnet/README.md](./dotnet/README.md)
* [go/docs.zig](./go/docs.zig), which generates [go/README.md](./go/README.md)
* [java/docs.zig](./java/docs.zig), which generates [java/README.md](./java/README.md)
* [node/docs.zig](./node/docs.zig), which generates [node/README.md](./node/README.md)

### Run

Go to the repo root.

If you don't already have the TigerBeetle version of `zig` run:

```console
$ ./scripts/install_zig.[sh|bat]
```

Use the `.sh` script if you're on macOS or Linux. Use the `.bat`
script on Windows.

To build and run the client docs generator:

```console
$ ./scripts/build.[sh|bat] client_docs --
```

Note: Omitting the `--` will only build, not run the client_docs script.

### Skip validation

Validation involves Docker at the moment and this can take a few
minutes. You can skip validation when you are adding things to docs
that can't break (such as more documentation, not code).

```console
$ ./scripts/build.[sh|bat] client_docs -- --no-validate
```

### Just one language

To run the generator only for a certain language (defined by `.markdown_name`):

```console
$ ./scripts/build.[sh|bat] client_docs -- --language node
```

```console
$ ./scripts/build.[sh|bat] client_docs -- --language node,go
```

Docs are only regenerated/modified when there would be a diff so the
mtime of each README changes only as needed.

### Format files

To format all Zig files (again, run from the repo root):

```console
./zig/zig fmt .
```


## integration.zig / client_integration

This directory contains `integration.zig` that sets up an environment
(without Docker) to run the sample code in
`src/clients/$client/samples/$sample` against a running TigerBeetle
server using `run_with_tb` (described below).

Examples:

```bash
$ ./scripts/build.[sh|bat] client_integration -- --language java --sample basic
```

This corresponds to setting up the sample code in
`src/clients/java/samples/basic/` and running it.

IMPORTANT: For almost all of the dependencies in this script
(`tigerbeetle`, `java_client`/`go_client`/etc.), this script does not
make sure that these dependencies are all up-to-date. This is because
it would take a long time to rebuild all of these every time.

If you're experience weirdness, try `git clean -xfd` (kind of drastic,
but still) and rebuild TigerBeetle and clients.

And finally, it assumes you have the dependencies like `maven` and
`java` installed when you are running that language's sample code.

### Keep temporary directory

This script munges packaging info to make sure the project is
referencing the local build of the client library rather than the
build from the package manager. This is important/useful for testing
local changes.

If you need to debug the munging though you can run this script with
`--keep-tmp` to prevent the script from deleting the temporary
directory where the sample code is copied into and where munging takes
place.

```bash
$ ./scripts/build.[sh|bat] client_integration -- --language java --sample basic --keep-tmp
```

## run_with_tb.zig / run_with_tb

This directory contains `run_with_tb.zig` that proxies commands passed
to it. Specifically, it:

* Creates a random temporary directory and formats TigerBeetle's data file there
* Spins up a TigerBeetle server on an open port
* Sets the port in the `TB_ADDRESS` environment variable
* Calls the commands passed on the CLI
* Turns off the server and deletes the temporary directory

Example:

```bash
$ ./scripts/build.[sh|bat] run_with_tb -- node $(pwd)/myscript.js
```

If you need to run multiple commands you can wrap in `bash -c " ... "`:

```bash
$ ./scripts/build.[sh|bat] run_with_tb -- bash -c "stuff && otherstuff"
```

NOTE: All relative file paths in the proxied commands should be
resolved to absolute file paths since the runner doesn't necessarily
run in the current directory.

### Set working directory with `R_CWD`

Since you often need to `cd` to a directory to run a command, there's
a shorthand via the `R_CWD` environment variable.

```bash
$ R_CWD=$(pwd)/src/clients/go/samples/two-phase ./scripts/build.[sh|bat] run_with_tb -- go run main.go
```
