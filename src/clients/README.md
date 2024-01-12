# Clients

## Documentation

Documentation for clients (i.e. client `README.md`s) are generated
from [../scripts/client_readmes.zig](../scripts/client_readmes.zig).

Each client implements the `Docs` struct from
[docs_types.zig](./docs_types.zig).

The template for the README is in code in
[../scripts/client_readmes.zig](../scripts/client_readmes.zig).

Existing `Docs` struct implementations are in:

* [dotnet/docs.zig](./dotnet/docs.zig), which generates [dotnet/README.md](./dotnet/README.md)
* [go/docs.zig](./go/docs.zig), which generates [go/README.md](./go/README.md)
* [java/docs.zig](./java/docs.zig), which generates [java/README.md](./java/README.md)
* [node/docs.zig](./node/docs.zig), which generates [node/README.md](./node/README.md)

### Run

Go to the repo root.

If you don't already have the TigerBeetle version of `zig` run:

```console
./scripts/install_zig.[sh|bat]
```

Use the `.sh` script if you're on macOS or Linux. Use the `.bat`
script on Windows.

To build and run the client docs generator:

```console
./zig/zig build scripts -- ci
```

### Just one language

To run the generator only for a certain language (defined by `.markdown_name`):

```console
./zig/zig build scripts -- ci --language=go
```

Docs are only regenerated/modified when there would be a diff so the
mtime of each README changes only as needed.

### Format files

To format all Zig files (again, run from the repo root):

```console
./zig/zig fmt .
```
