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

To run the generator:

```console
$ ../../zig/zig run docs_generate.zig # ../../zig/zig to get the standard TigerBeetle Zig binary
```

To run the generator only for a certain language (defined by `.markdown_name`):

```console
$ ../../zig/zig run docs_generate.zig -- --only javascript
```

```console
$ ../../zig/zig run docs_generate.zig -- --only javascript,go
```

Docs are only regenerated/modified when there would be a diff so the
mtime of each README changes only as needed.

To format all Zig files:

```console
$ find . -name "*.zig" -depth 2 | xargs -I {} zig fmt {}
```
