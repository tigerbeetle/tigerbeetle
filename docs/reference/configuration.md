# Configuration

## `--cache-grid`

`--cache-grid=<size><KiB|MiB|GiB>` sets the size of TigerBeetle’s grid cache, which acts like a page
cache. Pass it to `tigerbeetle start`, for example `--cache-grid=256MiB`. The `--development` flag
uses a smaller default cache size.
