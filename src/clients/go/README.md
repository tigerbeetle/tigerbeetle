# tigerbeetle-go

The TigerBeetle client for Go.

[![Go Reference](https://pkg.go.dev/badge/github.com/tigerbeetledb/tigerbeetle-go.svg)](https://pkg.go.dev/github.com/tigerbeetledb/tigerbeetle-go)

Make sure to import `github.com/tigerbeetledb/tigerbeetle-go`, not
this repo and subdirectory.

For example:

```bash
$ cat test.go
package main

import _ "github.com/tigerbeetledb/tigerbeetle-go"
import "fmt"

func main() {
  fmt.Println("Import ok!")
}

$ go mod init tigerbeetle-test
$ go mod tidy
$ go build
$ ./tigerbeetle-test
Import ok!
```

## A more real example

See [./samples/basic](./samples/basic) for a Go project
showing many features of the client.

## Development Setup

This section is only relevant to folks modifying the Go client code
itself. If you are just using the client, you can ignore this.

*Prerequisites:*
- go 1.17+
- zig 0.9.1

```sh
# Step 1 - Set up Zig
$ cd src/clients/go
$ ./tigerbeetle/scripts/install_zig.sh

# Step 2 - Build tb_client for your platform and use it in go
$ ./scripts/rebuild_binaries.sh

# Step 3 - Build and test tigerbeetle-go (zgo = go with CGO env setup)
$ ./zgo.sh test # on unix
$ zgo.bat test # on windows
```

## Other clients and documentation

- [Tigerbeetle Node](https://github.com/tigerbeetledb/tigerbeetle-node)
