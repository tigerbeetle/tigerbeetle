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

## Building from source 

This section is only relevant to folks modifying the Go client code
itself. If you are just using the client, you can ignore this.

*Prerequisites:*
- go 1.17+
- zig 0.9.1

### 1. Clone this repository

```bash
git clone https://github.com/tigerbeetledb/tigerbeetle.git
cd tigerbeetle
```

### 2. Install Zig and Tigerbeetle

**Linux and macOS**

```bash
./scripts/install.sh
```

**Windows**

```cmd
.\scripts\install.bat
```

### 3. Build Go client

> zgo = go with CGO env setup

**Linux and macOS**

```bash
./zig/zig build go_client -Drelease-safe
cd src/clients/go
./zgo.sh test
```

**Windows**

```cmd
.\zig\zig.exe build go_client -Drelease-safe
cd src\clients\go
zgo.bat test
```
