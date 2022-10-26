#!/usr/bin/env bash

set -eu

# Format all code
gofmt -w -s .

# Regenerate types
go install golang.org/x/tools/cmd/stringer@latest
go generate ./pkg/types/main.go
