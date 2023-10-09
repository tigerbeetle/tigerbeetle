# `lookup_transfers`

Fetch one or more transfers by their `id`s.

## Event

An [`id`](../transfers.md#id) belonging to a [`Transfer`](../transfers.md).

## Result

- If the transfer exists, return the [`Transfer`](../transfers.md).
- If the transfer does not exist, return nothing.

## Client libraries

For language-specific docs see:

* [Looking up transfers using the .NET library](/src/clients/dotnet/README.md#transfer-lookup)
* [Looking up transfers using the Java library](/src/clients/java/README.md#transfer-lookup)
* [Looking up transfers using the Go library](/src/clients/go/README.md#transfer-lookup)
* [Looking up transfers using the Node.js library](/src/clients/node/README.md#transfer-lookup)

## Internals

If you're curious and want to learn more, you can find the source code
for looking up a transfer in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig). Search
for `fn execute_lookup_transfers(`.
