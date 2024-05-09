# `lookup_transfers`

Fetch one or more transfers by their `id`s.

## Event

An [`id`](../transfer.md#id) belonging to a [`Transfer`](../transfer.md).

## Result

- If the transfer exists, return the [`Transfer`](../transfer.md).
- If the transfer does not exist, return nothing.

## Client libraries

For language-specific docs see:

* [.NET library](/src/clients/dotnet/README.md#transfer-lookup)
* [Java library](/src/clients/java/README.md#transfer-lookup)
* [Go library](/src/clients/go/README.md#transfer-lookup)
* [Node.js library](/src/clients/node/README.md#transfer-lookup)

## Internals

If you're curious and want to learn more, you can find the source code
for looking up a transfer in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig). Search
for `fn execute_lookup_transfers(`.
