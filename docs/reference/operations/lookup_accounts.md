# `lookup_accounts`

Fetch one or more accounts by their `id`s.

## Event

An [`id`](../accounts.md#id) belonging to a [`Account`](../accounts.md).

## Result

- If the account exists, return the [`Account`](../accounts.md).
- If the account does not exist, return nothing.

## Client libraries

For language-specific docs see:

* [Looking up accounts using the .NET library](/src/clients/dotnet/README.md#account-lookup)
* [Looking up accounts using the Java library](/src/clients/java/README.md#account-lookup)
* [Looking up accounts using the Go library](/src/clients/go/README.md#account-lookup)
* [Looking up accounts using the Node.js library](/src/clients/node/README.md#account-lookup)

## Internals

If you're curious and want to learn more, you can find the source code
for looking up an account in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig). Search
for `fn execute_lookup_accounts(`.
