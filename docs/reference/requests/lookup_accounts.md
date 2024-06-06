# `lookup_accounts`

Fetch one or more accounts by their `id`s.

⚠️ Note that you **should not** check an account's balance using this request before creating a
transfer. That would not be atomic and the balance could change in between the check and the
transfer. Instead, you should use a balance limit on the account or a
[balance-conditional transfer](../../develop/recipes/balance-conditional-transfers.md) to ensure the
balance check is done atomically with the transfer.

## Event

An [`id`](../account.md#id) belonging to a [`Account`](../account.md).

## Result

- If the account exists, return the [`Account`](../account.md).
- If the account does not exist, return nothing.

## Client libraries

For language-specific docs see:

* [.NET library](/src/clients/dotnet/README.md#account-lookup)
* [Java library](/src/clients/java/README.md#account-lookup)
* [Go library](/src/clients/go/README.md#account-lookup)
* [Node.js library](/src/clients/node/README.md#account-lookup)

## Internals

If you're curious and want to learn more, you can find the source code
for looking up an account in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig). Search
for `fn execute_lookup_accounts(`.
