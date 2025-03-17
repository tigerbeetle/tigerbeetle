# `lookup_accounts`

Fetch one or more accounts by their `id`s.

⚠️ Note that you **should not** check an account's balance using this request before creating a
transfer. That would not be atomic and the balance could change in between the check and the
transfer. Instead, set the
[`debits_must_not_exceed_credits`](../account.md#flagsdebits_must_not_exceed_credits) or
[`credits_must_not_exceed_debits`](../account.md#flagscredits_must_not_exceed_debits) flag on the
accounts to limit their account balances. More complex conditional transfers can be expressed using
[balance-conditional transfers](../../coding/recipes/balance-conditional-transfers.md).

⚠️ It is not possible currently to look up more than a full batch (8189) of accounts atomically.
When issuing multiple `lookup_accounts` calls, it can happen that other operations will interleave
between the calls leading to read skew. Consider using the
[`history`](../account.md#flagshistory) flag to enable atomic lookups.

## Event

An [`id`](../account.md#id) belonging to a [`Account`](../account.md).

## Result

- If the account exists, return the [`Account`](../account.md).
- If the account does not exist, return nothing.

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#account-lookup)
- [Java library](/src/clients/java/README.md#account-lookup)
- [Go library](/src/clients/go/README.md#account-lookup)
- [Node.js library](/src/clients/node/README.md#account-lookup)

## Internals

If you're curious and want to learn more, you can find the source code for looking up an account in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig).
Search for `fn execute_lookup_accounts(`.
