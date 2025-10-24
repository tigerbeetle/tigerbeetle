# `create_accounts`

Create one or more [`Account`](../account.md)s.

## Event

A batch of accounts to create.
See [`Account`](../account.md) for constraints.

## Results

An array containing the result for each account in the event batch.

### `timestamp`

- For [successful accounts](#ok), it is the [`timestamp`](../account.md#timestamp)
  assigned to the account object.
- For [existing accounts](#exists), it is the [`timestamp`](../account.md#timestamp)
  of the original object.
- For all other results, it indicates the time at which validation occurred.

<details>
<summary>Client release &lt; 0.17.0</summary>

Create results are sparse, containing only failed events and the `index`
of the account within the events batch.

The network protocol does not include an [`ok`](#ok) result for successfully
created accounts.

</details>

## Result

Results are listed in this section in order of descending precedence — that is, if more than one
error is applicable to the account being created, only the result listed first is returned.

### `ok`

The account was successfully created; it did not previously exist.

### `linked_event_failed`

The account was not created. One or more of the accounts in the
[linked chain](../account.md#flagslinked) is invalid, so the whole chain failed.

### `linked_event_chain_open`

The account was not created. The [`Account.flags.linked`](../account.md#flagslinked) flag was set on
the last event in the batch, which is not legal. (`flags.linked` indicates that the chain continues
to the next operation).

### `imported_event_expected`

The account was not created. The [`Account.flags.imported`](../account.md#flagsimported) was
set on the first account of the batch, but not all accounts in the batch.
Batches cannot mix imported accounts with non-imported accounts.

### `imported_event_not_expected`

The account was not created. The [`Account.flags.imported`](../account.md#flagsimported) was
expected to _not_ be set, as it's not allowed to mix accounts with different `imported` flag
in the same batch. The first account determines the entire operation.

### `timestamp_must_be_zero`

This result only applies when [`Account.flags.imported`](../account.md#flagsimported) is _not_ set.

The account was not created. The [`Account.timestamp`](../account.md#timestamp) is nonzero, but
must be zero. The cluster is responsible for setting this field.

The [`Account.timestamp`](../account.md#timestamp) can only be assigned when creating accounts
with [`Account.flags.imported`](../account.md#flagsimported) set.

### `imported_event_timestamp_out_of_range`

This result only applies when [`Account.flags.imported`](../account.md#flagsimported) is set.

The account was not created. The [`Account.timestamp`](../account.md#timestamp) is out of range,
but must be a user-defined timestamp greater than `0` and less than `2^63`.

### `imported_event_timestamp_must_not_advance`

This result only applies when [`Account.flags.imported`](../account.md#flagsimported) is set.

The account was not created. The user-defined [`Account.timestamp`](../account.md#timestamp) is
greater than the current [cluster time](../../coding/time.md), but it must be a past timestamp.

### `reserved_field`

The account was not created. [`Account.reserved`](../account.md#reserved) is nonzero, but must be
zero.

### `reserved_flag`

The account was not created. `Account.flags.reserved` is nonzero, but must be zero.

### `id_must_not_be_zero`

The account was not created. [`Account.id`](../account.md#id) is zero, which is a reserved value.

### `id_must_not_be_int_max`

The account was not created. [`Account.id`](../account.md#id) is `2^128 - 1`, which is a reserved
value.

### `exists_with_different_flags`

An account with the same `id` already exists, but with different [`flags`](../account.md#flags).

### `exists_with_different_user_data_128`

An account with the same `id` already exists, but with different
[`user_data_128`](../account.md#user_data_128).

### `exists_with_different_user_data_64`

An account with the same `id` already exists, but with different
[`user_data_64`](../account.md#user_data_64).

### `exists_with_different_user_data_32`

An account with the same `id` already exists, but with different
[`user_data_32`](../account.md#user_data_32).

### `exists_with_different_ledger`

An account with the same `id` already exists, but with different [`ledger`](../account.md#ledger).

### `exists_with_different_code`

An account with the same `id` already exists, but with different [`code`](../account.md#code).

### `exists`

An account with the same `id` already exists.

With the possible exception of the following fields, the existing account is identical to the
account in the request:

- `timestamp`
- `debits_pending`
- `debits_posted`
- `credits_pending`
- `credits_posted`

To correctly [recover from application crashes](../../coding/reliable-transaction-submission.md),
many applications should handle `exists` exactly as [`ok`](#ok).

### `flags_are_mutually_exclusive`

The account was not created. An account cannot be created with the specified combination of
[`Account.flags`](../account.md#flags).

The following flags are mutually exclusive:

- [`Account.flags.debits_must_not_exceed_credits`](../account.md#flagsdebits_must_not_exceed_credits)
- [`Account.flags.credits_must_not_exceed_debits`](../account.md#flagscredits_must_not_exceed_debits)

### `debits_pending_must_be_zero`

The account was not created. [`Account.debits_pending`](../account.md#debits_pending) is nonzero,
but must be zero.

An account's debits and credits are only modified by transfers.

### `debits_posted_must_be_zero`

The account was not created. [`Account.debits_posted`](../account.md#debits_posted) is nonzero, but
must be zero.

An account's debits and credits are only modified by transfers.

### `credits_pending_must_be_zero`

The account was not created. [`Account.credits_pending`](../account.md#credits_pending) is nonzero,
but must be zero.

An account's debits and credits are only modified by transfers.

### `credits_posted_must_be_zero`

The account was not created. [`Account.credits_posted`](../account.md#credits_posted) is nonzero,
but must be zero.

An account's debits and credits are only modified by transfers.

### `ledger_must_not_be_zero`

The account was not created. [`Account.ledger`](../account.md#ledger) is zero, but must be nonzero.

### `code_must_not_be_zero`

The account was not created. [`Account.code`](../account.md#code) is zero, but must be nonzero.

### `imported_event_timestamp_must_not_regress`

This result only applies when [`Account.flags.imported`](../account.md#flagsimported) is set.

The account was not created. The user-defined [`Account.timestamp`](../account.md#timestamp)
regressed, but it must be greater than the last timestamp assigned to any `Account` in the cluster and cannot be equal to the timestamp of any existing [`Transfer`](../transfer.md).

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#creating-accounts)
- [Java library](/src/clients/java/README.md#creating-accounts)
- [Go library](/src/clients/go/README.md#creating-accounts)
- [Node.js library](/src/clients/node/README.md#creating-accounts)
- [Python library](/src/clients/python/README.md#creating-accounts)

## Internals

If you're curious and want to learn more, you can find the source code for creating an account in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig).
Search for `fn create_account(` and `fn execute(`.
