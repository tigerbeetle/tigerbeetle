# `create_transfers`

Create one or more [`Transfer`](../transfer.md)s. A successfully created transfer will modify the
amount fields of its [debit](../transfer.md#debit_account_id) and
[credit](../transfer.md#credit_account_id) accounts.

## Event

A batch of transfers to create.
See [`Transfer`](../transfer.md) for constraints.

## Results

An array containing the result for each transfer in the events batch.

### `timestamp`

- For [successful transfers](#ok), it is the [`timestamp`](../transfer.md#timestamp)
  assigned to the transfer object.
- For [existing transfers](#exists), it is the [`timestamp`](../transfer.md#timestamp)
  of the original object.
- For all other results, it indicates the time at which validation occurred.

<details>
<summary>Client release &lt; 0.17.0</summary>

Create results are sparse, containing only failed events and the `index`
of the transfer within the events batch.

The network protocol does not include an [`ok`](#ok) result for successfully
created transfers.

</details>

## Result

Results are listed in this section in order of descending precedence — that is, if more than one
error is applicable to the transfer being created, only the result listed first is returned.

### `ok`

The transfer was successfully created; did not previously exist.

### `linked_event_failed`

The transfer was not created. One or more of the other transfers in the
[linked chain](../transfer.md#flagslinked) is invalid, so the whole chain failed.

### `linked_event_chain_open`

The transfer was not created. The [`Transfer.flags.linked`](../transfer.md#flagslinked) flag was set
on the last event in the batch, which is not legal. (`flags.linked` indicates that the chain
continues to the next operation).

### `imported_event_expected`

The transfer was not created. The [`Transfer.flags.imported`](../transfer.md#flagsimported) was
set on the first transfer of the batch, but not all transfers in the batch.
Batches cannot mix imported transfers with non-imported transfers.

### `imported_event_not_expected`

The transfer was not created. The [`Transfer.flags.imported`](../transfer.md#flagsimported) was
expected to _not_ be set, as it's not allowed to mix transfers with different `imported` flag
in the same batch. The first transfer determines the entire operation.

### `timestamp_must_be_zero`

This result only applies when [`Account.flags.imported`](../account.md#flagsimported) is _not_ set.

The transfer was not created. The [`Transfer.timestamp`](../transfer.md#timestamp) is nonzero, but
must be zero. The cluster is responsible for setting this field.

The [`Transfer.timestamp`](../transfer.md#timestamp) can only be assigned when creating transfers
with [`Transfer.flags.imported`](../transfer.md#flagsimported) set.

### `imported_event_timestamp_out_of_range`

This result only applies when [`Transfer.flags.imported`](../transfer.md#flagsimported) is set.

The transfer was not created. The [`Transfer.timestamp`](../transfer.md#timestamp) is out of range,
but must be a user-defined timestamp greater than `0` and less than `2^63`.

### `imported_event_timestamp_must_not_advance`

This result only applies when [`Transfer.flags.imported`](../transfer.md#flagsimported) is set.

The transfer was not created. The user-defined [`Transfer.timestamp`](../transfer.md#timestamp) is
greater than the current [cluster time](../../coding/time.md), but it must be a past timestamp.

### `reserved_flag`

The transfer was not created. `Transfer.flags.reserved` is nonzero, but must be zero.

### `id_must_not_be_zero`

The transfer was not created. [`Transfer.id`](../transfer.md#id) is zero, which is a reserved value.

### `id_must_not_be_int_max`

The transfer was not created. [`Transfer.id`](../transfer.md#id) is `2^128 - 1`, which is a reserved
value.

### `exists_with_different_flags`

A transfer with the same `id` already exists, but with different [`flags`](../transfer.md#flags).

### `exists_with_different_pending_id`

A transfer with the same `id` already exists, but with a different
[`pending_id`](../transfer.md#pending_id).

### `exists_with_different_timeout`

A transfer with the same `id` already exists, but with a different
[`timeout`](../transfer.md#timeout).

### `exists_with_different_debit_account_id`

A transfer with the same `id` already exists, but with a different
[`debit_account_id`](../transfer.md#debit_account_id).

### `exists_with_different_credit_account_id`

A transfer with the same `id` already exists, but with a different
[`credit_account_id`](../transfer.md#credit_account_id).

### `exists_with_different_amount`

A transfer with the same `id` already exists, but with a different
[`amount`](../transfer.md#amount).

If the transfer has [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit) or
[`flags.balancing_credit`](../transfer.md#flagsbalancing_credit) set, then the actual amount
transferred exceeds this failed transfer's `amount`.

### `exists_with_different_user_data_128`

A transfer with the same `id` already exists, but with a different
[`user_data_128`](../transfer.md#user_data_128).

### `exists_with_different_user_data_64`

A transfer with the same `id` already exists, but with a different
[`user_data_64`](../transfer.md#user_data_64).

### `exists_with_different_user_data_32`

A transfer with the same `id` already exists, but with a different
[`user_data_32`](../transfer.md#user_data_32).

### `exists_with_different_ledger`

A transfer with the same `id` already exists, but with a different [`ledger`](../transfer.md#ledger).

### `exists_with_different_code`

A transfer with the same `id` already exists, but with a different [`code`](../transfer.md#code).

### `exists`

A transfer with the same `id` already exists.

If the transfer has [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit) or
[`flags.balancing_credit`](../transfer.md#flagsbalancing_credit) set, then the existing
transfer may have a different [`amount`](../transfer.md#amount), limited to the maximum
`amount` of the transfer in the request.

If the transfer has [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
set, then the existing transfer may have a different [`amount`](../transfer.md#amount):
- If the original posted amount was less than the pending amount,
  then the transfer amount must be equal to the posted amount.
- Otherwise, the transfer amount must be greater than or equal to the pending amount.

<details>
<summary>Client release &lt; 0.16.0</summary>

If the transfer has [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit) or
[`flags.balancing_credit`](../transfer.md#flagsbalancing_credit) set, then the existing
transfer may have a different [`amount`](../transfer.md#amount), limited to the maximum
`amount` of the transfer in the request.

</details>

Otherwise, with the possible exception of the `timestamp` field, the existing transfer is identical
to the transfer in the request.

To correctly [recover from application crashes](../../coding/reliable-transaction-submission.md),
many applications should handle `exists` exactly as [`ok`](#ok).

### `id_already_failed`

The transfer was not created. A previous transfer with the same [`id`](../transfer.md#id) failed
due to one of the following _transient errors_:

- [`debit_account_not_found`](#debit_account_not_found)
- [`credit_account_not_found`](#credit_account_not_found)
- [`pending_transfer_not_found`](#pending_transfer_not_found)
- [`exceeds_credits`](#exceeds_credits)
- [`exceeds_debits`](#exceeds_debits)
- [`debit_account_already_closed`](#debit_account_already_closed)
- [`credit_account_already_closed`](#credit_account_already_closed)

Transient errors depend on the database state at a given point in time, and each attempt
is uniquely associated with the corresponding [`Transfer.id`](../transfer.md#id).
This behavior guarantees that retrying a transfer will not produce a different outcome
(either success or failure).

Without this mechanism, a transfer that previously failed could succeed if retried when the
underlying state changes (e.g., the target account has sufficient credits).

**Note:** The application should retry an event only if it was unable to acknowledge the last
response (e.g., due to an application restart) or because it is correcting a previously rejected
malformed request (e.g., due to an application bug).
If the application intends to submit the transfer again even after a transient error, it must
generate a new [idempotency id](../../coding/data-modeling.md#id).

<details>
<summary>Client release &lt; 0.16.4</summary>

The [`id`](../transfer.md#id) is never checked against failed transfers, regardless of the error.
Therefore, a transfer that failed due to a transient error could succeed if retried later.

</details>

### `flags_are_mutually_exclusive`

The transfer was not created. A transfer cannot be created with the specified combination of
[`Transfer.flags`](../transfer.md#flags).

Flag compatibility (✓ = compatible, ✗ = mutually exclusive):

- [`flags.pending`](../transfer.md#flagspending)
  - ✗ [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
  - ✗ [`flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer)
  - ✓ [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit)
  - ✓ [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit)
  - ✓ [`flags.closing_debit`](../transfer.md#flagsclosing_debit)
  - ✓ [`flags.closing_credit`](../transfer.md#flagsclosing_credit)
  - ✓ [`flags.imported`](../transfer.md#flagsimported)
- [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
  - ✗ [`flags.pending`](../transfer.md#flagspending)
  - ✗ [`flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer)
  - ✗ [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit)
  - ✗ [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit)
  - ✗ [`flags.closing_debit`](../transfer.md#flagsclosing_debit)
  - ✗ [`flags.closing_credit`](../transfer.md#flagsclosing_credit)
  - ✓ [`flags.imported`](../transfer.md#flagsimported)
- [`flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer)
  - ✗ [`flags.pending`](../transfer.md#flagspending)
  - ✗ [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
  - ✗ [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit)
  - ✗ [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit)
  - ✗ [`flags.closing_debit`](../transfer.md#flagsclosing_debit)
  - ✗ [`flags.closing_credit`](../transfer.md#flagsclosing_credit)
  - ✓ [`flags.imported`](../transfer.md#flagsimported)
- [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit)
  - ✓ [`flags.pending`](../transfer.md#flagspending)
  - ✗ [`flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer)
  - ✗ [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
  - ✓ [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit)
  - ✓ [`flags.closing_debit`](../transfer.md#flagsclosing_debit)
  - ✓ [`flags.closing_credit`](../transfer.md#flagsclosing_credit)
  - ✓ [`flags.imported`](../transfer.md#flagsimported)
- [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit)
  - ✓ [`flags.pending`](../transfer.md#flagspending)
  - ✗ [`flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer)
  - ✗ [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
  - ✓ [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit)
  - ✓ [`flags.closing_debit`](../transfer.md#flagsclosing_debit)
  - ✓ [`flags.closing_credit`](../transfer.md#flagsclosing_credit)
  - ✓ [`flags.imported`](../transfer.md#flagsimported)
- [`flags.closing_debit`](../transfer.md#flagsclosing_debit)
  - ✓ [`flags.pending`](../transfer.md#flagspending)
  - ✗ [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
  - ✗ [`flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer)
  - ✓ [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit)
  - ✓ [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit)
  - ✓ [`flags.closing_credit`](../transfer.md#flagsclosing_credit)
  - ✓ [`flags.imported`](../transfer.md#flagsimported)
- [`flags.closing_credit`](../transfer.md#flagsclosing_credit)
  - ✓ [`flags.pending`](../transfer.md#flagspending)
  - ✗ [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
  - ✗ [`flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer)
  - ✓ [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit)
  - ✓ [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit)
  - ✓ [`flags.closing_debit`](../transfer.md#flagsclosing_debit)
  - ✓ [`flags.imported`](../transfer.md#flagsimported)
- [`flags.imported`](../transfer.md#flagsimported)
  - ✓ [`flags.pending`](../transfer.md#flagspending)
  - ✓ [`flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer)
  - ✓ [`flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer)
  - ✓ [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit)
  - ✓ [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit)
  - ✓ [`flags.closing_debit`](../transfer.md#flagsclosing_debit)
  - ✓ [`flags.closing_credit`](../transfer.md#flagsclosing_credit)

### `debit_account_id_must_not_be_zero`

The transfer was not created. [`Transfer.debit_account_id`](../transfer.md#debit_account_id) is
zero, but must be a valid account id.

### `debit_account_id_must_not_be_int_max`

The transfer was not created. [`Transfer.debit_account_id`](../transfer.md#debit_account_id) is
`2^128 - 1`, but must be a valid account id.

### `credit_account_id_must_not_be_zero`

The transfer was not created. [`Transfer.credit_account_id`](../transfer.md#credit_account_id) is
zero, but must be a valid account id.

### `credit_account_id_must_not_be_int_max`

The transfer was not created. [`Transfer.credit_account_id`](../transfer.md#credit_account_id) is
`2^128 - 1`, but must be a valid account id.

### `accounts_must_be_different`

The transfer was not created. [`Transfer.debit_account_id`](../transfer.md#debit_account_id) and
[`Transfer.credit_account_id`](../transfer.md#credit_account_id) must not be equal.

That is, an account cannot transfer money to itself.

### `pending_id_must_be_zero`

The transfer was not created. Only post/void transfers can reference a pending transfer.

Either:

- [`Transfer.flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer) must be set,
  or
- [`Transfer.flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer) must be set,
  or
- [`Transfer.pending_id`](../transfer.md#pending_id) must be zero.

### `pending_id_must_not_be_zero`

The transfer was not created.
[`Transfer.flags.post_pending_transfer`](../transfer.md#flagspost_pending_transfer) or
[`Transfer.flags.void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer) is set, but
[`Transfer.pending_id`](../transfer.md#pending_id) is zero. A posting or voiding transfer must
reference a [`pending`](../transfer.md#flagspending) transfer.

### `pending_id_must_not_be_int_max`

The transfer was not created. [`Transfer.pending_id`](../transfer.md#pending_id) is `2^128 - 1`,
which is a reserved value.

### `pending_id_must_be_different`

The transfer was not created. [`Transfer.pending_id`](../transfer.md#pending_id) is set to the same
id as [`Transfer.id`](../transfer.md#id). Instead it should refer to a different (existing)
transfer.

### `timeout_reserved_for_pending_transfer`

The transfer was not created. [`Transfer.timeout`](../transfer.md#timeout) is nonzero, but only
[pending](../transfer.md#flagspending) transfers have nonzero timeouts.

### `closing_transfer_must_be_pending`

The transfer was not created. [`Transfer.flags.pending`](../transfer.md#flagspending) is not set,
but closing transfers must be two-phase pending transfers.

If either [`Transfer.flags.closing_debit`](../transfer.md#flagsclosing_debit) or
[`Transfer.flags.closing_credit`](../transfer.md#flagsclosing_credit) is set,
[`Transfer.flags.pending`](../transfer.md#flagspending) must also be set.

This ensures that closing transfers are reversible by
[voiding](../transfer.md#flagsvoid_pending_transfer) the pending transfer, and requires that the
reversal operation references the corresponding closing transfer, guarding against unexpected
interleaving of close/unclose operations.

### `amount_must_not_be_zero`

**Deprecated**: This error code is only returned to clients prior to release `0.16.0`.
Since `0.16.0`, zero-amount transfers are permitted.

<details>
<summary>Client release &lt; 0.16.0</summary>

The transfer was not created. [`Transfer.amount`](../transfer.md#amount) is zero, but must be
nonzero.

Every transfer must move value. Only posting and voiding transfer amounts may be zero — when zero,
they will move the full pending amount.

</details>

### `ledger_must_not_be_zero`

The transfer was not created. [`Transfer.ledger`](../transfer.md#ledger) is zero, but must be
nonzero.

### `code_must_not_be_zero`

The transfer was not created. [`Transfer.code`](../transfer.md#code) is zero, but must be nonzero.

### `debit_account_not_found`

The transfer was not created. [`Transfer.debit_account_id`](../transfer.md#debit_account_id) must
refer to an existing `Account`.

This is a [transient error](#id_already_failed).
The [`Transfer.id`](../transfer.md#id) associated with this particular attempt will always fail
upon retry, even if the underlying issue is resolved.
To succeed, a new [idempotency id](../../coding/data-modeling.md#id) must be submitted.

### `credit_account_not_found`

The transfer was not created. [`Transfer.credit_account_id`](../transfer.md#credit_account_id) must
refer to an existing `Account`.

This is a [transient error](#id_already_failed).
The [`Transfer.id`](../transfer.md#id) associated with this particular attempt will always fail
upon retry, even if the underlying issue is resolved.
To succeed, a new [idempotency id](../../coding/data-modeling.md#id) must be submitted.

### `accounts_must_have_the_same_ledger`

The transfer was not created. The accounts referred to by
[`Transfer.debit_account_id`](../transfer.md#debit_account_id) and
[`Transfer.credit_account_id`](../transfer.md#credit_account_id) must have an identical
[`ledger`](../account.md#ledger).

[Currency exchange](../../coding/recipes/currency-exchange.md) is implemented with multiple
transfers.

### `transfer_must_have_the_same_ledger_as_accounts`

The transfer was not created. The accounts referred to by
[`Transfer.debit_account_id`](../transfer.md#debit_account_id) and
[`Transfer.credit_account_id`](../transfer.md#credit_account_id) are equivalent, but differ from the
[`Transfer.ledger`](../transfer.md#ledger).

### `pending_transfer_not_found`

The transfer was not created. The transfer referenced by
[`Transfer.pending_id`](../transfer.md#pending_id) does not exist.

This is a [transient error](#id_already_failed).
The [`Transfer.id`](../transfer.md#id) associated with this particular attempt will always fail
upon retry, even if the underlying issue is resolved.
To succeed, a new [idempotency id](../../coding/data-modeling.md#id) must be submitted.

### `pending_transfer_not_pending`

The transfer was not created. The transfer referenced by
[`Transfer.pending_id`](../transfer.md#pending_id) exists, but does not have
[`flags.pending`](../transfer.md#flagspending) set.

### `pending_transfer_has_different_debit_account_id`

The transfer was not created. The transfer referenced by
[`Transfer.pending_id`](../transfer.md#pending_id) exists, but with a different
[`debit_account_id`](../transfer.md#debit_account_id).

The post/void transfer's `debit_account_id` must either be `0` or identical to the pending
transfer's `debit_account_id`.

### `pending_transfer_has_different_credit_account_id`

The transfer was not created. The transfer referenced by
[`Transfer.pending_id`](../transfer.md#pending_id) exists, but with a different
[`credit_account_id`](../transfer.md#credit_account_id).

The post/void transfer's `credit_account_id` must either be `0` or identical to the pending
transfer's `credit_account_id`.

### `pending_transfer_has_different_ledger`

The transfer was not created. The transfer referenced by
[`Transfer.pending_id`](../transfer.md#pending_id) exists, but with a different
[`ledger`](../transfer.md#ledger).

The post/void transfer's `ledger` must either be `0` or identical to the pending transfer's
`ledger`.

### `pending_transfer_has_different_code`

The transfer was not created. The transfer referenced by
[`Transfer.pending_id`](../transfer.md#pending_id) exists, but with a different
[`code`](../transfer.md#code).

The post/void transfer's `code` must either be `0` or identical to the pending transfer's `code`.

### `exceeds_pending_transfer_amount`

The transfer was not created. The transfer's [`amount`](../transfer.md#amount) exceeds the `amount`
of its [pending](../transfer.md#pending_id) transfer.

### `pending_transfer_has_different_amount`

The transfer was not created. The transfer is attempting to
[void](../transfer.md#flagsvoid_pending_transfer) a pending transfer. The voiding transfer's
[`amount`](../transfer.md#amount) must be either `0` or exactly the `amount` of the pending
transfer.

To partially void a transfer, create a [posting transfer](../transfer.md#flagspost_pending_transfer)
with an amount less than the pending transfer's `amount`.

<details>
<summary>Client release &lt; 0.16.0</summary>

To partially void a transfer, create a [posting transfer](../transfer.md#flagspost_pending_transfer)
with an amount between `0` and the pending transfer's `amount`.

</details>

### `pending_transfer_already_posted`

The transfer was not created. The referenced [pending](../transfer.md#pending_id) transfer was
already posted by a [`post_pending_transfer`](../transfer.md#flagspost_pending_transfer).

### `pending_transfer_already_voided`

The transfer was not created. The referenced [pending](../transfer.md#pending_id) transfer was
already voided by a [`void_pending_transfer`](../transfer.md#flagsvoid_pending_transfer).

### `pending_transfer_expired`

The transfer was not created. The referenced [pending](../transfer.md#pending_id) transfer was
already voided because its [timeout](../transfer.md#timeout) has passed.

### `imported_event_timestamp_must_not_regress`

This result only applies when [`Transfer.flags.imported`](../transfer.md#flagsimported) is set.

The transfer was not created. The user-defined [`Transfer.timestamp`](../transfer.md#timestamp)
regressed, but it must be greater than the last timestamp assigned to any `Transfer` in the cluster and cannot be equal to the timestamp of any existing [`Account`](../account.md).

### `imported_event_timestamp_must_postdate_debit_account`

This result only applies when [`Transfer.flags.imported`](../transfer.md#flagsimported) is set.

The transfer was not created. [`Transfer.debit_account_id`](../transfer.md#debit_account_id) must
refer to an `Account` whose [`timestamp`](../account.md#timestamp) is less than the
[`Transfer.timestamp`](../transfer.md#timestamp).

### `imported_event_timestamp_must_postdate_credit_account`

This result only applies when [`Transfer.flags.imported`](../transfer.md#flagsimported) is set.

The transfer was not created. [`Transfer.credit_account_id`](../transfer.md#credit_account_id) must
refer to an `Account` whose [`timestamp`](../account.md#timestamp) is less than the
[`Transfer.timestamp`](../transfer.md#timestamp).

### `imported_event_timeout_must_be_zero`

This result only applies when [`Transfer.flags.imported`](../transfer.md#flagsimported) is set.

The transfer was not created. The [`Transfer.timeout`](../transfer.md#timeout) is nonzero, but
must be zero.

It's possible to import [pending](../transfer.md#flagspending) transfers with a user-defined
timestamp, but since it's not driven by the cluster clock, it cannot define a timeout for
automatic expiration.
In those cases, the [two-phase post or rollback](../../coding/two-phase-transfers.md) must be
done manually.

### `debit_account_already_closed`

The transfer was not created. [`Transfer.debit_account_id`](../transfer.md#debit_account_id) must
refer to an `Account` whose [`Account.flags.closed`](../account.md#flagsclosed) is not already set.

This is a [transient error](#id_already_failed).
The [`Transfer.id`](../transfer.md#id) associated with this particular attempt will always fail
upon retry, even if the underlying issue is resolved.
To succeed, a new [idempotency id](../../coding/data-modeling.md#id) must be submitted.

### `credit_account_already_closed`

The transfer was not created. [`Transfer.credit_account_id`](../transfer.md#credit_account_id) must
refer to an `Account` whose [`Account.flags.closed`](../account.md#flagsclosed) is not already set.

This is a [transient error](#id_already_failed).
The [`Transfer.id`](../transfer.md#id) associated with this particular attempt will always fail
upon retry, even if the underlying issue is resolved.
To succeed, a new [idempotency id](../../coding/data-modeling.md#id) must be submitted.

### `overflows_debits_pending`

The transfer was not created. `debit_account.debits_pending + transfer.amount` would overflow a
128-bit unsigned integer.

### `overflows_credits_pending`

The transfer was not created. `credit_account.credits_pending + transfer.amount` would overflow a
128-bit unsigned integer.

### `overflows_debits_posted`

The transfer was not created. `debit_account.debits_posted + transfer.amount` would overflow a
128-bit unsigned integer.

### `overflows_credits_posted`

The transfer was not created. `debit_account.credits_posted + transfer.amount` would overflow a
128-bit unsigned integer.

### `overflows_debits`

The transfer was not created.
`debit_account.debits_pending + debit_account.debits_posted + transfer.amount` would overflow a
128-bit unsigned integer.

### `overflows_credits`

The transfer was not created.
`credit_account.credits_pending + credit_account.credits_posted + transfer.amount` would overflow a
128-bit unsigned integer.

### `overflows_timeout`

The transfer was not created. `transfer.timestamp + (transfer.timeout * 1_000_000_000)` would
exceed `2^63`.

[`Transfer.timeout`](../transfer.md#timeout) is converted to nanoseconds.

This computation uses the [`Transfer.timestamp`](../transfer.md#timestamp) value assigned by the
replica, not the `0` value sent by the client.

### `exceeds_credits`

The transfer was not created.

The [debit account](../transfer.md#debit_account_id) has
[`flags.debits_must_not_exceed_credits`](../account.md#flagsdebits_must_not_exceed_credits) set, but
`debit_account.debits_pending + debit_account.debits_posted + transfer.amount` would exceed
`debit_account.credits_posted`.

This is a [transient error](#id_already_failed).
The [`Transfer.id`](../transfer.md#id) associated with this particular attempt will always fail
upon retry, even if the underlying issue is resolved.
To succeed, a new [idempotency id](../../coding/data-modeling.md#id) must be submitted.

<details>
<summary>Client release &lt; 0.16.0</summary>

If [`flags.balancing_debit`](../transfer.md#flagsbalancing_debit) is set, then
`debit_account.debits_pending + debit_account.debits_posted + 1` would exceed
`debit_account.credits_posted`.

</details>

### `exceeds_debits`

The transfer was not created.

The [credit account](../transfer.md#credit_account_id) has
[`flags.credits_must_not_exceed_debits`](../account.md#flagscredits_must_not_exceed_debits) set, but
`credit_account.credits_pending + credit_account.credits_posted + transfer.amount` would exceed
`credit_account.debits_posted`.

This is a [transient error](#id_already_failed).
The [`Transfer.id`](../transfer.md#id) associated with this particular attempt will always fail
upon retry, even if the underlying issue is resolved.
To succeed, a new [idempotency id](../../coding/data-modeling.md#id) must be submitted.

<details>
<summary>Client release &lt; 0.16.0</summary>

If [`flags.balancing_credit`](../transfer.md#flagsbalancing_credit) is set, then
`credit_account.credits_pending + credit_account.credits_posted + 1` would exceed
`credit_account.debits_posted`.

</details>

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#create-transfers)
- [Java library](/src/clients/java/README.md#create-transfers)
- [Go library](/src/clients/go/README.md#create-transfers)
- [Node.js library](/src/clients/node/README.md#create-transfers)
- [Python library](/src/clients/python/README.md#create-transfers)

## Internals

If you're curious and want to learn more, you can find the source code for creating a transfer in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig).
Search for `fn create_transfer(` and `fn execute(`.
