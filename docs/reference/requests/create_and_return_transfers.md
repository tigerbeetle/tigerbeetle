# `create_and_return_transfers`

Create one or more [`Transfer`](../transfer.md)s and return the outcome of the operation.

A successfully created transfer will modify the
amount fields of its [debit](../transfer.md#debit_account_id) and
[credit](../transfer.md#credit_account_id) accounts.

## Event

The transfer to create. See [`Transfer`](../transfer.md) for constraints.

## Result

The outcome of the operation. Contains information about the transfer
and the balance of the accounts depending on the [result](#result).

### `result`

Indicates whether the transfer was successfully created or not.

See the complete list of [`result`s](./create_transfers.md#result).

Constraints:

- Type is 32-bit unsigned integer (4 bytes).

### `flags`

Indicates which information may be included in the outcome.

Constraints:

- Type is 32-bit unsigned integer (4 bytes).

#### `flags.transfer_set`

When set, the transfer's [`timestamp`](../transfer.md#timestamp)
and [`amount`](../transfer.md#amount) are returned.

#### `flags.account_balances_set`

When set, the [debit account](../transfer.md#debit_account_id)
and [credit account](../transfer.md#credit_account_id) balance amounts are returned.

### `timestamp`

The transfer timestamp.

Check [`flags.transfer_set`](#flagstransfer_set) to verify if this field is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with result [`ok`](./create_transfers.md#ok)
or when the transfer already exists (e.g., result [`exists`](./create_transfers.md#exists)).
  Zero otherwise.

### `amount`

The transfer amount.

Check [`flags.transfer_set`](#flagstransfer_set) to verify if this field is present in the result.

The actual transfer amount may differ from the requested amount in cases such as
[posting pending transfers](../transfer.md#flagspost_pending_transfer),
[balancing debits](../transfer.md#flagsbalancing_debit),
or [balancing credits](../transfer.md#flagsbalancing_credit).

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with result [`ok`](./create_transfers.md#ok)
or when the transfer already exists (e.g., result [`exists`](./create_transfers.md#exists)).
  Zero otherwise.

### `debit_account_debits_pending`

The [debit account](../transfer.md#debit_account_id)'s
[`debits_pending`](../account.md#debits_pending) balance.

Check [`flags.account_balances_set`](#flagsaccount_balances_set) to verify if this field
is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with the result
[`ok`](./create_transfers.md#ok) or when the transfer failed due to balance validation
(e.g., result [`exceeds_debits`](./create_transfers.md#exceeds_debits)
or [`exceeds_credits`](./create_transfers.md#exceeds_credits)).
  Zero otherwise.

### `debit_account_debits_posted`

The [debit account](../transfer.md#debit_account_id)'s
[`debits_posted`](../account.md#debits_posted) balance.

Check [`flags.account_balances_set`](#flagsaccount_balances_set) to verify if this field
is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with the result
[`ok`](./create_transfers.md#ok) or when the transfer failed due to balance validation
(e.g., result [`exceeds_debits`](./create_transfers.md#exceeds_debits)
or [`exceeds_credits`](./create_transfers.md#exceeds_credits)).
  Zero otherwise.

### `debit_account_credits_pending`

The [debit account](../transfer.md#debit_account_id)'s
[`credits_pending`](../account.md#credits_pending) balance.

Check [`flags.account_balances_set`](#flagsaccount_balances_set) to verify if this field
is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with the result
[`ok`](./create_transfers.md#ok) or when the transfer failed due to balance validation
(e.g., result [`exceeds_debits`](./create_transfers.md#exceeds_debits)
or [`exceeds_credits`](./create_transfers.md#exceeds_credits)).
  Zero otherwise.

### `debit_account_credits_posted`

The [debit account](../transfer.md#debit_account_id)'s
[`credits_posted`](../account.md#credits_posted) balance.

Check [`flags.account_balances_set`](#flagsaccount_balances_set) to verify if this field
is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with the result
[`ok`](./create_transfers.md#ok) or when the transfer failed due to balance validation
(e.g., result [`exceeds_debits`](./create_transfers.md#exceeds_debits)
or [`exceeds_credits`](./create_transfers.md#exceeds_credits)).
  Zero otherwise.

### `credit_account_debits_pending`

The [credit account](../transfer.md#credit_account_id)'s
[`debits_pending`](../account.md#debits_pending) balance.

Check [`flags.account_balances_set`](#flagsaccount_balances_set) to verify if this field
is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with the result
[`ok`](./create_transfers.md#ok) or when the transfer failed due to balance validation
(e.g., result [`exceeds_debits`](./create_transfers.md#exceeds_debits)
or [`exceeds_credits`](./create_transfers.md#exceeds_credits)).
  Zero otherwise.

### `credit_account_debits_posted`

The [credit account](../transfer.md#credit_account_id)'s
[`debits_posted`](../account.md#debits_posted) balance.

Check [`flags.account_balances_set`](#flagsaccount_balances_set) to verify if this field
is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with the result
[`ok`](./create_transfers.md#ok) or when the transfer failed due to balance validation
(e.g., result [`exceeds_debits`](./create_transfers.md#exceeds_debits)
or [`exceeds_credits`](./create_transfers.md#exceeds_credits)).
  Zero otherwise.

### `credit_account_credits_pending`

The [credit account](../transfer.md#credit_account_id)'s
[`credits_pending`](../account.md#credits_pending) balance.

Check [`flags.account_balances_set`](#flagsaccount_balances_set) to verify if this field
is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with the result
[`ok`](./create_transfers.md#ok) or when the transfer failed due to balance validation
(e.g., result [`exceeds_debits`](./create_transfers.md#exceeds_debits)
or [`exceeds_credits`](./create_transfers.md#exceeds_credits)).
  Zero otherwise.

### `credit_account_credits_posted`

The [credit account](../transfer.md#credit_account_id)'s
[`credits_posted`](../account.md#credits_posted) balance.

Check [`flags.account_balances_set`](#flagsaccount_balances_set) to verify if this field
is present in the result.

Constraints:

- Type is 128-bit unsigned integer (16 bytes).

- Always set when the transfer was successfully created with the result
[`ok`](./create_transfers.md#ok) or when the transfer failed due to balance validation
(e.g., result [`exceeds_debits`](./create_transfers.md#exceeds_debits)
or [`exceeds_credits`](./create_transfers.md#exceeds_credits)).
  Zero otherwise.

## Client libraries

For language-specific docs see:

- [.NET library](/src/clients/dotnet/README.md#create-and-return-transfers)
- [Java library](/src/clients/java/README.md#create-and-return-transfers)
- [Go library](/src/clients/go/README.md#create-and-return-transfers)
- [Node.js library](/src/clients/node/README.md#create-and-return-transfers)
- [Python library](/src/clients/python/README.md#create-and-return-transfers)
