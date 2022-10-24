# tigerbeetle-node
[TigerBeetle](https://github.com/tigerbeetledb/tigerbeetle) client for Node.js.

## Installation

Install the `tigerbeetle-node` module to your current working directory:

```shell
npm install tigerbeetle-node
```

If you run into issues, check out the distribution-specific install
steps that are run in CI to test support:

* [Alpine](./scripts/test_install_on_alpine.sh)
* [Amazon Linux](./scripts/test_install_on_amazonlinux.sh)
* [Debian](./scripts/test_install_on_debian.sh)
* [Fedora](./scripts/test_install_on_fedora.sh)
* [Ubuntu](./scripts/test_install_on_ubuntu.sh)
* [RHEL](./scripts/test_install_on_rhelubi.sh)

### Prerequisites

* NodeJS >= `14.0.0`. _(If the correct version is not installed, an installation error will occur)_

> Your operating system should be Linux (kernel >= v5.6) or macOS.
> Windows support is not yet available.

## Usage

A client needs to be configured with a `cluster_id` and `replica_addresses`. 
This instantiates the client where memory is allocated to internally buffer events to be sent. 
For the moment, only one client can be instantiated globally per process. 
Future releases will allow multiple client instantiations.

```js
const {
  createClient,
  CreateAccountError,
  CreateAccountFlags,
  CreateTransferFlags,
  CreateTransferError,
} = require('tigerbeetle-node');

const client = createClient({
  cluster_id: 0,
  replica_addresses: ['3001', '3002', '3003']
});
```

One of the ways TigerBeetle achieves its performance is through
batching. This is reflected in the below function interfaces where
each one takes in an array of items.

### Sidenote: `BigInt`

TigerBeetle uses 64-bit integers for many fields while JavaScript's
builtin `Number` maximum value is 2^53-1. The `n` suffix in JavaScript
means the value is a BigInt. This is useful for literal numbers. If
you already have a `Number` variable though, you can call the `BigInt`
constructor to get a `BigInt` from it. For example, `1n` is the same
as `BigInt(1)`.

## Creating Accounts: `client.createAccounts`

See details for account fields in the [Accounts
reference](https://docs.tigerbeetle.com/reference/accounts).

```js
const account = {
  id: 137n, // u128
  user_data: 0n, // u128, opaque third-party identifier to link this account to an external entity:
  reserved: Buffer.alloc(48, 0), // [48]u8
  ledger: 1,   // u32, ledger value
  code: 718, // u16, a chart of accounts code describing the type of account (e.g. clearing, settlement)
  flags: 0,  // u16
  debits_pending: 0n,  // u64
  debits_posted: 0n,  // u64
  credits_pending: 0n, // u64
  credits_posted: 0n, // u64
  timestamp: 0n, // u64, Reserved: This will be set by the server.
};

const errors = await client.createAccounts([account]);
if (errors.length) {
  // Grab a human-readable message from the response
  console.log(CreateAccountError[errors[0].code]);
}
```

### Account Flags

The account `flags` value is a bitfield. See details for these flags in
the [Accounts
reference](https://docs.tigerbeetle.com/reference/accounts#flags).

To toggle behavior for an account, combine enum values stored in the
CreateAccountFlags object (in TypeScript it is an actual enum) with
bitwise-or:

* `CreateAccountFlags.linked`
* `CreateAccountFlags.debits_must_not_exceed_credits`
* `CreateAccountFlags.credits_must_not_exceed_credits`

For example, to link `account0` and `account1`, where `account0`
additionally has the `debits_must_not_exceed_credits` constraint:

```js
const account0 = { ... account values ... };
const account1 = { ... account values ... };
account0.flags = CreateAccountFlags.linked | CreateAccountFlags.debits_must_not_exceed_credits;
// Create the account
const errors = client.createAccounts([account0, account1]);
```

### Response and Errors

The response is an empty array if all accounts were created
successfully. If the response is non-empty, each object in the
response array contains error information for an account that
failed. The error object contains an error code and the index of the
account in the request batch.

```js
const errors = await client.createAccounts([account1, account2, account3]);

// errors = [{ index: 1, code: 1 }];
for (const error of errors) {
  switch (error.code) {
    case CreateAccountError.exists:
      console.error(`Batch account at ${error.index} already exists.`);
	  break;
    default:
      console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.code]}.`);
  }
}
```

The example above shows that the account in index 1 failed with
error 1. This error here means that `account1` and `account3` were
created successfully. But `account2` was not created.

To handle errors you can either 1) exactly match error codes returned
from `client.createAccounts` with enum values in the
`CreateAccountError` object, or you can 2) look up the error code in
the `CreateAccountError` object for a human-readable string.

## Account Lookup: `client.lookupAccounts`

Account lookup is batched, like account creation. Pass in all `id`s to
fetch, and matched accounts are returned.

If no account matches an `id`, no object is returned for that
account. So the order of accounts in the response is not necessarily
the same as the order of `id`s in the request. You can refer to the
`id` field in the response to distinguish accounts.

```js
// account 137n exists, 138n does not
const accounts = await client.lookupAccounts([137n, 138n]);
/* console.log(accounts);
 * [{
 *   id: 137n,
 *   user_data: 0n,
 *   reserved: Buffer,
 *   ledger: 1,
 *   code: 718,
 *   flags: 0,
 *   debits_pending: 0n,
 *   debits_posted: 0n,
 *   credits_pending: 0n,
 *   credits_posted: 0n,
 *   timestamp: 1623062009212508993n,
 * }]
 */
```

## Creating Transfers: `client.createTransfers`

This creates a journal entry between two accounts.

See details for transfer fields in the [Transfers
reference](https://docs.tigerbeetle.com/reference/transfers).

```js
const transfer = {
  id: 1n, // u128
  pending_id: 0n, // u128
  // Double-entry accounting:
  debit_account_id: 1n,  // u128
  credit_account_id: 2n, // u128
  // Opaque third-party identifier to link this transfer to an external entity:
  user_data: 0n, // u128  
  reserved: 0n, // u128
  // Timeout applicable for a pending/2-phase transfer:
  timeout: 0n, // u64, in nano-seconds.
  // Collection of accounts usually grouped by the currency: 
  // You can't transfer money between accounts with different ledgers:
  ledger: 1,  // u32, ledger for transfer (e.g. currency).
  // Chart of accounts code describing the reason for the transfer:
  code: 720,  // u16, (e.g. deposit, settlement)
  flags: 0, // u16
  amount: 10n, // u64
  timestamp: 0n, //u64, Reserved: This will be set by the server.
};
const errors = await client.createTransfers([transfer]);
for (const error of errors) {
  switch (error.code) {
    default:
      console.error(`Batch transfer at ${error.index} failed to create: ${CreateAccountError[error.code]}.`);
  }
}
```

### Response and Errors

The response is an empty array if all transfers were created
successfully. If the response is non-empty, each object in the
response array contains error information for an transfer that
failed. The error object contains an error code and the index of the
transfer in the request batch.

```js
const errors = await client.createTransfers([transfer1, transfer2, transfer3]);

// errors = [{ index: 1, code: 1 }];
for (const error of errors) {
  switch (error.code) {
    case CreateTransferError.exists:
      console.error(`Batch transfer at ${error.index} already exists.`);
	  break;
    default:
      console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.code]}.`);
  }
}
```

The example above shows that the transfer in index 1 failed with
error 1. This error here means that `transfer1` and `transfer3` were
created successfully. But `transfer2` was not created.

To handle errors you can either 1) exactly match error codes returned
from `client.createTransfers` with enum values in the
`CreateTransferError` object, or you can 2) look up the error code in
the `CreateTransferError` object for a human-readable string.

### Batching

TigerBeetle performance is maximized when you batch inserts. The
client does not do this automatically for you. So, for example, you
can insert 1 million transfers one at a time like so:

```js
for (let i = 0; i < 1_000_000; i++) {
  const errors = client.createTransfers(transfers[i]);
  // error handling omitted
}
```

But the insert rate will be a fraction of potential. Instead, always
batch what you can. The maximum batch size is set in the TigerBeetle
server. The default is 8191.

```js
const BATCH_SIZE = 8191;
for (let i = 0; i < 1_000_000; i += BATCH_SIZE) {
  const errors = client.createTransfers(transfers.slice(i, Math.min(transfers.length, BATCH_SIZE)));
  // error handling omitted
}
```

### Transfer Flags

The transfer `flags` value is a bitfield. See details for these flags in
the [Transfers
reference](https://docs.tigerbeetle.com/reference/transfers#flags).

To toggle behavior for an transfer, combine enum values stored in the
`CreateTransferFlags` object (in TypeScript it is an actual enum) with
bitwise-or:

* `CreateTransferFlags.linked`
* `CreateTransferFlags.debits_must_not_exceed_credits`
* `CreateTransferFlags.credits_must_not_exceed_credits`

For example, to set the following `transfer0` as `linked` to
`transfer1` and that `transfer0` has the setting
`debits_must_not_exceed_credits`:

```js
const transfer0 = { ... transfer values ... };
const transfer1 = { ... transfer values ... };
transfer0.flags = CreateTransferFlags.linked | CreateTransferFlags.debits_must_not_exceed_credits;
// Create the transfer
const errors = client.createTransfers([transfer0, transfer1]);
```

#### Two-Phase Transfers

Two-phase transfers are supported natively by toggling the appropriate
flag. TigerBeetle will then adjust the `credits_pending` and
`debits_pending` fields of the appropriate accounts. A corresponding
post pending transfer then needs to be sent to post or void the
transfer.

##### Post a Pending Transfer

With `flags = post_pending_transfer`, TigerBeetle will post the
transfer. TigerBeetle will atomically rollback the changes to
`debits_pending` and `credits_pending` of the appropriate accounts and
apply them to the `debits_posted` and `credits_posted` balances.

```js
const post = {
    id: 2n, // u128, must correspond to the transfer id
    pending_id: 1n, // u128, id of the pending transfer
    flags: TransferFlags.post_pending_transfer, // to void, use [void_pending_transfer]
    timestamp: 0n, // u64, Reserved: This will be set by the server.
}
const errors = await client.createTransfers([post])
```

## Transfer Lookup: `client.lookupTransfers`

NOTE: While transfer lookup exists, it is not a flexible query API. We
are developing query APIs and there will be new methods for querying
transfers in the future.

Transfer lookup is batched, like transfer creation. Pass in all `id`s to
fetch, and matched transfers are returned.

If no transfer matches an `id`, no object is returned for that
transfer. So the order of transfers in the response is not necessarily
the same as the order of `id`s in the request. You can refer to the
`id` field in the response to distinguish transfers.

```js
// transfer 1n exists, 2n does not
const transfers = await client.lookupTransfers([1n, 2n]);
/* console.log(transfers);
 * [{
 *   id: 1n,
 *   pending_id: 0n,
 *   debit_account_id: 1n,
 *   credit_account_id: 2n,
 *   user_data: 0n,
 *   reserved: 0n,
 *   timeout: 0n,
 *   ledger: 1,
 *   code: 720,
 *   flags: 0,
 *   amount: 10n,
 *   timestamp: 1623062009212508993n,
 * }]
 */
```

## Linked Events

When the `linked` flag is specified for the `createAccount` or
`createTransfer` event, it links an event with the next event in the
batch, to create a chain of events, of arbitrary length, which all
succeed or fail together. The tail of a chain is denoted by the first
event without this flag. The last event in a batch may therefore never
have the `linked` flag set as this would leave a chain
open-ended. Multiple chains or individual events may coexist within a
batch to succeed or fail independently.

Events within a chain are executed within order, or are rolled back on
error, so that the effect of each event in the chain is visible to the
next, and so that the chain is either visible or invisible as a unit
to subsequent events after the chain. The event that was the first to
break the chain will have a unique error result. Other events in the
chain will have their error result set to `linked_event_failed`.

```js
let batch = []
let linkedFlag = 0
linkedFlag |= CreateTransferFlags.linked

// An individual transfer (successful):
batch.push({ id: 1n, ... })

// A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
batch.push({ id: 2n, ..., flags: linkedFlag }) // Commit/rollback.
batch.push({ id: 3n, ..., flags: linkedFlag }) // Commit/rollback.
batch.push({ id: 2n, ..., flags: linkedFlag }) // Fail with exists
batch.push({ id: 4n, ..., flags: 0 })          // Fail without committing.

// An individual transfer (successful):
// This should not see any effect from the failed chain above.
batch.push({ id: 2n, ..., flags: 0 })

// A chain of 2 transfers (the first transfer fails the chain):
batch.push({ id: 2n, ..., flags: linkedFlag })
batch.push({ id: 3n, ..., flags: 0 })

// A chain of 2 transfers (successful):
batch.push({ id: 3n, ..., flags: linkedFlag })
batch.push({ id: 4n, ..., flags: 0 })

const errors = await client.createTransfers(batch)

/**
 * [
 *  { index: 1, error: 1 },  // linked_event_failed
 *  { index: 2, error: 1 },  // linked_event_failed
 *  { index: 3, error: 25 }, // exists
 *  { index: 4, error: 1 },  // linked_event_failed
 * 
 *  { index: 6, error: 17 }, // exists_with_different_flags
 *  { index: 7, error: 1 },  // linked_event_failed
 * ]
 */
```

### Development

To get up and running when cloning the repo:

```shell
git clone --recurse-submodules https://github.com/tigerbeetledb/tigerbeetle-node.git
cd tigerbeetle-node/
npm install --include dev # This will automatically install and build everything you need.
```

#### Rebuild

To rebuild the TypeScript distribution, and to rebuild the native Node library, again after changes:

```shell
npm run build
```

*If you ever run `npm run clean` then you will need to `npm install --include dev` to reinstall
TypeScript within `node_modules`, as TypeScript is required by `npm run prepack` when publishing.*

#### Benchmark

```shell
npm run benchmark
```

#### Test

```shell
./tigerbeetle format --cluster=0 --replica=0 ./cluster_0_replica_0_test.tigerbeetle
./tigerbeetle start --addresses=3001 ./cluster_0_replica_0_test.tigerbeetle > tigerbeetle_test.log 2>&1
npm run test
```
For more information, type; `./tigerbeetle -h` 
