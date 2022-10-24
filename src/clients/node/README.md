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
import { createClient } from 'tigerbeetle-node'

const client = createClient({
  cluster_id: 0,
  replica_addresses: ['3001', '3002', '3003']
})
```

One of the ways TigerBeetle achieves its performance is through batching. 
This is reflected in the below function interfaces where each one takes in an array of events.

### Account Creation

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
}

const errors = await client.createAccounts([account])
```
Successfully executed events return an empty array whilst unsuccessful ones return an array with errors for **only the ones that failed**. An error will point to the index in the submitted array of the failed event.
```js
  const errors = await client.createAccounts([account1, account2, account3])

  // errors = [{ index: 1, code: 1 }]
  const error = errors[0]
  switch (error.code) {
    case CreateAccountError.exists: {
      console.error(`Batch event at ${error.index} already exists.`)
    }
  }
```
The example above shows that the event in index 1 failed with error 1. This means that `account1` and `account3` were created successfully but not `account2`.

The `flags` on an account provide a way for you to enforce policies by toggling the bits below.
| bit 0    | bit 1                            | bit 2                                   |
|----------|----------------------------------|-----------------------------------------|
| `linked` | `debits_must_not_exceed_credits` | `credits_must_not_exceed_debits` |

The creation of an account can be linked to the successful creation of another by setting the `linked` flag (see [linked events](#linked-events)). By setting `debits_must_not_exceed_credits`, then any transfer such that `debits_posted + debits_pending + amount > credits_posted` will fail. Similarly for `credits_must_not_exceed_debits`.
```js
  enum CreateAccountFlags {
    linked = (1 << 0),
    debits_must_not_exceed_credits = (1 << 1),
    credits_must_not_exceed_debits = (1 << 2)
  }

  let flags = 0
  flags |= CreateAccountFlags.debits_must_not_exceed_credits
```

### Account Lookup

The `id` of the account is used for lookups. Only matched accounts are returned.
```js
  // account 137n exists, 138n does not
  const accounts = await client.lookupAccounts([137n, 138n])

  /**
   * const accounts = [{
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

### Creating a Transfer

This creates a journal entry between two accounts.
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
}
const errors = await client.createTransfers([transfer])
```
Two-phase transfers are supported natively by toggling the appropriate flag. TigerBeetle will then adjust the `credits_pending` and `debits_pending` fields of the appropriate accounts. A corresponding commit transfer then needs to be sent to accept or reject the transfer.

Transfers within a batch may also be linked (see [linked events](#linked-events)).
```js
  enum TransferFlags {
    linked = (1 << 0),
    pending = (1 << 1),
    post_pending_transfer = (1 << 2),
    void_pending_transfer = (1 << 3)
  }
  
  // Two-phase transfer (pending):
  let flags = 0n
  flags |= TransferFlags.pending

  // Linked two-phase transfer (pending):
  let flags = 0n
  flags |= TransferFlags.linked
  flags |= TransferFlags.pending
```

### Post a Pending Transfer (2-Phase)

With `flags = post_pending_transfer`, TigerBeetle will accept the transfer. TigerBeetle will atomically rollback the changes to `debits_pending` and `credits_pending` of the appropriate accounts and apply them to the `debits_posted` and `credits_posted` balances.
```js
const post = {
    id: 2n, // u128, must correspond to the transfer id
    pending_id: 1n, // u128, id of the pending transfer
    flags: TransferFlags.post_pending_transfer, // to void, use [void_pending_transfer]
    timestamp: 0n, // u64, Reserved: This will be set by the server.
}
const errors = await client.createTransfers([post])
```

### Linked Events

When the `linked` flag is specified for the `createAccount`, `createTransfer`, `commitTransfer` event, it links an event with the next event in the batch, to create a chain of events, of arbitrary length, which all succeed or fail together. The tail of a chain is denoted by the first event without this flag. The last event in a batch may therefore never have the `linked` flag set as this would leave a chain open-ended. Multiple chains or individual events may coexist within a batch to succeed or fail independently. Events within a chain are executed within order, or are rolled back on error, so that the effect of each event in the chain is visible to the next, and so that the chain is either visible or invisible as a unit to subsequent events after the chain. The event that was the first to break the chain will have a unique error result. Other events in the chain will have their error result set to `linked_event_failed`.

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
