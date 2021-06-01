# tigerbeetle-node
[TigerBeetle](https://github.com/coilhq/tigerbeetle) client for NodeJS

**Note:** We will be making breaking changes to our data types in the next few days. 

## Installation
**Prerequisites:** The current version of the client reuses components from TigerBeetle. As such it targets Linux kernel v5.6 or newer. Node >= 14.0.0 is also required.

Later portable versions of TigerBeetle may supplement `io_uring` with `kqueue` for macOS and FreeBSD support, or `IOCP` for Windows support.

```sh
yarn add tigerbeetle-node
```
or
```sh
npm install tigerbeetle-node
```

**Development**

Follow these steps to get up and running when cloning the repo:

```sh
git clone --recurse-submodules https://github.com/coilhq/tigerbeetle-node.git
yarn
```

## Usage
A client needs to be configured with a `cluster_id` and `replica_addresses`. This instantiates the client where memory is allocated to internally buffer commands to be sent. For the moment, only one client can be instantiated globally per process. Future releases will allow multiple client instantiations.
```js
import { createClient } from 'tigerbeetle-node'

const client = createClient({
  cluster_id: 0x0a5ca1ab1ebee11en,
  replica_addresses: ['3001', '3002', '3003']
})
```

One of the ways TigerBeetle achieves its performance is through batching. This is reflected in the below function interfaces where each one takes in an array of commands.

### Account Creation

```js
const account = {
    id: 137n, // u64
    reserved: Buffer.alloc(48, 0), // [48]u8
    user_data: 0n,
    code: 718, // u16, a chart of accounts code describing the type of account (e.g. clearing, settlement)
    unit: 1,   // u16, unit of value
    flags: 0,  // u32
    credits_accepted: 0n, // u64
    credits_reserved: 0n, // u64
    debits_accepted: 0n,  // u64
    debits_reserved: 0n,  // u64
    timestamp: 0n, // u64, Reserved: This will be set by the server.
}

const results = await client.createAccounts([account])
```
Successfully executed commands return an empty array whilst unsuccessful ones return an array with errors for **only the ones that failed**. An error will point to the index in the submitted array of the failed command.
```js
  const results = await client.createAccounts([account1, account2, account3])

  // Successful result
  // results = []
  //
  // Unsuccessful result
  // results = [{ index: 1, error: 1 }]
  const { error } = results[0]
  switch (error) {
    case CreateAccountError.exists: {

    }
  }
```
The unsuccessful result above shows that the command in index 1 failed with error 1. This means that `account1` and `account3` were created successfully but not `account2`.

The `flags` on an account provide a way for you to enforce policies by toggling the bits below.
| bit 0  | bit 1                          | bit 2                                 |
|--------|--------------------------------|---------------------------------------|
| linked | debits_must_not_exceed_credits | credits_must_not_exceed_debits-commit |

The creation of an account can be linked to the successful creation of another by setting the `linked` flag (see [linked events](#linked-events)). By setting `debits_must_not_exceed_credits`, then any transfer such that `debits_accepted + debits_reserved + amount > credit_accepted` will fail. Similarly for `credits_must_not_exceed_debits`.
```js
  enum CreateAccountFlags {
    linked = (1 << 0),
    debits_must_not_exceed_credits = (1 << 1),
    credits_must_not_exceed_debits = (1 << 2)
  }

  let flags = 0
  flags |= CreateAccountFlags.debits_must_not_exceed_credits
```

### Account lookup

The `id` of the account is used for lookups. Only matched accounts are returned.
```js
  // account 137n exists, 138n does not
  const results = await client.lookupAccounts([137n, 138n])

  /**
   * const results = [{
   *   id: 137n,
   *   reserved: Buffer,
   *   user_data: 0n,
   *   code: 718,
   *   unit: 1,
   *   flags: 0,
   *   credits_accepted: 0n,
   *   credits_reserved: 0n,
   *   debits_accepted: 0n,
   *   debits_reserved: 0n,
   * }]
   */
```

### Creating a transfer

This creates a journal entry between two accounts.
```js
const transfer = {
    id: 1n, // u64
    debit_account_id: 1n,  // u64
    credit_account_id: 2n, // u64
    user_data: 0n, // u128, opaque third-party identifier to link this transfer (many-to-one) to an external entity 
    reserved: Buffer.alloc(32, 0), // two-phase condition can go in here
    timeout: 0n, // u64, in nano-seconds. 
    code: 1,  // u32, a chart of accounts code describing the reason for the transfer (e.g. deposit, settlement)
    flags: 0, // u32
    amount: 10n, // u64
}

const results = await client.createTransfers([transfer])
```
Two-phase transfers are supported natively by toggling the appropriate flag. TigerBeetle will then adjust the `credits_reserved` and `debits_reserved` fields of the appropriate accounts. A corresponding commit transfer then needs to be sent to accept or reject the transfer.
| bit 0  | bit 1            | bit 2            |
|--------|------------------|------------------|
| linked | two_phase_commit | condition-commit |

The `condition-commit` flag signals to TigerBeetle that a 256-bit cryptographic condition will be supplied in the `reserved` field. This will be validated against a supplied pre-image when the transfer is committed. Transfers within a batch may also be linked (see [linked events](#linked-events)).
```js
  enum CreateTransferFlags {
    linked = (1 << 0>>),
    two_phase_commit = (1 << 1),
    condition = (1 << 2)
  }

// two-phase transfer
  let flags = 0n
  flags |= TransferFlags.two_phase_commit

  // two-phase transfer with condition supplied in `reserved`
  let flags = 0n
  flags |= TransferFlags.two_phase_commit
  flags |= TransferFlags.condition
```

### Committing a transfer

This is used to commit a two-phase transfer.
| bit 0  | bit 1  | bit 2    |
|--------|--------|----------|
| linked | reject | preimage |

By default (`flags = 0`), it will accept the transfer. TigerBeetle will atomically rollback the changes to `debit_reserved` and `credit_reserved` of the appropriate accounts and apply them to the `debit_accepted` and `credit_accepted` balances. If the `preimage` bit is set then TigerBeetle will look for it in the `reserved` field and validate it against the `condition` from the associated transfer. If this validation fails, or `reject` is set, then the changes to the `reserved` balances are atomically rolled back.
```js
let flags = 0n
flags |= CommitFlags.reject
const commit = {
    id: 1n,   // must correspond to the transfer id
    reserved: Buffer.alloc(32, 0),
    code: 1,  // accounting system code to identify type of transfer
    flags,
}
const results = await client.commitTransfers([commit])
```

### Linked events

When the `linked` flag is specified for the `createAccount`, `createTransfer`, `commitTransfer` commands, it links an event with the next event in the batch, to create a chain of events, of arbitrary length, which all succeed or fail together. The tail of a chain is denoted by the first event without this flag. The last event in a batch may therefore never have the `linked` flag set as this would leave a chain open-ended. Multiple chains or individual events may coexist within a batch to succeed or fail independently. Events within a chain are executed within order, or are rolled back on error, so that the effect of each event in the chain is visible to the next, and so that the chain is either visible or invisible as a unit to subsequent events after the chain. The event that was the first to break the chain will have a unique error result. Other events in the chain will have their error result set to `linked_event_failed`.

```js
let batch = []
let linkedFlag = 0
linkedFlag |= CreateTransferFlags.linked

// An individual transfer (successful)
batch.push({ id: 1n, ... })

// A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false)
batch.push({ id: 2n, ..., flags: linkedFlag }) // Commit/rollback.
batch.push({ id: 3n, ..., flags: linkedFlag }) // Commit/rollback.
batch.push({ id: 2n, ..., flags: linkedFlag }) // Fail with exists
batch.push({ id: 4n, ..., flags: 0 })          // Fail without committing.

// An individual transfer (successful):
// This should not see any effect from the failed chain above.
batch.push({ id: 2n, ..., flags: 0 })

// A chain of 2 transfers (the first transfer fails the chain)
batch.push({ id: 2n, ..., flags: linkedFlag })
batch.push({ id: 3n, ..., flags: 0 })

// A chain of 2 transfers (successful)
batch.push({ id: 3n, ..., flags: linkedFlag })
batch.push({ id: 4n, ..., flags: 0 })

// Results
const results = await client.createTransfers(batch)

/**
 * [
 *  { index: 1, error: 1 }, // linked_event_failed
 *  { index: 2, error: 1 }, // linked_event_failed
 *  { index: 3, error: 2 }, // exists
 *  { index: 4, error: 1 }, // linked_event_failed
 * 
 *  { index: 6, error: 7 }, // exists_with_different_flags
 *  { index: 7, error: 1 }, // linked_event_failed
 * ]
 */
```