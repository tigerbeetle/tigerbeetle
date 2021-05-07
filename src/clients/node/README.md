# tigerbeetle-node
TigerBeetle client for NodeJS

## Installation
**Prerequisites:** The current version of the client reuses components from TigerBeetle. As such it targets Linux kernel v5.6 or newer. Node >= 14.0.0 is also required.

Later portable versions of TigerBeetle may supplement `io_uring` with `kqueue` for macOS and FreeBSD support, or `IOCP` for Windows support.

```sh
yarn install tigerbeetle-node
```

## Usage
A client needs to be configured with a `client_id`, `cluster_id` and `replica_addresses`. This instantiates the client where memory is allocated to internally buffer commands to be sent. For the moment, only one client can be instantiated globally per process. Future releases will allow multiple client instantiations.
```js
import { createClient } from 'tigerbeetle-node'

const client = createClient({
  client_id: 123n,
  cluster_id: 0x1ee1be1eaba15c0an,
  replica_addresses: ['3001', '3002', '3003']
})
```

One of the ways TigerBeetle achieves its performance is through batching. This is reflected in the below function interfaces where each one takes in an array of commands.

**Account Creation**

```js
const account = {
    id: 137n,
    custom: 0n,
    flags: 0n,
    unit: 1n,
    debit_accepted: 0n,
    debit_reserved: 0n,
    credit_accepted: 0n,
    credit_reserved: 0n,
    debit_accepted_limit: 10000000n,
    debit_reserved_limit: 10000000n,
    credit_accepted_limit: 10000000n,
    credit_reserved_limit: 10000000n,
    timeout: 1n,
}

const result = await client.createAccount([account])
```
Successfully executed commands return an empty array whilst unsuccessful ones return an array with errors for **only the ones that failed**. An error will point to the index in the submitted array of the failed command.
```js
  const result = await client.createAccount([account1, account2, account3])

  // Successful result
  // result = []
  //
  // Unsuccessful result
  // result = [{ index: 1, error: 1 }]
```
The unsuccessful result above shows that the command in index 1 failed with error 1. This means that `account1` and `account3` were created successfully but not `account2`.

**Creating a transfer**
```js
const transfer = {
    id: 1n,
    debit_account_id: 1n,
    credit_account_id: 2n,
    custom_1: 0n,
    custom_2: 0n,
    custom_3: 0n,
    flags: 0n,
    amount: 10n,
    timeout: 10000000000n // in nano-seconds
}

const result = await client.createTransfer([transfer])
```
The `flags` field is a bit field that determines the flavour of transfer you want to create.
| bit 0  | bit 1  | bit 2       |
|--------|--------|-------------|
| accept | reject | auto-commit |

By default (`flags = 0n`), a two-phase transfer is created i.e. after the transfer is created you will need to send in a commit command to accept/reject it.

An `auto-committing` transfer can also be created where a subsequent commit is not necessary by setting bit 0 and bit 2. This would be akin to creating a journal entry. Similarly, if you want the transfer to be automatically rejected then set bit 1 and bit 2.
```js
  // auto-committing transfer
  let flags = 0n
  flags |= (1 << 0)
  flags |= (1 << 2)

  // auto-rejected transfer
  let flags = 0n
  flags |= (1 << 1)
  flags |= (1 << 2)
```
Future releases will support supplying TigerBeetle with a condition that will be used to validate a corresponding fulfillment for two-phase transfers.

**Committing a transfer**
The flags field will determine the type of commit viz. accept or reject.
| bit 0  | bit 1  |
|--------|--------|
| accept | reject |
```js
const commit = {
    id: 136n,
    custom_1: 0n,
    custom_2: 0n,
    custom_3: 0n,
    flags: 1n,
}
const result = await client.commitTransfer([commit])
```
