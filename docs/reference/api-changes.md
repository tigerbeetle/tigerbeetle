# API changes

## New return type for `create_accounts` and `create_transfers`.

TigerBeetle release `0.17.0` introduced a new API for handling the results when creating
[Accounts](./requests/create_accounts.md) and [Transfers](./requests/create_transfers.md).

This document provides guidelines for migrating applications that use TigerBeetle `0.16.x`
clients to the new API.

New TigerBeetle releases will continue to support old clients for an extended period,
giving application developers time to plan a smooth upgrade to new client library.

### Motivation

The previous API for [`create_accounts`](./requests/create_accounts.md) and
[`create_transfers`](./requests/create_transfers.md) reported outcomes only for failed events,
returning a _sparse_ array of results. Each result included the `index` of the failed element
within the events batch and the corresponding error code. Successfully created events were not
returned by the TigerBeetle cluster, and the application could safely assume them as `ok`.

While this approach prioritized saving network bandwidth by omitting results for the common
_happy path_, it didn’t provide enough information about the complete outcome of an event.

We’ve changed the result type of [`create_accounts`](./requests/create_accounts.md) and
[`create_transfers`](./requests/create_transfers.md) to report the status of every event,
including successfully created ones, along with the `timestamp` when each event was processed
by the TigerBeetle cluster.

Applications can now benefit from knowing the `timestamp` assigned to a successfully created
[`Account`](./account.md#timestamp) or [`Transfer`](./transfer.md#timestamp), while also handling
alternative paths more efficiently by knowing _when_ validation occurred for those that couldn’t
be created.

The enums `CreateAccountResult` and `CreateTransferResult` were renamed to `CreateAccountStatus`
and `CreateTransferStatus`, and the status code `ok` renamed as `created`.
This makes it explicit whether an event is part of the database state (`created` and `exists`) or
not. By removing the duality of _ok_ and _errors_, applications can handle results more clearly
according to their policies, since some outcomes might not be considered a failure by application
logic — for example, a `Transfer` not being created due to balance checks or expiry timeouts isn’t
necessarily an _error_.
Likewise, `exists` now reports the same `timestamp` returned by `created`, allowing the application
to treat both cases consistently.

Before:
```zig
pub const CreateTransfersResult = extern struct {
    index: u32,
    result: CreateTransferResult,
};
```

After:
```zig
pub const CreateTransferResult = extern struct {
    timestamp: u64,
    status: CreateTransferStatus,
    reserved: u32 = 0,
};
```

### Clients breaking changes

Along with the new result types, some client libraries have also changed the API to be more
idiomatic, for naming consistency, or even due to bug fixes in the previous API.

Below is the list of API changes specific to each client library:

#### Java Client

The way of handling the return values of `createAccounts`, `createAccountsAsync`,
`createTransfers`, and `createTransfersAsync` in the TigerBeetle Java Client has changed.

Before:
```java
CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
if (transferErrors.getLength() > 0) {
    // Error handling ...
}
```

After:
```java
CreateTransferResultBatch transferResults = client.createTransfers(transfers);
assert transferResults.getLength() == transfers.getLength();
while (transferResults.next()) {
    switch (transferResults.getStatus()) {
        case Created:
        case Exists:
            // Successfully created.
            break;
        default:
            // Could not be created.
            break;
    }
}
```

##### API changes:

|Type       |Before                                 | After                                   |
|-----------|---------------------------------------|-----------------------------------------|
|Enum       |CreateAccountResult                    |CreateAccountStatus                      |
|Enum Value |CreateAccountResult.Ok                 |CreateAccountStatus.Created              |
|Enum       |CreateTransferResult                   |CreateTransferStatus                     |
|Enum Value |CreateTransferResult.Ok                |CreateTransferStatus.Created             |
|Method     |CreateAccountResultBatch.getIndex()    |_removed_                                |
|Method     |_NA_                                   |CreateAccountResultBatch.getTimestamp()  |
|Method     |CreateAccountResultBatch.getResult()   |CreateAccountResultBatch.getStatus()     |
|Method     |CreateTransferResultBatch.getIndex()   |_removed_                                |
|Method     |_NA_                                   |CreateTransferResultBatch.getTimestamp() |
|Method     | CreateTransferResultBatch.getResult() |CreateTransferResultBatch.getStatus()    |

##### Other changes:

- All the blocking methods of the
  [`Client`](https://javadoc.io/doc/com.tigerbeetle/tigerbeetle-java/latest/com.tigerbeetle/com/tigerbeetle/Client.html)
  class (such as `createAccounts`, `createTransfers`, and others) can throw a _checked_ exception
  [`InterruptedException`](https://docs.oracle.com/javase/8/docs/api/java/lang/InterruptedException.html)
  to signal that the waiting thread was interrupted.
  The non-blocking version of the same methods returning a
  [`CompletableFuture<T>`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html)
  remain unchanged.

For more details, please refer to the
[Java client reference page](https://docs.tigerbeetle.com/coding/clients/java).

#### .NET Client

The way of handling the return values of `CreateAccounts`, `CreateAccountsAsync`,
`CreateTransfers`, and `CreateTransfersAsync` in the TigerBeetle  .NET Client has changed.

Before:
```c#
CreateTransfersResult[] transferErrors = client.CreateTransfers(transfers);
if (transferErrors.Length > 0) {
    // Error handling ...
}
```

After:
```c#
CreateTransferResult[] transferResults = client.CreateTransfers(transfers);
Assert.AreEqual(transferResults.Length, transfers.Length);
foreach(CreateTransferResult result in transferResults)
{
    switch(result.Status)
    {
        case CreateTransferStatus.Created:
        case CreateTransferStatus.Exists:
            // Successfully created.
            break;
        default:
            // Could not be created.
            break;
    }
}
```

##### API changes:

|Type       |Before                                 | After                                   |
|-----------|---------------------------------------|-----------------------------------------|
|Enum       |CreateAccountResult                    |CreateAccountStatus                      |
|Enum Value |CreateAccountResult.Ok                 |CreateAccountStatus.Created              |
|Enum       |CreateTransferResult                   |CreateTransferStatus                     |
|Enum Value |CreateTransferResult.Ok                |CreateTransferStatus.Created             |
|Struct     |CreateAccountsResult                   |CreateAccountResult                      |
|Property   |CreateAccountsResult.Index             |_removed_                                |
|Property   |_NA_                                   |CreateAccountResult.Timestamp            |
|Property   |CreateAccountsResult.Result            |CreateAccountResult.Status               |
|Struct     |CreateTransfersResult                  |CreateTransferResult                     |
|Property   |CreateTransfersResult.Index            |_removed_                                |
|Property   |_NA_                                   |CreateTransferResult.Timestamp           |
|Property   |CreateTransfersResult.Result           |CreateTransferResult.Status              |

##### Other changes:

- All non-batched version of methods were removed, such as `CreateAccount`, `CreateTransfer`,
  `LookupAccount`, and `LookupTransfer`. The batched versions `CreateAccounts`, `CreateTransfers`,
  `LookupAccounts`, and `LookupTransfers` remain unchanged.

For more details, please refer to the
[.NET client reference page](https://docs.tigerbeetle.com/coding/clients/dotnet).

#### Go Client

The way of handling the return values of `CreateAccounts` and `CreateTransfers`
in the TigerBeetle Go Client has changed.

Before:
```go
var transferErrors []TransferEventResult
var err error

transferErrors, err = client.CreateTransfers(transfers);
if err != nil {
    // Request error ...
}
if len(transferErrors) > 0 {
    // Error handling ...
}
```

After:
```go
var transferResults []CreateTransferResult
var err error

transferResults, err = client.CreateTransfers(transfers);
if err != nil {
    // Request error ...
}
for _, result := range transferResults {
    switch result.Status {
    case TransferCreated, TransferExists:
        // Successfully created.
    default:
        // Could not be created.
    }
}
```

##### API changes:

|Type       |Before                                 | After                                   |
|-----------|---------------------------------------|-----------------------------------------|
|Enum       |CreateAccountResult                    |CreateAccountStatus                      |
|Enum Value |AccountOK                              |AccountCreated                           |
|Enum       |CreateTransferResult                   |CreateTransferStatus                     |
|Enum Value |TransferOK                             |TransferCreated                          |
|Type       |AccountEventResult                     |CreateAccountResult                      |
|Field      |AccountEventResult.Index               |_removed_                                |
|Field      |_NA_                                   |CreateAccountResult.Timestamp            |
|Field      |AccountEventResult.Result              |CreateAccountResult.Status               |
|Type       |TransferEventResult                    |CreateTransferResult                     |
|Field      |TransferEventResult.Index              |_removed_                                |
|Field      |_NA_                                   |CreateTransferResult.Timestamp           |
|Field      |TransferEventResult.Result             |CreateTransferResult.Status              |

##### Other changes:

- The `types` and `errors` packages were removed, consolidating all definitions under the root
  `tigerbeetle_go` package.

For more details, please refer to the
[Go client reference page](https://docs.tigerbeetle.com/coding/clients/go).

#### Node Client

The way of handling the return values of `createAccounts` and `createTransfers`
in the TigerBeetle Node.js Client has changed.

Before:
```typescript
const transferErrors: CreateTransfersError[] = await client.createTransfers(transfers);
if (transferErrors.length > 0) {
    // Error handling ...
}
```

After:
```typescript
const transferResults: CreateTransferResult[] = await client.createTransfers(transfers);
assert.strictEqual(transferResults.length, transfers.length);
for (const result of transferResults) {
    switch (result.status) {
        case CreateTransferStatus.created:
        case CreateTransferStatus.exists:
            // Successfully created.
            break;
        default:
            // Could not be created.
            break;
    }
}
```

##### API changes:

|Type       |Before                                 | After                                   |
|-----------|---------------------------------------|-----------------------------------------|
|Enum       |CreateAccountError                     |CreateAccountStatus                      |
|Enum Value |CreateAccountError.ok                  |CreateAccountStatus.created              |
|Enum       |CreateTransferError                    |CreateTransferStatus                     |
|Enum Value |CreateTransferError.ok                 |CreateTransferStatus.created             |
|Type       |CreateAccountsError                    |CreateAccountResult                      |
|Field      |CreateAccountsError.index              |_removed_                                |
|Field      |_NA_                                   |CreateAccountResult.timestamp            |
|Field      |CreateAccountsError.result             |CreateAccountResult.status               |
|Type       |CreateTransfersError                   |CreateTransferResult                     |
|Field      |CreateTransfersError.index             |_removed_                                |
|Field      |_NA_                                   |CreateTransferResult.timestamp           |
|Field      |CreateTransfersError.result            |CreateTransferResult.status              |

For more details, please refer to the
[Node.js client reference page](https://docs.tigerbeetle.com/coding/clients/node).

#### Python Client

The way of handling the return values of `create_accounts` and `create_transfers`
in the TigerBeetle Python Client has changed.

Before:
```python
transfer_errors = client.create_transfers(transfers)
if len(transfer_errors) > 0:
    # Error handling ...
```

After:
```python
transfer_results = client.create_transfers(transfers)
assert len(transfer_results) == len(transfers)
for result in transfer_results:
    if result.status == tb.CreateAccountResult.CREATED or \
       result.status == tb.CreateAccountResult.EXISTS:
        # Successfully created.
    else:
        # Could not be created.
```

##### API changes:

|Type       |Before                                 | After                                   |
|-----------|---------------------------------------|-----------------------------------------|
|Enum       |CreateAccountResult                    |CreateAccountStatus                      |
|Enum Value |CreateAccountResult.OK                 |CreateAccountStatus.CREATED              |
|Enum       |CreateTransferResult                   |CreateTransferStatus                     |
|Enum Value |CreateTransferResult.OK                |CreateTransferStatus.CREATED             |
|Type       |CreateAccountsResult                   |CreateAccountResult                      |
|Field      |CreateAccountsResult.index             |_removed_                                |
|Field      |_NA_                                   |CreateAccountResult.timestamp            |
|Field      |CreateAccountsResult.result            |CreateAccountResult.status               |
|Type       |CreateTransfersResult                  |CreateTransferResult                     |
|Field      |CreateTransfersResult.index            |_removed_                                |
|Field      |_NA_                                   |CreateTransferResult.timestamp           |
|Field      |CreateTransfersResult.result           |CreateTransferResult.status              |

For more details, please refer to the
[Python client reference page](https://docs.tigerbeetle.com/coding/clients/python).