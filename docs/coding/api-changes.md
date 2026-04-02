# API changes

## [0.17.0](https://github.com/tigerbeetle/tigerbeetle/releases/tag/0.17.0)

Applications using the TigerBeetle Client `0.16.x` **will require changes** when upgrading to
`0.17.x`.

Future releases of the TigerBeetle cluster tagged as `0.17.x` will continue to support old clients
for an extended period, giving application developers time to plan a smooth upgrade to the new
client library while independently upgrading the cluster to newer releases.

### New return type for `create_accounts` and `create_transfers`.

The TigerBeetle release `0.17.0` introduced a new API for handling results when creating
[Accounts](../reference/requests/create_accounts.md) and
[Transfers](../reference/requests/create_transfers.md).

The previous API for [`create_accounts`](../reference/requests/create_accounts.md) and
[`create_transfers`](../reference/requests/create_transfers.md) reported outcomes only for
failed events, returning a _sparse array_ of results. Each result included the `index` of the
failed element within the event batch and the corresponding error code. Successfully created
events were not returned by the TigerBeetle cluster, and the application could safely assume
them as `ok`.

While this approach prioritized saving network bandwidth by omitting results for the common
_happy path_, it didn’t provide enough information about the outcome.

The new protocol departs from the _sparse array_ style, returning the status of each event,
including the successfully created ones, along with the `timestamp` when they were processed
by the TigerBeetle cluster.

Applications can now benefit from knowing the `timestamp` assigned to a successfully created
[`Account`](../reference/account.md#timestamp) or
[`Transfer`](../reference/transfer.md#timestamp), while also handling alternative paths more
efficiently by knowing _when_ validation occurred for those that couldn’t be created.

The enums <code>CreateAccount<b>Result</b></code> and <code>CreateTransfer<b>Result</b></code> were
renamed to <code>CreateAccount<b>Status</b></code> and <code>CreateTransfer<b>Status</b></code>,
and the status code `ok` was renamed to `created`.

This makes it explicit whether an event became part of the database state (`created` and `exists`)
or not. By removing the duality of _ok_ and _errors_, applications can handle results more clearly
according to their policies, since some outcomes might not be considered a failure by application
logic — for example, a `Transfer` not being created due to balance checks or pending timeouts
isn’t necessarily an _error_.

Likewise, the `exists` status now reports the same `timestamp` returned by `created`,
allowing the application to treat both cases consistently.

The result types <code>CreateAccount<b>s</b>Result</code> and
<code>CreateTransfer<b>s</b>Result</code> were renamed to the singular form,
<code>CreateAccountResult</code> and <code>CreateTransferResult</code>.

<table>
<tr><th>Before</th><th>After</th></tr>
<tr><td>

```zig
pub const CreateAccountsResult = extern struct {
    index: u32,
    result: CreateAccountResult,
};

```

</td><td>

```zig
pub const CreateAccountResult = extern struct {
    timestamp: u64,
    status: CreateAccountStatus,
    reserved: u32 = 0,
};
```

</td></tr>
<tr><!-- Empty row to avoid the zebra stripe style--></tr>
<tr><td>

```zig
pub const CreateTransfersResult = extern struct {
    index: u32,
    result: CreateTransferResult,
};

```

</td><td>

```zig
pub const CreateTransferResult = extern struct {
    timestamp: u64,
    status: CreateTransferStatus,
    reserved: u32 = 0,
};
```

</td></tr>
</table>

### Query limits.

The previous API did not validate the maximum range of the fields
[`AccountFilter.limit`](../reference/account-filter.md#limit) and
[`QueryFilter.limit`](../reference/query-filter.md#limit).
Values up to `2^32 - 1` were accepted, and the TigerBeetle cluster was
responsible for capping the number of results to fit the message size.

Now the TigerBeetle client enforces a valid `limit` and rejects requests with limits
greater than the [maximum batch size](../coding/requests.md#batching-events),
returning the `too_much_data` error code.

This is the same behavior as when, for example,
[`create_accounts`](../reference/requests/create_accounts.md) or
[`create_transfers`](../reference/requests/create_transfers.md)
are called with more than _8189_ events.

The operations [`query_accounts`](../reference/requests/query_accounts.md),
[`query_transfers`](../reference/requests/query_transfers.md),
[`get_account_transfers`](../reference/requests/get_account_transfers.md), and
[`get_account_balances`](../reference/requests/get_account_balances.md) are affected.

### Client breaking changes

Along with the new result types, some client libraries have changed the API to be more
idiomatic, for naming consistency, or even due to bug fixes in the previous API.

See below is a list of API changes specific to each client library:

<details><summary><b>.NET</b></summary>

### .NET Client breaking changes

The TigerBeetle .NET Client `0.17.0` introduced the following breaking changes:

- The enum types <code>CreateAccount<b>Result</b></code> and
  <code>CreateTransfer<b>Result</b></code>, with the status codes for the
  [`create_accounts`](../reference/requests/create_accounts.md) and
  [`create_transfers`](../reference/requests/create_transfers.md) operations respectively,
  were renamed to <code>CreateAccount<b>Status</b></code> and
  <code>CreateTransfer<b>Status</b></code>.

  The enum value `Ok`, present in both enum types, was replaced by the new status code
  `Created`, which indicates that the event was successfully created.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Enum       |`CreateAccountResult`                  |`CreateAccountStatus`                    |
  |Enum Value |`CreateAccountResult.Ok`               |`CreateAccountStatus.Created`            |
  |Enum       |`CreateTransferResult`                 |`CreateTransferStatus`                   |
  |Enum Value |`CreateTransferResult.Ok`              |`CreateTransferStatus.Created`           |

- The result types <code>CreateAccount<b>s</b>Result</code> and
  <code>CreateTransfer<b>s</b>Result</code> were renamed to the singular form,
  <code>CreateAccountResult</code> and <code>CreateTransferResult</code>.

  The property `Index` was removed, since each result value corresponds to an event in the batch
  at the same index.

  Additional changes include the new `Timestamp` property and renaming the `Result` property to
  `Status`, reflecting the change to the associated enums.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Struct     |`CreateAccountsResult`                 |`CreateAccountResult`                    |
  |Property   |`CreateAccountsResult.Index`           |_removed_                                |
  |Property   |_NA_                                   |`CreateAccountResult.Timestamp`          |
  |Property   |`CreateAccountsResult.Result`          |`CreateAccountResult.Status`             |
  |Struct     |`CreateTransfersResult`                |`CreateTransferResult`                   |
  |Property   |`CreateTransfersResult.Index`          |_removed_                                |
  |Property   |_NA_                                   |`CreateTransferResult.Timestamp`         |
  |Property   |`CreateTransfersResult.Result`         |`CreateTransferResult.Status`            |

- Non-batched methods of the `Client` class that received a single event such as
  `CreateAccount`, `CreateTransfer`, `LookupAccount`, and `LookupTransfer` were removed.

  The batched versions that take an array of events such as
  <code>CreateAccount<b>s</b></code>, <code>CreateTransfer<b>s</b></code>,
  <code>LookupAccount<b>s</b></code>, and <code>LookupTransfer<b>s</b></code> remain unchanged.

- New exceptions were introduced for conditions that can be handled by the application.
  The `PacketStatus` enum is now internal and the `RequestException` was made `abstract`.

  The client can be explicitly closed by calling `Client.Close()`, whereas previously the only
  way to close a client was through `Client.Dispose()`.
  Interacting with a closed client now throws a `ClientClosedException`
  instead of an `ObjectDisposedException`.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Enum       |`PacketStatus`                         |_removed_                                |
  |Exception  |`RequestException`                     |_removed_                                |
  |Method     | _added_                               |`Client.Close()`                         |
  |Exception  |                                       |`ClientClosedException`                  |
  |Exception  |                                       |`ClientEvictedException`                 |
  |Exception  |                                       |`ClientReleaseException`                 |
  |Exception  |                                       |`TooMuchDataException`                   |

### Example:

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

For more details, please refer to the
[.NET client reference page](https://docs.tigerbeetle.com/coding/clients/dotnet).

</details><br><!--.NET-->

<details><summary><b>Go</b></summary>

### Go Client breaking changes

The TigerBeetle Go Client `0.17.0` introduced the following breaking changes:

- The enum types <code>CreateAccount<b>Result</b></code> and
  <code>CreateTransfer<b>Result</b></code>, with the status codes for the
  [`create_accounts`](../reference/requests/create_accounts.md) and
  [`create_transfers`](../reference/requests/create_transfers.md) operations respectively,
  were renamed to <code>CreateAccount<b>Status</b></code> and
  <code>CreateTransfer<b>Status</b></code>.

  The enum values `AccountOK` and `TransferOK`, were replaced by the new status codes
  `AccountCreated` and `TransferCreated`, which indicates the event was successfully
  created by the operation.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Enum       |`CreateAccountResult`                  |`CreateAccountStatus`                    |
  |Enum Value |`AccountOK`                            |`AccountCreated`                         |
  |Enum       |`CreateTransferResult`                 |`CreateTransferStatus`                   |
  |Enum Value |`TransferOK`                           |`TransferCreated`                        |

- The result types `AccountEventResult` and `TransferEventResult` were renamed to
  `CreateAccountResult` and `CreateTransferResult`.

  The field `Index` was removed, since each result value corresponds to an event in the batch
  at the same index.

  Additional changes include the new `Timestamp` field and renaming the `Result` field to
  `Status`, reflecting the change to the associated enums.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Type       |`AccountEventResult`                   |`CreateAccountResult`                    |
  |Field      |`AccountEventResult.Index`             |_removed_                                |
  |Field      |_NA_                                   |`CreateAccountResult.Timestamp`          |
  |Field      |`AccountEventResult.Result`            |`CreateAccountResult.Status`             |
  |Type       |`TransferEventResult`                  |`CreateTransferResult`                   |
  |Field      |`TransferEventResult.Index`            |_removed_                                |
  |Field      |_NA_                                   |`CreateTransferResult.Timestamp`         |
  |Field      |`TransferEventResult.Result`           |`CreateTransferResult.Status`            |

- The `types` and `errors` packages were removed, consolidating all definitions under the root
  `tigerbeetle_go` package.

  The `Err` types were removed in favor of simple value declarations for error codes,
  allowing the use of idiomatic constructions such as `errors.Is(err, ErrTooMuchData)`.

- Conversions between `UInt128` and `big.Int` now return and accept a pointer to a big integer
  `*big.Int`, making the API more idiomatic.

  |Type      |Before                                  | After                                   |
  |----------|----------------------------------------|-----------------------------------------|
  |Function  |`BigInt() big.Int`                      |`BigInt() *big.Int`                      |
  |Function  |`BigIntToUint128(value big.Int) Uint128`|`BigIntToUint128(value *big.Int) Uint128`|

### Example:

Before:
```go
import (
	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

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
import (
	. "github.com/tigerbeetle/tigerbeetle-go"
)

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

For more details, please refer to the
[Go client reference page](https://docs.tigerbeetle.com/coding/clients/go).

</details><br><!--Go-->

<details><summary><b>Java</b></summary>

### Java Client breaking changes

The TigerBeetle Java Client `0.17.0` introduced the following breaking changes:

- The enum types <code>CreateAccount<b>Result</b></code> and
  <code>CreateTransfer<b>Result</b></code>, with the status codes for the
  [`create_accounts`](../reference/requests/create_accounts.md) and
  [`create_transfers`](../reference/requests/create_transfers.md) operations respectively,
  were renamed to <code>CreateAccount<b>Status</b></code> and
  <code>CreateTransfer<b>Status</b></code>.

  The enum value `Ok`, present in both enum types, was replaced by the new status code
  `Created`, which indicates that the event was successfully created.

  |Type       |Before                                 | After                                    |
  |-----------|---------------------------------------|------------------------------------------|
  |Enum       |`CreateAccountResult`                  |`CreateAccountStatus`                     |
  |Enum Value |`CreateAccountResult.Ok`               |`CreateAccountStatus.Created`             |
  |Enum       |`CreateTransferResult`                 |`CreateTransferStatus`                    |
  |Enum Value |`CreateTransferResult.Ok`              |`CreateTransferStatus.Created`            |


- The result types `CreateAccountResultBatch` and `CreateTransferResultBatch`,
  had the `getIndex()` property removed, since each result value corresponds to one event in the
  batch at the same index.

  Additional changes include the new `getTimestamp()` property and renaming the
  `getResult()` property to `getStatus()`, reflecting the change to the associated enums.

  |Type       |Before                                 | After                                    |
  |-----------|---------------------------------------|------------------------------------------|
  |Method     |`CreateAccountResultBatch.getIndex()`  |_removed_                                 |
  |Method     |_NA_                                   |`CreateAccountResultBatch.getTimestamp()` |
  |Method     |`CreateAccountResultBatch.getResult()` |`CreateAccountResultBatch.getStatus()`    |
  |Method     |`CreateTransferResultBatch.getIndex()` |_removed_                                 |
  |Method     |_NA_                                   |`CreateTransferResultBatch.getTimestamp()`|
  |Method     |`CreateTransferResultBatch.getResult()`|`CreateTransferResultBatch.getStatus()`   |

- All the _blocking_ methods of the
  [`Client`](https://javadoc.io/doc/com.tigerbeetle/tigerbeetle-java/latest/com.tigerbeetle/com/tigerbeetle/Client.html)
  class (such as `createAccounts`, `createTransfers`, and others) may throw the _checked_ exception
  [`InterruptedException`](https://docs.oracle.com/javase/8/docs/api/java/lang/InterruptedException.html)
  to signal that the waiting thread was interrupted by the Java environment.
  The non-blocking versions of the same methods that return a
  [`CompletableFuture<T>`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html)
  remain unchanged.

- New exceptions were introduced for conditions that can be handled by the application.
  The `PacketStatus` enum is now internal and the `RequestException` was removed.

  All operations throw `ClientClosedException` instead of `IllegalStateException`
  if the client is  closed.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Enum       |`PacketStatus`                         |_removed_                                |
  |Exception  |`RequestException`                     |_abstract class_                         |
  |Exception  |`ClientClosedException`                |_extends_ `RequestException`             |
  |Exception  |                                       |`ClientEvictedException`                 |
  |Exception  |                                       |`ClientReleaseException`                 |
  |Exception  |                                       |`TooMuchDataException`                   |

### Example:

Before:
```java
CreateTransferResultBatch transferErrors = client.createTransfers(transfers);
if (transferErrors.getLength() > 0) {
    // Error handling ...
}
```

After:
```java
try {
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
} catch (InterruptedException exception) {
    // The thread was interrupted.
}
```

For more details, please refer to the
[Java client reference page](./clients/java).

</details><br><!--Java-->

<details><summary><b>Node.js</b></summary>

### Node.js Client breaking changes

The TigerBeetle Node.js Client `0.17.0` introduced the following breaking changes:

- The enum types `CreateAccountError` and `CreateTransferError`, with the status codes
  for the [`create_accounts`](../reference/requests/create_accounts.md) and
  [`create_transfers`](../reference/requests/create_transfers.md) operations respectively,
  were renamed to `CreateAccountStatus` and `CreateTransferStatus`.

  The enum value `ok`, present in both enum types, was replaced by the new status code
  `created`, which indicates that the event was successfully created.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Enum       |`CreateAccountError`                   |`CreateAccountStatus`                    |
  |Enum Value |`CreateAccountError.ok`                |`CreateAccountStatus.created`            |
  |Enum       |`CreateTransferError`                  |`CreateTransferStatus`                   |
  |Enum Value |`CreateTransferError.ok`               |`CreateTransferStatus.created`           |

- The result types `CreateAccountsError` and `CreateTransfersError` were renamed to
  `CreateAccountResult` and `CreateTransferResult`.

  The field `index` was removed, since each result value corresponds to an event in the batch
  at the same index.

  Additional changes include the new `timestamp` field and renaming the
  `result` field to `status`, reflecting the change to the associated enums.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Type       |`CreateAccountsError`                  |`CreateAccountResult`                    |
  |Field      |`CreateAccountsError.index`            |_removed_                                |
  |Field      |_NA_                                   |`CreateAccountResult.timestamp`          |
  |Field      |`CreateAccountsError.result`           |`CreateAccountResult.status`             |
  |Type       |`CreateTransfersError`                 |`CreateTransferResult`                   |
  |Field      |`CreateTransfersError.index`           |_removed_                                |
  |Field      |_NA_                                   |`CreateTransferResult.timestamp`         |
  |Field      |`CreateTransfersError.result`          |`CreateTransferResult.status`            |

- New error type `RequestError` was introduced for conditions that can be handled by the
  application.
  Match the `RequestError.code` property against the constants defined in `ErrorCodes` to
  determine the specific failure.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Type       |                                       |`ErrorCodes`                             |
  |Error      |                                       |`RequestError`                           |


### Example:

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

For more details, please refer to the
[Node.js client reference page](./clients/node).

</details><br><!--Node.js-->

<details><summary><b>Python</b></summary><br>

### Python Client breaking changes

The TigerBeetle Python Client `0.17.0` introduced the following breaking changes:

- The enum types `CreateAccountResult` and `CreateTransferResult`, with the status codes
  for the [`create_accounts`](../reference/requests/create_accounts.md) and
  [`create_transfers`](../reference/requests/create_transfers.md) operations respectively,
  were renamed to `CreateAccountStatus` and `CreateTransferStatus`.

  The enum value `OK`, present in both enum types, was replaced by the new status code
  `CREATED`, which indicates that the event was successfully created.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Enum       |`CreateAccountResult`                  |`CreateAccountStatus`                    |
  |Enum Value |`CreateAccountResult.OK`               |`CreateAccountStatus.CREATED`            |
  |Enum       |`CreateTransferResult`                 |`CreateTransferStatus`                   |
  |Enum Value |`CreateTransferResult.OK`              |`CreateTransferStatus.CREATED`           |

- The result types <code>CreateAccount<b>s</b>Result</code> and
  <code>CreateTransfer<b>s</b>Result</code> were renamed to the singular form,
  <code>CreateAccountResult</code> and <code>CreateTransferResult</code>.

  The field `index` was removed, since each result value corresponds to an event in the batch
  at the same index.

  Additional changes include the new `timestamp` field and renaming the `result` field to
  `status`, reflecting the change to the associated enums.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Type       |`CreateAccountsResult`                 |`CreateAccountResult`                    |
  |Field      |`CreateAccountsResult.index`           |_removed_                                |
  |Field      |_NA_                                   |`CreateAccountResult.timestamp`          |
  |Field      |`CreateAccountsResult.result`          |`CreateAccountResult.status`             |
  |Type       |`CreateTransfersResult`                |`CreateTransferResult`                   |
  |Field      |`CreateTransfersResult.index`          |_removed_                                |
  |Field      |_NA_                                   |`CreateTransferResult.timestamp`         |
  |Field      |`CreateTransfersResult.result`         |`CreateTransferResult.status`            |

- The constant `amount_max` was renamed to `AMOUNT_MAX`.

- New exceptions were introduced for conditions that can be handled by the application.
  The `PacketStatus` enum is now internal and the `PacketError` exception was removed.

  |Type       |Before                                 | After                                   |
  |-----------|---------------------------------------|-----------------------------------------|
  |Enum       |`PacketStatus`                         |_removed_                                |
  |Exception  |`PacketError`                          |_removed_                                |
  |Exception  |                                       |`ClientEvictedError`                     |
  |Exception  |                                       |`ClientReleaseTooLowError`               |
  |Exception  |                                       |`ClientReleaseTooHighError`              |
  |Exception  |                                       |`TooMuchDataError`                       |

### Example:

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

For more details, please refer to the
[Python client reference page](./clients/python).

</details><br><!--Python-->

<details><summary><b>Rust</b></summary><br>

### Rust Client breaking changes

The TigerBeetle Rust Client `0.17.0` also introduced the new API.
However, the Rust client is not yet publicly available on Crates.io at the time of this release,
so they are not considered breaking changes.

For more details, please refer to the
[Rust client reference page](./clients/rust).

</details><!--Rust-->

## [0.16.33](https://github.com/tigerbeetle/tigerbeetle/releases/tag/0.16.33)

### Oldest supported client version is `0.16.4`

Please make sure that all of your clients are running on at `0.16.4` or newer before upgrading to this release!

## [0.16.4](https://github.com/tigerbeetle/tigerbeetle/releases/tag/0.16.4)

### Transient Failures

Clients have the strong guarantee that a `Transfer.id` that has once failed due to a transient error code will never succeed again if retried.

Transient failures occur when semantically valid transfers fail due to reasons that depend exclusively on the database status. For example, when an `account` does not have enough credits to fulfill the `transfer`.

Please refer to the [documentation](../reference/requests/create_transfers.md) for a complete list of all transient failures.

As of `0.16.4`:

- New precedence order for the `create_transfers` and `create_accounts` error codes.
  The idempotency checks (e.g., all `exists_*` cases) are now validated prior to the semantical checks.

  For example, when submitting a `transfer` with the `id` of an existing one, clients might receive `exists_with_different_<field_name>` instead of `<field_name>_must_not_be_zero`.

  _This is **not** a breaking API change and affects all supported client versions._

- `create_transfers` now returns `exists_with_different_ledger`
  Reflecting the new precedence order change.

  _This is a breaking API change which is gated in the state machine by the client's release._
  _Clients of previous versions will receive `transfer_must_have_the_same_ledger_as_accounts` instead._

- Added a new result code
  [`id_already_failed`](../reference/requests/create_transfers.md#id_already_failed).
  It is returned by `create_transfers` when the `Transfer.id` was already used in a previous attempt that resulted in a transient failure.

  _This is a breaking API change which is gated in the state machine by the client's release._
  _Clients of previous versions will be able to successfully create transfers submitted with an `id` that previously failed, since it passes all validations._


## [0.16.0](https://github.com/tigerbeetle/tigerbeetle/releases/tag/0.16.0)

### Zero-amount transfers

Zero-amount transfers are now permitted.

This is a _breaking API change_ which is gated in the state machine by the _client's_ release.

As of `0.16.0`:

- Clients no longer return `amount_must_not_be_zero` to `create_transfers`.

- Balancing transfers no longer use `amount=0` as a sentinel value to represent "transfer as much as possible". For that purpose, use `AMOUNT_MAX` (exact constant name depends on client implementation) instead.

- Post-pending transfers no longer use `amount=0` as a sentinel value to represent "transfer full pending amount". For that purpose, use `AMOUNT_MAX` instead. In `0.16.0`, sending a post-pending transfer with `amount=0` will successfully post `amount=0`, voiding the remainder (i.e. voiding the whole pending amount).