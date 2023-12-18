# Operations

An _operation_ queries or updates the database state.

There are four operations available to clients:

- [`create_accounts`](./create_accounts.md): create `Account`s
- [`create_transfers`](./create_transfers.md): create `Transfer`s
- [`lookup_accounts`](./lookup_accounts.md): fetch `Account`s by `id`
- [`lookup_transfers`](./lookup_transfers.md): fetch `Transfer`s by `id`
- [`get_account_transfers`](./get_account_transfers.md): fetch `Transfer`s
by `debit_account_id` or `credit_account_id`

## Events and Results

Each operation has a corresponding _event_ and _result_ type.

- The client sends events to the cluster.
- The client receives results from the cluster (1:1 for each event sent).
- Events and results for the same operation are
  [batched](../../design/client-requests.md#batching-events) for throughput.
- A batch of an operation's events is called a [_request_](../../design/client-requests.md).
- A batch of an operation's results is called a _reply_.

Client implementations provide an API for sending batched events and decoding the corresponding
batched results.

| Operation               | Event                                                  | Result                                                     |
| ----------------------- | ------------------------------------------------------ | ---------------------------------------------------------- |
| `create_accounts`       | [`Account`](./create_accounts.md#Event)                | [`CreateAccountResult`](./create_accounts.md#Result)       |
| `create_transfers`      | [`Transfer`](./create_transfers.md#Event)              | [`CreateTransferResult`](./create_transfers.md#Result)     |
| `lookup_accounts`       | [`Account.id`](./lookup_accounts.md#Event)             | [`Account`](./lookup_accounts.md#Result) or nothing        |
| `lookup_transfers`      | [`Transfer.id`](./lookup_transfers.md#Event)           | [`Transfer`](./lookup_transfers.md#Result) or nothing      |
| `get_account_transfers` | [`AccountTransfers`](./get_account_transfers.md#Event) | [`Transfer`](./get_account_transfers.md#Result) or nothing |
