# Consistency

TigerBeetle is designed to guard against bugs not only in its
[own code](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md), but
at the boundaries, in the application code which interfaces with TigerBeetle.
This is exhibited by the client's API design, which may be surprising (see [Retries](#retries)) when
contrasted with a more conventional database.

Strict consistency guarantees (at the database level) simplify 1) application logic and 2) error handling
farther up the stack.

## Guarantees

TigerBeetle provides strict serializability
([serializability + linearizability](http://www.bailis.org/blog/linearizability-versus-serializability/))
to each [client session](./client-sessions.md).

But consistency models can seem arcane.
What specific guarantees does TigerBeetle provide to applications?

#### Sessions

- A client session may have at most one in-flight request.
- A client session reads its own writes.
- A client session observes writes in the order that they occur on the cluster.
- A client session observes [`debits_posted`](../reference/accounts.md#debits_posted) and
  [`credits_posted`](../reference/accounts.md#credits_posted) as monotonically increasing.
  That is, a client session will never see `credits_posted` or `debits_posted` decrease.
- A client session never observes uncommitted updates.
- A client session never observes a broken invariant (e.g.
  [`flags.credits_must_not_exceed_debits`](../reference/accounts.md#flagscredits_must_not_exceed_debits)
  or [`flags.linked`](../reference/transfers.md#flagslinked)).
- Multiple client sessions may receive replies [out of order](#reply-order) relative to one another.
- A client session can consider a request executed when it receives a reply for the request.

#### Requests

- A request executes within the cluster at most once.
- Requests do not [time out](#retries) — a timeout typically implies failure, which cannot be
  conclusively determined in the context of network faults.
- Requests retried by their original client session receive identical replies.
- Requests retried by a different client (same request body, different session) may receive
  [different replies](#consistency-with-foreign-databases).
- Events within a request are executed in sequential order.
- Events within a request do not interleave with events from other requests.
  (TODO: Can timeouts interleave batches, or should we shift the batch so that timeouts land
  entirely before/after?)
- All events within a request batch are committed, or none are.

#### Events

- Once committed, an event will always be committed — the cluster's state never backtracks.
- Within a cluster, object timestamps are unique.
  For all objects `A` and `B` belonging to the same cluster, `A.timestamp ≠ B.timestamp`.
- Within a cluster, object timestamps are strictly increasing.
  For all objects `A` and `B` belonging to the same cluster, if `A.timestamp < B.timestamp`,
  then `A` was committed earlier than `B`.
- If a client session is terminated and restarts, it is guaranteed to see updates for which the
  corresponding reply was received prior to termination.
- If a client session is terminated and restarts, it is _not_ guaranteed to see updates for
  which the corresponding reply was _not_ received prior to the restart. Those updates may
  occur at any point in the future, or never. Handling application crash recovery safely requires
  [using `id`s to idempotently retry events](#consistency-with-foreign-databases).

#### Accounts

- Accounts are immutable — once created, they are never modified
  (excluding balance fields, which are modified by transfers).
- There is at most one `Account` with a particular `id`.
- The sum of all accounts' `debits_pending` equals the sum of all accounts' `credits_pending`.
- The sum of all accounts' `debits_posted` equals the sum of all accounts' `credits_posted`.

#### Transfers

- Transfers are immutable — once created, they are never modified.
- There is at most one `Transfer` with a particular `id`.
- A [pending transfer](../reference/transfers.md#pending-transfer) resolves at most once.
- Transfer [timeouts](../reference/transfers.md#timeout) are deterministic, driven
  by the cluster's timestamp.

### Reply Order

Replies to a client session always arrive in order — a client session may have only one request
in-flight, and clients ignore (duplicate) replies to their prior requests.

- Requests are executed in the order they arrive at the cluster's primary.
- Replies to different clients may arrive out of order.

#### Example

Consider two clients `A` and `B`:

  1. Client `A` sends request `A₁`.
  2. Client `B` sends request `B₁`.

Client `A` sent its request first, but requests `A₁` and `B₁` may execute in either order —
whichever arrives first at the primary will execute first.

In this diagram, the requests are delivered out of order — `B₁` then `A₁`:

```mermaid
sequenceDiagram
    autonumber
    participant Client A
    participant Client B
    participant (Network)
    participant Cluster
    Client A->>(Network): A₁ (request)
    Client B->>(Network): B₁ (request)
    Note over (Network): Request A₁ is delayed by the network.
    (Network)->>Cluster: B₁ (request)
    (Network)->>Cluster: A₁ (request)
    Cluster->>(Network): B₁ (reply)
    Cluster->>(Network): A₁ (reply)
    (Network)->>Client B: B₁(reply)
    (Network)->>Client A: A₁ (reply)
```

Suppose instead `A₁` arrives and executes before `B₁`.
The replies may be delivered in the same order (`A₁` then `B₁`), or they may be reordered, as shown below:

```mermaid
sequenceDiagram
    autonumber
    participant Client A
    participant Client B
    participant (Network)
    participant Cluster
    Client A->>(Network): A₁ (request)
    Client B->>(Network): B₁ (request)
    (Network)->>Cluster: A₁ (request)
    (Network)->>Cluster: B₁ (request)
    Cluster->>(Network): A₁ (reply)
    Cluster->>(Network): B₁ (reply)
    Note over (Network): Reply A₁ is delayed by the network.
    (Network)->>Client B: B₁(reply)
    (Network)->>Client A: A₁ (reply)
```

### Retries

A [client session](./client-sessions.md) will automatically retry a request until either:

- the client receives a corresponding reply from the cluster, or
- the client is terminated.

Unlike most database or RPC clients:

- the TigerBeetle client will never time out
- the TigerBeetle client has no retry limits
- the TigerBeetle client does not surface network errors

With TigerBeetle's strict consistency model, surfacing these errors at the client/application level
would be misleading. An error would imply that a request did not execute, when that is not known:

- A request delayed by the network could execute after its timeout.
- A reply delayed by the network could execute before its timeout.

### Consistency with Foreign Databases

TigerBeetle objects may correspond to objects in a foreign data store (e.g. another DBMS). Keeping
multiple data stores consistent (in sync) is subtle in the context of application process faults.

Object creation events are idempotent, but only the first attempt will return `.ok`,
while all successive identical attempts return `.exists`. The client may crash after creating
the object, but before receiving the `.ok` reply. Because the session resets, neither that client
nor any others will see the object's corresponding `.ok` result.

Therefore, to recover to the correct state after a crash, an application that synchronizes updates
between multiple data stores must treat `.exists` as equivalent to `.ok`.

#### Example

Suppose that an application creates users within Postgres, and for each user a
corresponding `Account` in TigerBeetle.

This scenario depicts the typical case:

  1. _Application_: Create user `U₁` in Postgres with `U₁.account_id = A₁` and
    `U₁.account_exists = false`.
  2. _Application_: Send "create account" request `A₁` to the cluster.
  3. _Cluster_: Create `A₁`; reply `ok`.
  4. _Application_: Receive reply `A₁: ok` from the cluster.
  5. _Application_: Set `U₁.account_exists = true`.

But suppose the application crashes and restarts immediately after sending its request (step 2):

  1. _Application_: Create user `U₁` in Postgres with `U₁.account_id = A₁` and
    `U₁.account_exists = false`.
  2. _Application_: Send "create account" request `A₁` to the cluster.
  3. _Application_: Crash. Restart.
  4. _Cluster_: Create `A₁`; reply `ok` — but the application session has reset,
    so this reply never reaches the application).
  5. _Application_: Send "create account" request `A₁` to the cluster.
  6. _Cluster_: Create `A₁`; reply `exists`.
  7. _Application_: Receive reply `A₁: exists` from the cluster.
  8. _Application_: Set `U₁.account_exists = true`.

In the second case, the application observes that the account is created by receiving `.exists`
(step 6) instead of `.ok`.

Note that the retry (step 5) reused the same account `id` from the original request (step 2).
An alternate approach is to generate a new account `id` for each "create account" attempt,
and perhaps store the account's `id` on `U₁` when it is successfully created.
Then account creation could be restricted to the `.ok` code — but application restarts would leave
orphaned accounts in TigerBeetle, which may be confusing for auditing and debugging.
