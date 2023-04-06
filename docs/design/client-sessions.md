# Client Sessions

A _client session_ is a sequence of alternating [requests](./client-requests.md) and replies between a
client and a cluster.

A client session may have at most one in-flight request — i.e. at most one unique request on the
network for which a reply has not been received. This simplifies consistency and allows the cluster
to statically guarantee capacity in its incoming message queue. Additional requests from the
application are queued by the client, to be dequeued and sent when their preceding request receives
a reply.

TigerBeetle has a [hard limit](#eviction) on the number of concurrent
client sessions, and encourages minimizing the number of concurrent clients to
[maximize throughput](./client-requests.md#batching-events).

## Lifecycle

A client session begins when a client registers itself with the cluster.

- Each client session has a unique identifier ("client id") — an ephemeral random 128-bit id.
- The client sends a special "register" message which is committed by the cluster, at which point
  the client is "registered" — once it receives the reply, it may begin sending requests.
- Client registration is handled automatically by the TigerBeetle client implementation when the
  client is initialized, before it sends its first request.
- When a client restarts (for example, the application service running the TigerBeetle client is
  restarted) it does not resume its old session — it starts a new session, with a new (random)
  client id.

A client session ends when either:

  - the client session is [evicted](#eviction), or
  - the client terminates

— whichever occurs first.

## Eviction

When a client session is registering and the number of active sessions in the cluster is already at
the cluster's concurrent client session
[limit](https://tigerbeetle.com/blog/a-database-without-dynamic-memory/) (`config.clients_max`,
32 by default), an existing client session must be evicted to make space for
the new session.

- After a session is evicted by the cluster, no future requests from that session will ever execute.
- The evicted session is chosen as the session that committed a request the longest time ago.

The cluster sends a message to notify the evicted session that it has ended. Typically the evicted
client is no longer active (already terminated), but if it is active, the eviction message causes it
to self-terminate, bubbling up to the application as an `session evicted` error.

(TODO: Right now evicted clients panic — fix that so this is accurate.)

If active clients are terminating with `session evicted` errors, it (most likely) indicates that
the application is trying to run [too many](./client-requests.md#batching-events) concurrent clients.
