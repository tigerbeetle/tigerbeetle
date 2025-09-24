# Change Data Capture

TigerBeetle can stream changes (transfers and balance updates) to message queues using
the AMQP 0.9.1 protocol, which is compatible with RabbitMQ and various other message brokers.

See [Installing](./installing.md) for instructions on how to deploy the TigerBeetle binary.

Here’s how to start the CDC job:

```console
./tigerbeetle amqp --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 --cluster=0 \
    --host=127.0.0.1 \
    --vhost=/ \
    --user=guest --password=guest \
    --publish-exchange=tigerbeetle
```

Here what the arguments mean:

* `--addresses` specify IP addresses of all the replicas in the cluster.
  **The order of addresses must correspond to the order of replicas**.

* `--cluster` specifies a globally unique 128 bit cluster ID.

* `--host` the AMQP host address in the format `ip:port`.<br>
  Both IPv4 and IPv6 addresses are supported.
  If `port` is omitted, the AMQP default `5672` is used.<br>
  Multiple addresses (for clustered environments) and DNS names are **not supported**.<br>
  The operator must resolve the IP address of the preferred/reachable server.<br>
  The CDC job will exit with a non-zero code in case of any connectivity or configuration issue
  with the AMQP server.

* `--vhost` the AMQP virtual host name.

* `--user` the AMQP username.

* `--password` the AMQP password.<br>
   Only PLAIN authentication is supported.

* `--publish-exchange` the exchange name.<br>
  Must be a pre-existing exchange provided by the operator.<br>
  Optional. May be omitted if `--publish-routing-key` is present.

* `--publish-routing-key` the routing key used in combination with the exchange.<br>
  Optional. May be omitted if `publish-exchange` is present.

* `--event-count-max` the maximum number of events fetched from TigerBeetle
  and published to the AMQP server per batch.<br>
  Optional. Defaults to `2730` if omitted.

* `--idle-interval-ms` the time interval in milliseconds to wait before querying again
  when the last query returned no events.<br>
  Optional. Defaults to `1000` ms if omitted.

* `--requests-per-second-limit` throttles the maximum number of requests per second made
  to TigerBeetle.<br>
  Must be greater than zero.<br>
  Optional. No limit if omitted.

* `--timestamp-last` overrides the last published timestamp, resuming from this point.<br>
  This is a TigerBeetle timestamp with nanosecond precision.<br>
  Optional. If omitted, the last acknowledged timestamp is used.

## Message content:

Messages are published with custom headers,
allowing users to implement routing and filtering rules.

Message headers:

| Key                   | AMQP data type     | Description                              |
|-----------------------|--------------------|------------------------------------------|
| `event_type`          | `string`           | The event type.                          |
| `ledger`              | `long_long_int`    | The ledger of the transfer and accounts. |
| `transfer_code`       | `long_int`         | The transfer code.                       |
| `debit_account_code`  | `long_int`         | The debit account code.                  |
| `credit_account_code` | `long_int`         | The credit account code.                 |
| `app_id`              | `string`           | Constant `tigerbeetle`.                  |
| `content_type`        | `string`           | Constant `application/json`              |
| `delivery_mode`       | `short_short_uint` | Constant `2` which means _persistent_.   |
| `timestamp`           | `timestamp`        | The event timestamp.¹                    |

> ¹ _AMQP timestamps are represented in seconds, so TigerBeetle timestamps are truncated.<br>
    Use the `timestamp` field in the message body for full nanosecond precision._

Message body:

Each _event_ published contains information about the [transfer](../reference/transfer.md)
and the [account](../reference/account.md)s involved.

* `type`: The type of event.<br>
  One of `single_phase`, `two_phase_pending`, `two_phase_posted`, `two_phase_voided` or
  `two_phase_expired`.<br>
  See the [Two-Phase Transfers](../coding/two-phase-transfers.md) for more details.

* `timestamp`: The event timestamp.<br>
  Usually, it's the same as the transfer's timestamp,
  except when `event_type == 'two_phase_expired'` when it's the expiry timestamp.

* `ledger`: The [ledger](../coding/data-modeling.md#ledgers) code.

* `transfer`: Full details of the [transfer](../reference/transfer.md).<br>
  For `two_phase_expired` events, it's the pending transfer that was reverted.

* `debit_account`: Full details of the [debit account](../reference/transfer.md#debit_account_id),
  with the balance _as of_ the time of the event.

* `credit_account`: Full details of the [credit account](../reference/transfer.md#credit_account_id),
  with the balance _as of_ the time of the event.

The message body is encoded as a UTF-8 JSON without line breaks or spaces.
Long integers such as `u128` and `u64` are encoded as JSON strings to improve interoperability.

Here is a formatted example (with indentation and line breaks) for readability.

```json
{
  "timestamp": "1745328372758695656",
  "type": "single_phase",
  "ledger": 2,
  "transfer": {
    "id": 9082709,
    "amount": 3794,
    "pending_id": 0,
    "user_data_128": "79248595801719937611592367840129079151",
    "user_data_64": "13615171707598273871",
    "user_data_32": 3229992513,
    "timeout": 0,
    "code": 20295,
    "flags": 0,
    "timestamp": "1745328372758695656"
  },
  "debit_account": {
    "id": 3750,
    "debits_pending": 0,
    "debits_posted": 8463768,
    "credits_pending": 0,
    "credits_posted": 8861179,
    "user_data_128": "118966247877720884212341541320399553321",
    "user_data_64": "526432537153007844",
    "user_data_32": 4157247332,
    "code": 1,
    "flags": 0,
    "timestamp": "1745328270103398016"
  },
  "credit_account": {
    "id": 6765,
    "debits_pending": 0,
    "debits_posted": 8669204,
    "credits_pending": 0,
    "credits_posted": 8637251,
    "user_data_128": "43670023860556310170878798978091998141",
    "user_data_64": "12485093662256535374",
    "user_data_32": 1924162092,
    "code": 1,
    "flags": 0,
    "timestamp": "1745328270103401031"
  }
}
```

## Guarantees

TigerBeetle guarantees _at-least-once_ semantics when publishing to message brokers,
and makes a best effort to prevent duplicate messages.
However, during crash recovery, the CDC job may replay unacknowledged messages that could have
been already delivered to consumers.

It is the consumer's responsibility to perform **idempotency checks** when processing messages.

## Upgrading

The CDC job requires TigerBeetle cluster version `0.16.43` or greater.

The same [upgrade planning](./upgrading.md#planning-for-upgrades) recommended for clients applies
to the CDC job. The CDC job version must not be newer than the cluster version, as it will fail
with an error message if so.

Any transactions _originally_ created by TigerBeetle versions before `0.16.29` have the following
limitations for CDC processing:

- Events of type `two_phase_expired` are **not** supported.
- Only transfers where both the debit and credit accounts have the
  [`flags.history`](../reference/account.md#flagshistory) enabled are visible to CDC.

Transactions committed after version `0.16.29` are fully compatible with CDC and do not require
the `history` flag.

## CDC to RabbitMQ (AMQP 0.9.1) in production

### High Availability

The CDC job is single instance. Starting a second `tigerbeetle amqp` with the same `cluster_id`
will exit with a non-zero exit code. For high availability, the CDC job could be monitored for
crashes and restarted in case a failure.

The CDC job itself is stateless, and will resume from the last event acknowledged by RabbitMQ,
however it may replay events that weren't acknowledged but received by the exchange.

### TLS Support

For secure `AMQPS` connections, we recommend using a TLS Tunnel to wrap the connection between
TigerBeetle and RabbitMQ.

### Event Replay

By default, when the CDC job starts, it resumes from the timestamp of the last acknowledged event in
RabbitMQ. This can be overridden to using `--timestamp-last`. For example, `--timestamp-last=0` will
replay all events.
