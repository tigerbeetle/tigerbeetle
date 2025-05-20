# Change Data Capture

TigerBeetle can stream changes (transfers and balance updates) to message queues using
the AMQP 0.9.1 protocol, which is compatible with RabbitMQ and various other message brokers.

Here’s how to deploy the CDC job:

```console
curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip && ./tigerbeetle version

./tigerbeetle amqp --cluster-id=0 \
    --addresses=3000 \
    --host=127.0.0.1 \
    --user=guest \
    --password=guest \
    --vhost=/ \
    --publish-exchange=tigerbeetle
```

Here what the arguments mean:

* `--cluster` specifies a globally unique 128 bit cluster ID.

* `--addresses` specify IP addresses of all the replicas in the cluster.
  **The order of addresses must correspond to the order of replicas**.

* `--host` the AMQP host address in the format `ip:port`.<br>
  Both IPv4 and IPv6 addresses are supported. If `port` is omitted, the default is `5672`.<br>
  Multiple addresses (for clustered environments) and DNS names are not supported.<br>
  The operator must resolve the IP address of the preferred/reachable server.<br>
  The CDC job will exit with a non-zero code in case of any connectivity or configuration issue
  with the AMQP server.

* `--user` the AMQP username.

* `--password` the AMQP password.<br>
   Only PLAIN authentication is supported.

* `--vhost` the AMQP virtual host name.

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

* `--timestamp-last` overrides the last published timestamp, resuming from this point.<br>
  This is a TigerBeetle timestamp with nanosecond precision.<br>
  Optional. If omitted, the last acknowledged timestamp is used.

## Message content:

Messages are published with custom headers,
allowing users to implement routing and filtering rules.

Message headers:

| Key                   | AMQP data type     | Description                              |
|-----------------------|--------------------|------------------------------------------|
| `event_type`          | `string`           | The event type.¹                         |
| `ledger`              | `long_uint`        | The ledger of the transfer and accounts. |
| `transfer_code`       | `short_uint`       | The transfer code.                       |
| `debit_account_code`  | `short_uint`       | The debit account code.                  |
| `credit_account_code` | `short_uint`       | The credit account code.                 |
| `app_id`              | `string`           | Constant `tigerbeetle`.                  |
| `content_type`        | `string`           | Constant `application/json`              |
| `delivery_mode`       | `short_short_uint` | Constant `2` which means _persistent_.   |
| `timestamp`           | `timestamp`        | The event timestamp.²                    |


> ¹ _One of `single_phase`, `two_phase_pending`, `two_phase_posted`, `two_phase_voided` or
  `two_phase_expired`._

> ² _It's the transfer's timestamp, except when `event_type == 'two_phase_expired'` when
  it's the expiry timestamp.<br>
  The AMQP standard is in seconds, so TigerBeetle timestamps are truncated.
  Use the message body timestamp for the full precision._

Message body:

The message body is encoded as JSON without line breaks or spaces.
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

**It is the consumer's responsibility to perform idempotency checks.**
