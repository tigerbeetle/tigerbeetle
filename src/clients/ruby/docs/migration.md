# TigerBeetle gem migration guide

Between 0.0.x and 0.x.y the TigerBeetle gem changed from a third-party client
developed by [Anthony D](https://github.com/antstorm) to an official client
maintained by TigerBeetle. While the overall API remains similar, some changes
were made to provide an experience that's more consistent with the official
clients for other languages.

- [Client changes](#client-changes)
  - [Connecting](#connecting)
  - [Callback API](#callback-api)
  - [Splat parameters](#splat-parameters)
  - [Flag handling](#flag-handling)
  - [Returned objects](#returned-objects)
  - [Exception classes](#exception-classes)
  - [Logging](#logging)
  - [`TB` top-level alias](#tb-top-level-alias)
- [Licensing](#licensing)

## Client changes

### Connecting

The `TigerBeetle.connect` method has been removed and `TigerBeetle::Client.new`
no longer provides default arguments.

```rb
# Before
client = TigerBeetle.connect # using default cluster_id (0) and address (127.0.0.1:3000)

# After
client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: "127.0.0.1:3000")
```

The preferred way to manage the client's lifecycle is `TigerBeetle::Client.open`,
which will automatically close the connection when it is no longer needed:

```rb
replica_addresses = ENV.fetch("TB_ADDRESS", "3000")

TigerBeetle::Client.open(cluster_id: 0, replica_addresses:) do |client|
  # Use the client.
end
```

When manually managing the client's lifecycle, `client.close` replaces the old
`client.deinit`.

See ["Creating a Client"](../README.md#creating-a-client) for details.

### Callback API

The callback-based async API has been removed. `TigerBeetle::Client` is now
fiber-scheduler aware and works with the [`async`](https://github.com/socketry/async)
gem out of the box.

```rb
# Before
client.lookup_accounts(100) do |result|
  result # [#<struct TigerBeetle::Account id=100, ... >]
end

# After
semaphore = Async::Semaphore.new(16)

account_batches = [...] # set up account batches

TigerBeetle::Client.open(cluster_id: 0, replica_addresses: "3000") do |client|
  Async do
    account_batches
      .map { |batch| semaphore.async { client.create_accounts(batch) } }
      .each(&:wait)
  end
end
```

### Splat parameters

All methods that previously took splat parameters now require explicit arrays:

```rb
# Before
account_1, account_2 = client.lookup_accounts(100, 101)

# After
account_1, account_2 = client.lookup_accounts([100, 101])
```

### Flag handling

All flags are now explicit numeric constants combined with `|`, not symbol arrays.

```rb
# Before
filter = TigerBeetle::AccountFilter.new(
  account_id: 100,
  limit: 10,
  flags: [:DEBITS, :CREDITS]
)

transfers = client.get_account_transfers(filter)

# After
filter = TigerBeetle::AccountFilter.new(
  account_id: 100,
  limit: 10,
  flags: TigerBeetle::AccountFilterFlags::DEBITS |
    TigerBeetle::AccountFilterFlags::CREDITS
)

transfers = client.get_account_transfers(filter)
```

### Returned objects

Returned objects have changed from `FFI::Struct`/`Struct`-style objects to
regular Ruby classes, so `[]` no longer works for accessing fields.

```rb
# Before
account[:debits_posted]

# After
account.debits_posted
```

The return types of create operations also changed from two-element arrays to
result objects:

```rb
# Before
index, status = result

# After
result.timestamp
result.status
```

### Exception classes

There was a change to the exception classes raised by the gem.

```rb
# Before
StandardError
  TigerBeetle::Error
    TigerBeetle::ClientError

# After
StandardError
  TigerBeetle::InitError
  TigerBeetle::ClientClosedError
  TigerBeetle::PacketError
```

### Logging

The `client.logger=` API has been removed. Any logging should happen in the
surrounding application code.

### `TB` top-level alias

The new gem provides a top-level `TB` alias which can be enabled as follows:

```rb
require "tigerbeetle/tb"

account = TB::Account.new(id: TB.id, ledger: 1, code: 1)
```

## Licensing

No licensing changes have been made. The gem remains under the [Apache License, Version 2.0](../LICENSE).
