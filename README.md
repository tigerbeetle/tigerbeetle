# tigerbeetle

*TigerBeetle is a financial accounting database designed for mission critical safety and performance to power the future of financial services.*

**Take part in TigerBeetle's $20k consensus challenge: [Viewstamped Replication Made Famous](https://github.com/tigerbeetledb/viewstamped-replication-made-famous)**

Watch an introduction to TigerBeetle on [Zig SHOWTIME](https://www.youtube.com/watch?v=BH2jvJ74npM) for our design decisions regarding performance, safety, and financial accounting primitives:

[![A million financial transactions per second in Zig](https://img.youtube.com/vi/BH2jvJ74npM/0.jpg)](https://www.youtube.com/watch?v=BH2jvJ74npM)

Read more about the [history](./docs/HISTORY.md) of TigerBeetle, the problem of balance tracking at scale, and the solution of a purpose-built financial accounting database.

## TigerBeetle (under active development)

TigerBeetle is not yet production-ready. The production version of **TigerBeetle is now under active development**. Our [DESIGN doc](docs/DESIGN.md) provides an overview of TigerBeetle's data structures and our [project board](https://github.com/tigerbeetledb/tigerbeetle/projects?type=classic) provides a glimpse of where we want to go.

## QuickStart

TigerBeetle is easy to run with or without Docker, depending on your
preference.

### With Docker

First provision TigerBeetle's data directory.

```bash
$ docker run -v $(pwd)/data:/data ghcr.io/coilhq/tigerbeetle \
    format --cluster=0 --replica=0 /data/0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

Then run the server.

```bash
$ docker run -p 3000:3000 -v $(pwd)/data:/data ghcr.io/coilhq/tigerbeetle \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 0.0.0.0:3000
```

### From Source

To build from source, clone the repo and run the install script.

```bash
$ git clone https://github.com/tigerbeetledb/tigerbeetle.git
$ cd tigerbeetle
$ scripts/install.sh
```

Don't worry, this will only make changes within the `tigerbeetle`
directory. No global changes.

Then create the TigerBeetle data file.

```bash
$ ./tigerbeetle format --cluster=0 --replica=0 0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

And start the server.

```bash
$ ./tigerbeetle start --addresses=3000 0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 127.0.0.1:3000
```

### Use Node as a CLI

Now that you've got the server running with or without Docker, let's
connect to the running server and do some accounting!

First install the Node client.

```javascript
$ npm install -g tigerbeetle-node
```

Then create a client connection.

```javascript
$ node
Welcome to Node.js v16.14.0.
Type ".help" for more information.
> let { createClient } = require('tigerbeetle-node');
> let client = createClient({ cluster_id: 0, replica_addresses: ['3000'] });
info(message_bus): connected to replica 0
```

Now create two accounts. (Don't worry about the details, you can
read about them later.)

```javascript
> let errors = await client.createAccounts([
  {
    id: 1n,
    ledger: 1,
    code: 718,
    user_data: 0n,
    reserved: Buffer.alloc(48, 0),
    flags: 0,
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: 0n,
    timestamp: 0n,
  },
  {
    id: 2n,
    ledger: 1,
    code: 718,
    user_data: 0n,
    reserved: Buffer.alloc(48, 0),
    flags: 0,
    debits_pending: 0n,
    debits_posted: 0n,
    credits_pending: 0n,
    credits_posted: 0n,
    timestamp: 0n,
  },
]);
> errors
[]
```

Now create a transfer of `10` (of some amount/currency) between the two accounts.

```javascript
> errors = await client.createTransfers([
  {
    id: 1n,
    debit_account_id: 1n,
    credit_account_id: 2n,
    pending_id: 0n,
    user_data: 0n,
    reserved: 0n,
    timeout: 0n,
    ledger: 1,
    code: 718,
    flags: 0,
    amount: 10n,
    timestamp: 0n,
  }
]);
```

Now, the amount of `10` has been credited to account `2` and debited
from account `1`. Let's query TigerBeetle for these two accounts to
verify!

```javascript
> let accounts = await client.lookupAccounts([1n, 2n]);
> console.log(accounts.map(a => ({
    id: a.id,
	debits_posted: a.debits_posted,
	credits_posted: a.credits_posted,
	timestamp: a.timestamp,
  })));
[
  {
    id: 1n,
    debits_posted: 10n,
    credits_posted: 0n,
    timestamp: 1662489240014463675n
  },
  {
    id: 2n,
    debits_posted: 0n,
    credits_posted: 10n,
    timestamp: 1662489240014463676n
  }
]
```

And indeed you can see that account `1` has `debits_posted` as `10`
and account `2` has `credits_posted` as `10`. The `10` amount is fully
accounted for!

For further reading:

* [Running a 3-node cluster locally with docker-compose](./docs/DOCKER_COMPOSE.md)

## Clients

* For Node.js: [tigerbeetle-node](https://github.com/tigerbeetledb/tigerbeetle-node)
* For Golang: [tigerbeetle-go](https://github.com/tigerbeetledb/tigerbeetle-go)
* For Java: [tigerbeetle-java](https://github.com/tigerbeetledb/tigerbeetle-java)

## Community

[Join the TigerBeetle community on Slack.](https://join.slack.com/t/tigerbeetle/shared_invite/zt-1gf3qnvkz-GwkosudMCM3KGbGiSu87RQ)

## Benchmarks

First grab the sources and run the setup script:

```bash
$ git clone https://github.com/tigerbeetledb/tigerbeetle.git
$ cd tigerbeetle
$ scripts/install.sh
```

With TigerBeetle installed, you are ready to benchmark!

```bash
$ scripts/benchmark.sh
```

*If you encounter any benchmark errors, please send us the resulting `benchmark.log`.*

## Contributing

Read [docs/HACKING.md](docs/HACKING.md).

## Performance Demos

Along the way, we also put together a series of performance demos and sketches to get you comfortable building TigerBeetle, show how low-level code can sometimes be easier than high-level code, help you understand some of the key components within TigerBeetle, and enable back-of-the-envelope calculations to motivate design decisions.

You may be interested in:

* [demos/protobeetle](./demos/protobeetle), how batching changes everything.
* [demos/bitcast](./demos/bitcast), how Zig makes zero-overhead network deserialization easy, fast and safe.
* [demos/io_uring](./demos/io_uring), how ring buffers can eliminate kernel syscalls, reduce server hardware requirements by a factor of two, and change the way we think about event loops.
* [demos/hash_table](./demos/hash_table), how linear probing compares with cuckoo probing, and what we look for in a hash table that needs to scale to millions (and billions) of account transfers.

## License

Copyright 2020-2022 Coil Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
