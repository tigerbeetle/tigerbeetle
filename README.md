# tigerbeetle

GitHub doesn't have a great way for you to stay in the loop. So don't
just star the repo, [join the mailing
list](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle)!

*TigerBeetle is a financial accounting database designed for mission critical safety and performance to power the future of financial services.*

**Take part in TigerBeetle's $20k consensus challenge: [Viewstamped Replication Made Famous](https://github.com/tigerbeetledb/viewstamped-replication-made-famous)**

Watch an introduction to TigerBeetle on [Zig SHOWTIME](https://www.youtube.com/watch?v=BH2jvJ74npM) for our design decisions regarding performance, safety, and financial accounting primitives:

[![A million financial transactions per second in Zig](https://img.youtube.com/vi/BH2jvJ74npM/0.jpg)](https://www.youtube.com/watch?v=BH2jvJ74npM)

Read more about the [history](./docs/HISTORY.md) of TigerBeetle, the problem of balance tracking at scale, and the solution of a purpose-built financial accounting database.

## TigerBeetle (under active development)

TigerBeetle is not yet production-ready. The production version of
**TigerBeetle is now under active development**. Our [DESIGN
doc](docs/DESIGN.md) provides an overview of TigerBeetle's data
structures.

Check out our [short-term roadmap](#short-term-roadmap) below for where we're heading!

## Documentation

Check out [docs.tigerbeetle.com](https://docs.tigerbeetle.com/).
Here are a few key pages you might be interested in:

- Deployment
  - [Hardware](https://docs.tigerbeetle.com/deployment/hardware)
- Usage
  - [Integration](https://docs.tigerbeetle.com/usage/integration)
- Reference
  - [Accounts](https://docs.tigerbeetle.com/reference/accounts)
  - [Transfers](https://docs.tigerbeetle.com/reference/transfers)
  - [Operations](https://docs.tigerbeetle.com/reference/operations)

## Quickstart

TigerBeetle is easy to run with or without Docker, depending on your
preference. First, we'll cover running the [Single
Binary](#single-binary). And below that is how to run [with
Docker](#with-docker).

### Single Binary

Install TigerBeetle by grabbing the latest release from
GitHub.

#### Prebuilt Linux binary

```bash
$ curl -LO https://github.com/tigerbeetledb/tigerbeetle/releases/download/2022-11-16-weekly/tigerbeetle-Linux-x64-2022-11-16-weekly.zip
$ unzip tigerbeetle-Linux-x64-2022-11-16-weekly.zip 
$ sudo cp tigerbeetle /usr/local/bin/tigerbeetle
$ tigerbeetle version --verbose | head -n6
TigerBeetle version experimental

git_commit="b47292aaf2492e6b56a977009b85f7fca6e66775"

zig_version=0.9.1
mode=Mode.ReleaseSafe
```

#### Prebuilt macOS binary

```bash
$ curl -LO https://github.com/tigerbeetledb/tigerbeetle/releases/download/2022-11-16-weekly/tigerbeetle-macOS-x64-2022-11-16-weekly.zip
$ unzip tigerbeetle-macOS-x64-2022-11-16-weekly.zip 
$ sudo cp tigerbeetle /usr/local/bin/tigerbeetle
$ tigerbeetle version --verbose | head -n6
TigerBeetle version experimental

git_commit="b47292aaf2492e6b56a977009b85f7fca6e66775"

zig_version=0.9.1
mode=Mode.ReleaseSafe
```

#### Building from source

Or to build from source, clone the repo, checkout a release, and run
the install script.

You will need POSIX userland, curl or wget, tar, and xz.

```bash
$ git clone https://github.com/tigerbeetledb/tigerbeetle.git
$ git checkout 2022-11-16-weekly # Or latest tag
$ cd tigerbeetle
$ scripts/install.sh
```

Don't worry, this will only make changes within the `tigerbeetle`
directory. No global changes. The result will place the compiled
`tigerbeetle` binary into the current directory.

#### Running TigerBeetle

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

Now skip ahead to [Use Node as a CLI](#use-node-as-a-cli).

### With Docker

First provision TigerBeetle's data directory.

```bash
$ docker run -v $(pwd)/data:/data ghcr.io/tigerbeetledb/tigerbeetle \
    format --cluster=0 --replica=0 /data/0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

Then run the server.

```bash
$ docker run -p 3000:3000 -v $(pwd)/data:/data ghcr.io/tigerbeetledb/tigerbeetle \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 0.0.0.0:3000
```

Note: if you are on macOS, you will need to call the Docker run
command with `--cap-add IPC_LOCK` or `--ulimit memlock=-1:-1`. See
[here](https://docs.tigerbeetle.com/deployment/with-docker#error-systemresources-on-macos) for
more information.

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

* [Running a 3-node cluster locally with docker-compose](https://docs.tigerbeetle.com/deployment/with-docker-compose)
* [Run a single-node cluster with Docker](https://docs.tigerbeetle.com/deployment/with-docker)
* [Run a single-node cluster from source](https://docs.tigerbeetle.com/deployment/single-binary)

## Clients

* For Node.js: [tigerbeetle-node](./src/clients/node)
* For Golang: [tigerbeetle-go](./src/clients/go)
* For Java: [tigerbeetle-java](./src/clients/java)
* For C# and Dotnet: [tigerbeetle-dotnet](./src/clients/dotnet)

## Community

* [Join the TigerBeetle community on Slack.](https://join.slack.com/t/tigerbeetle/shared_invite/zt-1gf3qnvkz-GwkosudMCM3KGbGiSu87RQ)
* [Subscribe to our monthly newsletter.](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle)

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

## Short-Term Roadmap

Done:

1. Consensus protocol readiness.

In progress:

2. Storage engine readiness.
  * Until this happens you'll occasionally see assertion failures in
    LSM code as we tune overtight or incorrect assertions, or as
    assertions detect bugs and shut TigerBeetle down safely.
  * See [#189](https://github.com/tigerbeetledb/tigerbeetle/issues/189) for details.
3. Recovery readiness.
  * See [#212](https://github.com/tigerbeetledb/tigerbeetle/issues/212) for details.
4. Support querying all transfers associated with an account id.
  * Until this is implemented you can only query individual accounts and transfers by id.

## License

Copyright 2023 TigerBeetle, Inc
Copyright 2020-2022 Coil Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
