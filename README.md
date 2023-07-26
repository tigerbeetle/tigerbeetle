# tigerbeetle

*TigerBeetle is a financial accounting database designed for mission critical safety and performance to power the future of financial services.*

TigerBeetle is not yet production-ready. The production version of
**TigerBeetle is now under active development**.

## Quickstart

First, download a prebuilt copy of TigerBeetle.

On macOS/Linux:

```console
$ git clone https://github.com/tigerbeetle/tigerbeetle; ./tigerbeetle/bootstrap.sh
```

On Windows:

```console
$ git clone https://github.com/tigerbeetle/tigerbeetle; .\tigerbeetle\bootstrap.ps1
```

Want to build from source locally? Add `-build` as an argument to the bootstrap script.

#### Running TigerBeetle

Then create the TigerBeetle data file.

```console
$ ./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 0_0.tigerbeetle
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

And start the replica.

```console
$ ./tigerbeetle start --addresses=3000 0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 127.0.0.1:3000
```

Now let's connect to the replica and do some accounting!

### Use Node as a CLI

First install the Node client.

```console
$ npm install tigerbeetle-node
```

Then create a client connection.

```console
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

* [Running a 3-node cluster locally with docker-compose](https://docs.tigerbeetle.com/quick-start/with-docker-compose)
* [Run a single-node cluster with Docker](https://docs.tigerbeetle.com/quick-start/with-docker)
* [Run a single-node cluster](https://docs.tigerbeetle.com/quick-start/single-binary)

## Next Steps

Watch an introduction to TigerBeetle on [Zig
SHOWTIME](https://www.youtube.com/watch?v=BH2jvJ74npM) for our design
decisions regarding performance, safety, and financial accounting
primitives:

[![A million financial transactions per second in
Zig](https://img.youtube.com/vi/BH2jvJ74npM/0.jpg)](https://www.youtube.com/watch?v=BH2jvJ74npM)

Read more about the [history](./docs/HISTORY.md) of TigerBeetle, the
problem of balance tracking at scale, and the solution of a
purpose-built financial accounting database.

Check out our [DESIGN doc](./docs/DESIGN.md) to see an overview of
TigerBeetle's data structures, take a look at our
[roadmap](https://github.com/tigerbeetle/tigerbeetle/issues/259), and
[join one of our communities](#Community) to stay in the loop about
fixes and features!

## Documentation

Check out [docs.tigerbeetle.com](https://docs.tigerbeetle.com/).

Here are a few key pages you might be interested in:

- Deployment
  - [Hardware](https://docs.tigerbeetle.com/deploy/hardware)
- Usage
  - [Integration](https://docs.tigerbeetle.com/#designing-for-tigerbeetle)
- Reference
  - [Accounts](https://docs.tigerbeetle.com/reference/accounts)
  - [Transfers](https://docs.tigerbeetle.com/reference/transfers)
  - [Operations](https://docs.tigerbeetle.com/reference/operations)

## Clients

* [.NET](https://docs.tigerbeetle.com/clients/dotnet)
* [Golang](https://docs.tigerbeetle.com/clients/go)
* [Java](https://docs.tigerbeetle.com/clients/java)
* [Node.js](https://docs.tigerbeetle.com/clients/node)

## Community

* [Projects using TigerBeetle developed by community members.](./docs/COMMUNITY_PROJECTS.md)
* [Join the TigerBeetle chat on Slack.](https://join.slack.com/t/tigerbeetle/shared_invite/zt-1gf3qnvkz-GwkosudMCM3KGbGiSu87RQ)
* [Follow us on Twitter](https://twitter.com/TigerBeetleDB), [YouTube](https://www.youtube.com/@tigerbeetledb), and [Twitch](https://www.twitch.tv/tigerbeetle).
* [Subscribe to our monthly newsletter for the backstory on recent database changes.](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle)
* [Check out past and upcoming talks.](/docs/TALKS.md)

## Benchmarks

First grab the sources and run the setup script:

```bash
$ git clone https://github.com/tigerbeetle/tigerbeetle.git
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

## Roadmap

See https://github.com/tigerbeetle/tigerbeetle/issues/259.

## License

Copyright 2023 TigerBeetle, Inc

Copyright 2020-2022 Coil Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
