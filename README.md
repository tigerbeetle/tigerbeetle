# tigerbeetle

*TigerBeetle is a financial accounting database designed for mission critical safety and performance to power the future of financial services.*

TigerBeetle is not yet production-ready. In particular, the protocol and data file formats may change and [might not be compatible across different commits](https://github.com/tigerbeetle/tigerbeetle/issues/1109), while we fine-tune the format ahead of release.

The production version of **TigerBeetle is now under active development**.

## Quickstart

First, download a prebuilt copy of TigerBeetle.

On macOS/Linux:

```console
git clone https://github.com/tigerbeetle/tigerbeetle; cd tigerbeetle; ./bootstrap.sh
```

On Windows:

```console
git clone https://github.com/tigerbeetle/tigerbeetle; cd tigerbeetle; .\bootstrap.ps1
```

Want to build from source locally? Add `-build` as an argument to the bootstrap script.

#### Running TigerBeetle

Then create the TigerBeetle data file.

```console
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 0_0.tigerbeetle
```
```console
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

And start the replica.

```console
./tigerbeetle start --addresses=3000 0_0.tigerbeetle
```
```console
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 127.0.0.1:3000
```

### Using the CLI Client

Now that you've got a cluster running, let's connect to it and do some
accounting!

First let's create two accounts. (Don't worry about the details, you
can read about them later.)

```console
./tigerbeetle client --cluster=0 --addresses=3000
```
```console
TigerBeetle Client
  Hit enter after a semicolon to run a command.

Examples:
  create_accounts id=1 code=10 ledger=700,
                  id=2 code=10 ledger=700;
  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
  lookup_accounts id=1;
  lookup_accounts id=1, id=2;
```
```console
create_accounts id=1 code=10 ledger=700,
                id=2 code=10 ledger=700;
```
```console
info(message_bus): connected to replica 0
```

Now create a transfer of `10` (of some amount/currency) between the two accounts.

```console
create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
```

Now, the amount of `10` has been credited to account `2` and debited
from account `1`. Let's query TigerBeetle for these two accounts to
verify!

```console
lookup_accounts id=1, id=2;
```
```json
{
  "id": "1",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": "",
  "debits_pending": "0",
  "debits_posted": "10",
  "credits_pending": "0",
  "credits_posted": "0"
}
{
  "id": "2",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": "",
  "debits_pending": "0",
  "debits_posted": "0",
  "credits_pending": "0",
  "credits_posted": "10"
}
```

And indeed you can see that account `1` has `debits_posted` as `10`
and account `2` has `credits_posted` as `10`. The `10` amount is fully
accounted for!

For further reading:

* [Run a single-node cluster](https://docs.tigerbeetle.com/quick-start/single-binary)
* [Run a three-node cluster](https://docs.tigerbeetle.com/quick-start/single-binary-three)

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
* [Go](https://docs.tigerbeetle.com/clients/go)
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

```console
git clone https://github.com/tigerbeetle/tigerbeetle.git
cd tigerbeetle
scripts/install.sh
```

With TigerBeetle installed, you are ready to benchmark!

```console
scripts/benchmark.sh
```

*If you encounter any benchmark errors, please send us the resulting `benchmark.log`.*

## Contributing

Read [docs/HACKING.md](docs/HACKING.md).

## Roadmap

See https://github.com/tigerbeetle/tigerbeetle/issues/259.

## License
Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
