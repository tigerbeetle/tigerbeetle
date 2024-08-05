# tigerbeetle

*TigerBeetle is a financial transactions database designed for mission critical safety and performance to power the next 30 years of OLTP.*

## Quickstart

First, download a prebuilt copy of TigerBeetle.

```console
# macOS
curl -Lo tigerbeetle.zip https://mac.tigerbeetle.com && unzip tigerbeetle.zip && ./tigerbeetle version

# Linux
curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip && ./tigerbeetle version

# Windows
powershell -command "curl.exe -Lo tigerbeetle.zip https://windows.tigerbeetle.com; Expand-Archive tigerbeetle.zip .; .\tigerbeetle version"
```

Want to build from source locally?

```console
git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle
./zig/download.sh # or .bat if you're on Windows.
./zig/zig build
./tigerbeetle version
```

#### Running TigerBeetle

Then create the TigerBeetle data file.

```console
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development 0_0.tigerbeetle
```
```console
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

And start the replica.

```console
./tigerbeetle start --addresses=3000 --development 0_0.tigerbeetle
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
./tigerbeetle repl --cluster=0 --addresses=3000
```
```console
TigerBeetle Client
  Hit enter after a semicolon to run a command.

Examples:
  create_accounts id=1 code=10 ledger=700 flags=linked|history,
                  id=2 code=10 ledger=700;
  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
  lookup_accounts id=1;
  lookup_accounts id=1, id=2;
  get_account_transfers account_id=1 flags=debits|credits;
  get_account_balances account_id=1 flags=debits|credits;
```
```console
create_accounts id=1 code=10 ledger=700,
                id=2 code=10 ledger=700;
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
* [Run a single-node cluster](https://docs.tigerbeetle.com/quick-start)
* [Run a three-node cluster](https://docs.tigerbeetle.com/quick-start/#optional-run-a-multi-node-cluster)
* [Run on docker](https://docs.tigerbeetle.com/operating/docker)

## Next Steps

Watch an introduction to TigerBeetle on [The Primeagen](https://www.youtube.com/watch?v=sC1B3d9C_sI) for our design
decisions regarding performance, safety, and financial accounting debit/credit
primitives:

[![The FASTEST and SAFEST Database
](https://img.youtube.com/vi/sC1B3d9C_sI/0.jpg)](https://www.youtube.com/watch?v=sC1B3d9C_sI)

Read more about the [history](./docs/about/README.md#history) of TigerBeetle, the
problem of balance tracking at scale, and the solution of a
purpose-built financial transactions database.

Check out our [DESIGN doc](./docs/DESIGN.md) to see an overview of
TigerBeetle's data structures, take a look at our
[roadmap](https://github.com/tigerbeetle/tigerbeetle/issues/259), and
[join one of our communities](#Community) to stay in the loop about
fixes and features!

## Documentation

Check out [docs.tigerbeetle.com](https://docs.tigerbeetle.com/).

Here are a few key pages you might be interested in:

- Deployment
  - [Hardware](https://docs.tigerbeetle.com/deploy/hardware/)
- Usage
  - [Integration](https://docs.tigerbeetle.com/coding/system-architecture)
- Reference
  - [Account](https://docs.tigerbeetle.com/reference/account)
  - [Transfer](https://docs.tigerbeetle.com/reference/transfer)
  - [Requests](https://docs.tigerbeetle.com/reference/requests)

## Clients

* [.NET](https://docs.tigerbeetle.com/clients/dotnet)
* [Go](https://docs.tigerbeetle.com/clients/go)
* [Java](https://docs.tigerbeetle.com/clients/java)
* [Node.js](https://docs.tigerbeetle.com/clients/node)

## Community

* [Projects using TigerBeetle developed by community members.](./docs/COMMUNITY_PROJECTS.md)
* [Join the TigerBeetle chat on Slack.](https://slack.tigerbeetle.com/invite)
* [Follow us on Twitter](https://twitter.com/TigerBeetleDB), [YouTube](https://www.youtube.com/@tigerbeetledb), and [Twitch](https://www.twitch.tv/tigerbeetle).
* [Subscribe to our monthly newsletter for the backstory on recent database changes.](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle)
* [Check out past and upcoming talks.](/docs/TALKS.md)

## Contributing

Read [docs/HACKING.md](docs/HACKING.md).

## Roadmap

See https://github.com/tigerbeetle/tigerbeetle/issues/259.

## License
Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
