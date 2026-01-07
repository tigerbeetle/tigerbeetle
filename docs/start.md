# Start

TigerBeetle is a reliable, fast, and highly available database for financial accounting. It tracks
financial transactions or anything else that can be expressed as double-entry bookkeeping, providing
three orders of magnitude more performance and guaranteeing durability even in the face of network,
machine, and storage faults. You will learn more about why this is an important and hard problem to
solve in the [Concepts](./concepts/) section, but let's make some real transactions first!

## Install

TigerBeetle is a single, small, statically linked binary.

You can download a pre-built binary from `tigerbeetle.com`:

<details open>
<summary>Linux</summary>

```console
curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip
./tigerbeetle version
```
</details>

<details>
<summary>macOS</summary>

```console
curl -Lo tigerbeetle.zip https://mac.tigerbeetle.com && unzip tigerbeetle.zip
./tigerbeetle version
```
</details>

<details>
<summary>Windows</summary>

```console
powershell -command "curl.exe -Lo tigerbeetle.zip https://windows.tigerbeetle.com; Expand-Archive tigerbeetle.zip ."
.\tigerbeetle version
```
</details>

See [Installing](./operating/installing.md) for other options.

## Run a Cluster

Typically, TigerBeetle is deployed as a cluster of 6 replicas, which is described in the
[Operating](./operating/) section. But it is also possible to run a single-replica cluster, which of
course doesn't provide high-availability, but is convenient for experimentation. That's what we'll
do here.

First, format a data file:

```console
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development ./0_0.tigerbeetle
```

A TigerBeetle replica stores everything in a single file (`./0_0.tigerbeetle` in this case).
The `--cluster`, `--replica`, and `--replica-count` arguments set the topology of the cluster (a
single replica for this tutorial).

Now, start a replica:

```console
./tigerbeetle start --addresses=3000 --development ./0_0.tigerbeetle
```

It will listen on port 3000 for connections from clients. There's intentionally no way to gracefully
shut down a replica. You can `^C` it freely, and the data will be safe as long as the underlying
storage functions correctly. Note that with a real cluster of 6 replicas, the data is safe even if
the storage misbehaves.

## Connecting to a Cluster

Now that the cluster is running, we can connect to it using a client. TigerBeetle already has
clients for several popular programming languages, including Go, Node.js, Java, and Python, and more
are coming; see the [Coding](./coding) section for details. For this tutorial, we'll keep it simple
and connect to the cluster using the built-in CLI client. In a separate terminal, start a REPL with

```console
./tigerbeetle repl --cluster=0 --addresses=3000
```

The `--addresses` argument is the port the server is listening on. The `--cluster` argument is
required to double-check that the client connects to the correct cluster. While not strictly
necessary, it helps prevent operator errors.

## Issuing Transactions

TigerBeetle comes with a pre-defined database schema --- double-entry bookkeeping. The [Concept](./concepts)
section explains why this particular schema, and the [Reference](./reference) documents all the bells and
whistles, but, for the purposes of this tutorial, it is enough to understand that there are accounts
holding `credits` and `debits` balances, and that each transfer moves value between two accounts by
incrementing `credits` on one side and `debits` on the other.

In the REPL, let's create two empty accounts:

```console
> create_accounts id=1 code=10 ledger=700, id=2 code=10 ledger=700;
> lookup_accounts id=1, id=2;
```

```json
{
  "id": "1",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": [],
  "debits_pending": "0",
  "debits_posted": "0",
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
  "credits_posted": "0"
}
```

Now, create our first transfer and inspect the state of accounts afterwards:

```console
> create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
> lookup_accounts id=1, id=2;
```

```json
{
  "id": "1",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": [],
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

Note how the transfer amount is added both to the credits and to the debits. That the sum of debits
and credits stays equal no matter what is a powerful invariant of the double-entry bookkeeping
system.

## Conclusion

This is the end of the quick start! You now know how to format a data file, run a single-replica
TigerBeetle cluster, and run transactions through it. Here's where to go from here:

* [Concepts](./concepts/) explains the "why?" of TigerBeetle, read this to decide if you need to use
  TigerBeetle.
* [Coding](./coding/) gives guidance on developing applications which store accounting data in a
  TigerBeetle cluster.
* [Operating](./operating/) explains how to deploy a TigerBeetle cluster in a highly-available
  manner, with replication enabled.
* [Reference](./reference/) meticulously documents every single available feature and flag of the
  underlying data model.

## Community

If you want to keep up to speed with recent TigerBeetle developments, here are some things to
follow:

- [Monthly Newsletter](https://mailchi.mp/8e9fa0f36056/subscribe-to-tigerbeetle) covers everything
  of importance that happened with TigerBeetle. It is a changelog director's cut!
- [Slack](https://slack.tigerbeetle.com/join) is the place to hang out with users and developers
  of TigerBeetle. We try to answer every question.
- [YouTube](https://www.youtube.com/@tigerbeetledb) channel has most of the talks about TigerBeetle,
  as well as talks from the Systems Distributed conference. We also stream on
  [Twitch](https://www.twitch.tv/tigerbeetle), with recordings duplicated to YouTube.
- [𝕏](https://twitter.com/TigerBeetleDB) is good for smaller updates, and word-of-mouth historical
  trivia you won't learn elsewhere!
- [GitHub](https://github.com/tigerbeetle/tigerbeetle) --- if you want to keep closer to the source!
