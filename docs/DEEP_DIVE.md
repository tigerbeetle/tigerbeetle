# Install TigerBeetle, benchmark! and work your way through six demos...

- [Clone TigerBeetle](#clone-tigerbeetle)
- [Install Zig](./HACKING.md#setup)
- [Simulate non-pristine lab conditions](#simulate-non-pristine-lab-conditions)
- [Benchmark!](#benchmark)
- [Explore the API through six demos](#explore-the-api-through-six-demos)

## Video walkthrough

[![Video walkthrough on Youtube](https://img.youtube.com/vi/lQSIVgvea48/0.jpg)](https://www.youtube.com/watch?v=lQSIVgvea48)

## Clone TigerBeetle

```bash
git clone https://github.com/tigerbeetle/tigerbeetle.git
cd tigerbeetle/
```

## Install Zig and build TigerBeetle

```bash
scripts/install.sh
```

## Simulate non-pristine lab conditions

You are ready to rock! To simulate non-pristine lab conditions (tiger beetles thrive in harsh environments with noisy neighbors), turn up your favorite album, or if you're looking for something new, we suggest Noel Gallagher's High Flying Birds.

Be aware that super non-pristine lab conditions such as an active video call might cause cache pollution and drop throughput. But rock 'n roll is generally fine.

## Benchmark!

With TigerBeetle installed, you are ready to benchmark!

```bash
scripts/benchmark.sh
```

## Explore the API through six demos

Take a look at the source code of these demos before you run them. Check out our screencast of these demos for much more detail (note that there have been a few changes to our types since the screencast).

Let's turn up the log level some more (and your favorite album), so you can see everything the server does as you run these demos:

- Open `src/config.zig` in your editor and change `log_level` to `.debug`.

- Rebuild TigerBeetle using the new debug log level by running `zig/zig build -Drelease && mv zig-out/bin/tigerbeetle .`

- Start a single replica cluster:
Format the data file:
`./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 0_0.tigerbeetle`
Start the replica from the data file:
`./tigerbeetle start --addresses=3001 0_0.tigerbeetle &`

### Demo 1, 2: Create and lookup accounts

Let's create some accounts and check their balances and limits:

```bash
zig/zig run src/demos/demo_01_create_accounts.zig --pkg-begin vsr src/vsr.zig
zig/zig run src/demos/demo_02_lookup_accounts.zig --pkg-begin vsr src/vsr.zig
```

What happens if we create those accounts again?

```bash
zig/zig run src/demos/demo_01_create_accounts.zig --pkg-begin vsr src/vsr.zig
```

### Demo 3: Simple journal entries

Let's create some simple double-entry accounting journal entries:

```bash
zig/zig run src/demos/demo_03_create_transfers.zig --pkg-begin vsr src/vsr.zig
# Now let's look at both accounts and the transfers for those accounts:
zig/zig run src/demos/demo_02_lookup_accounts.zig --pkg-begin vsr src/vsr.zig
zig/zig run src/demos/demo_07_lookup_transfers.zig --pkg-begin vsr src/vsr.zig
```

### Demo 4, 5, 6: Two-phase transfer journal entries

Let's try full two-phase transfers (create and then post):

*These two-phase transfers are designed for two-phase systems such as Interledger or Mojaloop, where the fulfill packet only includes the transfer id, and you want to avoid a lookup query roundtrip to the database before writing the compensating journal entry.*

You will see the second transfer is rejected with an error for tripping the debit reserved limit.

```bash
zig/zig run src/demos/demo_04_create_pending_transfers.zig --pkg-begin vsr src/vsr.zig
zig/zig run src/demos/demo_02_lookup_accounts.zig --pkg-begin vsr src/vsr.zig
```

You will see these two-phase transfers only update the reserved inflight limits.

Let's post (and accept):

Again, the second transfer is rejected because it was never created.

```bash
zig/zig run src/demos/demo_05_post_pending_transfers.zig --pkg-begin vsr src/vsr.zig
# At this point, Account[1] has reached its credit limit (no more debit transfers allowed).
zig/zig run src/demos/demo_02_lookup_accounts.zig --pkg-begin vsr src/vsr.zig
```

Let's also pretend someone else tried to void the pending transfer concurrently:

```bash
zig/zig run src/demos/demo_06_void_pending_transfers.zig --pkg-begin vsr src/vsr.zig
```

**From here, feel free to tweak these demos and see what happens. You can explore all our accounting invariants (and the DSL we created for these) in `src/state_machine.zig` by grepping the source for `fn create_account` or `fn create_transfer`.**

**Enjoy...**
