# Install TigerBeetle, benchmark! and work your way through six demos...

- [Upgrade Ubuntu to the 5.7.15 kernel](#upgrade-ubuntu-to-the-5715-kernel)
- [Install Zig](#install-zig)
- [Clone TigerBeetle (and simulate non-pristine lab conditions)](#clone-tigerbeetle-and-simulate-non-pristine-lab-conditions)
- [Benchmark!](#benchmark)
- [Explore the API through six demos](#explore-the-api-through-six-demos)

## Upgrade Ubuntu to the 5.7.15 kernel

Here are instructions to get Ubuntu 20.04 to a recent kernel with io_uring. This should take less than a minute.

These are direct links to the 5.7.15 amd64 generic kernel files (note the "_all.deb" or "generic" keywords) you need:

```
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-headers-5.7.15-050715_5.7.15-050715.202008111432_all.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-headers-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-image-unsigned-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-modules-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb

sudo dpkg -i *.deb
sudo reboot

uname -sr
```

For newer than 5.7.15, you can also find the full list of [mainline releases here](https://kernel.ubuntu.com/~kernel-ppa/mainline/?C=N;O=D), but note that kernel 5.7.16 and up introduced a [network performance regression](https://github.com/axboe/liburing/issues/215) that has recently been patched.

## Install Zig

First, [download the Zig master binary](https://ziglang.org/download/) for your platform.

Next, from within your downloads directory, run:

```bash
rm -rf zig-cache
tar -xf zig-*.tar.xz
rm zig-*.tar.xz
sudo rm -rf /usr/local/lib/zig
sudo mv zig-* /usr/local/lib/zig
sudo ln -s --force /usr/local/lib/zig/zig /usr/local/bin/zig
zig version
```

You can also re-run the same steps above to update your Zig to latest master. Zig has a rapid release cadence at present and we are tracking master to keep pace.

## Clone TigerBeetle (and simulate non-pristine lab conditions)

```bash
git clone https://github.com/coilhq/tigerbeetle.git
cd tigerbeetle
```

You are ready to rock! To simulate non-pristine lab conditions (tiger beetles thrive in harsh environments with noisy neighbors) turn up your favorite album, or if you're looking for something new we suggest Noel Gallagher's High Flying Birds.

Be aware that super non-pristine lab conditions such as an active video call might cause cache pollution and drop throughput. But rock 'n roll is generally fine.

## Benchmark!

Launch the server with a clean journal:

```bash
rm -rf journal && ./tigerbeetle
```

In another tab:

```bash
zig run src/benchmark.zig -O ReleaseSafe
```

After each run of the benchmark, you must delete TigerBeetle's `journal` data file and restart the server to ensure a clean journal. The benchmark will abort if any accounts or transfers already exist.

## Explore the API through six demos

Take a look at the source code of these demos before you run them. Check out our screencast of these demos for much more detail.

Let's turn up the log level some more (and your favorite album) so you can see everything the server does as you run these demos:

* Open `src/tigerbeetle.conf` in your editor and change `log_level` to `7` (debug).

* Restart the server with a clean journal: `rm -rf journal && ./tigerbeetle`

### Demo 1 & 2: Create and lookup accounts

Let's create some accounts and check their balances and limits:

```bash
zig run src/demo_01_create_accounts.zig
zig run src/demo_02_lookup_accounts.zig
```

What happens if we create those accounts again?

```bash
zig run src/demo_01_create_accounts.zig
```

### Demo 3: Simple journal entries

Let's create some simple double-entry accounting journal entries (with *auto-commit* transfers):

```
zig run src/demo_03_auto_commit_transfers.zig
zig run src/demo_02_lookup_accounts.zig
```

### Demo 4, 5, 6: Two-phase commit journal entries

Let's try full two-phase commit transfers (create, and then commit):

*These two-phase commit transfers are designed for two-phase commit systems such as Mojaloop, where the fulfil packet only includes the transfer id and you want to avoid a lookup query roundtrip to the database before writing the compensating journal entry.*

You will see the second transfer is rejected with an error for tripping the debit reserved limit.

```
zig run src/demo_04_create_transfers.zig
zig run src/demo_02_lookup_accounts.zig
```

You will see these two-phase transfers only update the inflight reserved limits.

Let's commit (and accept):

Again, the second transfer is rejected because it was never created.

```
zig run src/demo_05_accept_transfers.zig
zig run src/demo_02_lookup_accounts.zig
```

Let's also pretend someone else tried to commit (but reject) concurrently:

```
zig run src/demo_06_reject_transfers.zig
```

**From here, feel free to tweak these demos and see what happens. You can explore all our accounting invariants (and the DSL we created for these) in `src/state.zig` by grepping the source for `fn create_account`, `fn create_transfer`, and `fn commit_transfer`.**

**Enjoy...**
