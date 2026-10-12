<!-- This file is specific to the rust client and *not* auto-generated -->
# Zero Allocation Rust Sample

Code for this sample is in [./src/main.rs](./src/main.rs).

## Prerequisites

Linux >= 5.6 is the only production environment we support.
But for ease of development we also support macOS and Windows.
* Rust 1.68+

## Setup

First, clone this repo and `cd` into `tigerbeetle/src/clients/rust/samples/zero-allocation`.

Then, install the TigerBeetle client:

## Start the TigerBeetle server

Follow steps in the repo README to [run TigerBeetle](/README.md#running-tigerbeetle).

If you are not running on port `localhost:3000`, set the environment variable `TB_ADDRESS`
to the full address of the TigerBeetle server you started.

## Run this sample

Now you can run this sample:

```console
cargo run
```

## Walkthrough

The goal of this project is to showcase how the rust client can be used in a way that avoids
unnecessary allocations and copying. Here's what this project does:

- The sample pre-allocates all memory up front: a reusable completion, input, and result vectors.
- It then creates 128 accounts.
- It sends multiple batches of up to 128 transfers, continuously reusing the pre-allocated memory.
- Once it has created 5000 transfers, it stops.
