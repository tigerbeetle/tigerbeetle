# io_uring file system and network benchmarks

## File system benchmarks

### fs_blocking.zig

Uses blocking syscalls to write a page of 4096 bytes, fsync the write, read the page back in, and repeats to iterate across a large file of 256 MB. This benchmark, while obviously artificial, and while using the minimum [Advanced Format](https://en.wikipedia.org/wiki/Advanced_Format) sector size instead of a larger block size, is not too far removed from what some storage systems in paranoid mode might do to protect against misdirected writes or LSEs for critical data. This should be the fastest candidate for the task on Linux, faster than Zig's evented I/O since that incurs additional context switches to the userspace I/O thread.

### fs_io_uring.zig

Uses `io_uring` syscalls to write a page of 4096 bytes, fsync the write, read the page back in, and repeats to iterate across a large file of 256 MB. This is all non-blocking, with a pure single-threaded event loop, something that is not otherwise possible on Linux for file system I/O apart from `O_DIRECT`. This is how we solve [#1908](https://github.com/ziglang/zig/issues/1908) and [#5962](https://github.com/ziglang/zig/issues/5962) for Zig.

There are also further `io_uring` optimizations that we don't take advantage of here in this benchmark:

* `SQPOLL` to eliminate submission syscalls entirely.
* Registered file descriptors to eliminate atomic file referencing in the kernel.
* Registered buffers to eliminate page mapping in the kernel.

### File system results

These were taken on a 2020 MacBook Air running Ubuntu 20.04 with a 5.7.15 kernel. Some Linux machines may further show an order of magnitude worse performance for `fs_blocking.zig`:

```bash
$ zig run fs_blocking.zig --release-fast
fs blocking: write(4096)/fsync/read(4096) * 65536 pages = 196608 syscalls in 4138ms
fs blocking: write(4096)/fsync/read(4096) * 65536 pages = 196608 syscalls in 3626ms
fs blocking: write(4096)/fsync/read(4096) * 65536 pages = 196608 syscalls in 4428ms
fs blocking: write(4096)/fsync/read(4096) * 65536 pages = 196608 syscalls in 3693ms
fs blocking: write(4096)/fsync/read(4096) * 65536 pages = 196608 syscalls in 4153ms
```

```bash
$ zig run fs_io_uring.zig --release-fast
fs io_uring: write(4096)/fsync/read(4096) * 65536 pages = 193 syscalls in 3024ms
fs io_uring: write(4096)/fsync/read(4096) * 65536 pages = 193 syscalls in 3261ms
fs io_uring: write(4096)/fsync/read(4096) * 65536 pages = 193 syscalls in 2482ms
fs io_uring: write(4096)/fsync/read(4096) * 65536 pages = 193 syscalls in 2890ms
fs io_uring: write(4096)/fsync/read(4096) * 65536 pages = 193 syscalls in 2513ms
```

As you can see, `io_uring` wins by amortizing the cost of syscalls. What you don't see here, though, is that your single-threaded application is now also non-blocking, so you could spend the I/O time doing CPU-intensive work such as encrypting your next write, or authenticating your last read, while you wait for I/O to complete. **This is cycles for jam.**

## Network benchmarks

### net_blocking.zig

Uses blocking syscalls to `accept` at most one TCP connection and then `recv`/`send` up to 1000 bytes per message on this connection as an echo server. This should be the fastest candidate for the task on Linux, since it does not use `epoll` or any async methods.

*This was designed by [MasterQ32](https://github.com/MasterQ32), Felix (xq) Queißner, who wrote [zig-network](https://github.com/MasterQ32/zig-network). Thanks for jumping in and creating this echo server candidate!*

### net_io_uring.zig

Uses `io_uring` syscalls to `accept` one or more TCP connections and then `recv`/`send` up to 1000 bytes per message on these connections as an echo server. This server is non-blocking and takes advantage of kernel 5.6 or higher with support for `IORING_FEAT_FAST_POLL`.

**Note that kernel 5.7.16 and up introduces a [network performance regression](https://github.com/axboe/liburing/issues/215) that is being patched. If you can't reproduce these network performance results then check that you are on kernel 5.7.15.

### C Contenders (and a Node.js candidate)

We also throw two C contenders in the ring:

* [`epoll.c`](https://github.com/frevib/epoll-echo-server)
* [`io_uring.c`](https://github.com/frevib/io_uring-echo-server/tree/master)

...and a Node.js candidate that simply does `socket.pipe(socket)`. The Node.js candidate is intended for those coming from Node.js, and is provided only to show the full range of the spectrum, so that we can have low-level and high-level echo-server candidates. JavaScript is not a slow language in itself, and [major network performance improvements for Node.js are still possible](https://github.com/nodejs/node/pull/6923).

### Network results

We use [`rust_echo_bench`](https://github.com/haraldh/rust_echo_bench) to send and receive 512-byte payloads for 20 seconds, varying the number of connections, doing several runs per benchmark and taking the best of each run:

```bash
cargo run --release -- --address "localhost:3001" --number  1 --duration 20 --length 512
cargo run --release -- --address "localhost:3001" --number  2 --duration 20 --length 512
cargo run --release -- --address "localhost:3001" --number 50 --duration 20 --length 512
```

These were taken on a 2020 MacBook Air running Ubuntu 20.04 with a 5.7.15 kernel:

<table>
  <tr>
    <td><strong>Connections</strong></td><td><strong>1</strong></td><td><strong>2</strong></td><td><strong>50</strong></td>
  </tr>
  <tr>
    <td>epoll.c</td><td>75775</td><td>119451</td><td>111572</td>
  </tr>
  <tr>
    <td>io_uring.c</td><td>68717</td><td>121236</td><td>140814</td>
  </tr>
  <tr>
    <td>node.js</td><td>43694</td><td>52635</td><td>52977</td>
  </tr>
  <tr>
    <td>blocking.zig</td><td>77588</td><td>N/A</td><td>N/A</td>
  </tr>
  <tr>
    <td>io_uring.zig</td><td>74770</td><td>121432</td><td>135685</td>
  </tr>
</table>

An echo server benchmark is a tough benchmark for `io_uring` because it's read-then-write, read-then-write, so for a single connection there's no opportunity for `io_uring` to amortize syscalls across connections. However, as the number of connections increases, `io_uring` should outperform `epoll`.

Bear in mind that these network benchmarks are not as stable across machines as the file system benchmarks, so you may get different numbers. The performance of networking stacks on Linux is also heavily optimized.

What is unique about `io_uring` here is that the same simple interface can be used for both file system and networking I/O on Linux without resorting to user-space thread pools to emulate async file system I/O. We are also relying on the kernel to do fast polling for us thanks to `IORING_FEAT_FAST_POLL`, without having to use `epoll`.

## What this means for the future of event loops

Rough benchmarks aside, more importantly, `io_uring` optimizations such as registered fds, registered buffers enable **a whole new way of doing I/O syscalls not possible with the blocking syscall alternatives**, allowing the kernel to take long term references to internal data structures or create long term mappings of application memory, greatly reducing per-I/O overhead.

In the past, event loops took existing interfaces for blocking syscalls and made them asynchronous. But now, the `io_uring` interfaces are more powerful than the blocking alternatives, so this changes everything.

This means that future event loop designs may need to:

1. Design for `io_uring` first, and fallback to older polling methods and blocking syscall threadpools second.

2. Use shims for advanced `io_uring` features such as [automatic buffer selection](https://lwn.net/Articles/815491/) as fallbacks where `io_uring` is not available, to ensure that the event loop abstraction can expose the full interface of `io_uring` without compromising performance for platforms where `io_uring` is available.

In other words, if you want to know what an I/O interface on Linux should look like, start with `io_uring`, don't retrofit `io_uring` onto an existing event loop design, and think of how your event loop or networking library will expose features such as [automatic buffer selection](https://lwn.net/Articles/815491/).
