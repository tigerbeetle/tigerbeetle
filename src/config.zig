const std = @import("std");
const assert = std.debug.assert;
const tigerbeetle = @import("tigerbeetle.zig");
const vsr = @import("vsr.zig");

const Environment = enum {
    development,
    production,
    simulation,
};

/// Whether development or production:
pub const deployment_environment: Environment =
    if (@hasDecl(@import("root"), "deployment_environment")) @import("root").deployment_environment else .development;

/// The maximum log level in increasing order of verbosity (emergency=0, debug=3):
pub const log_level = 2;

/// The maximum number of replicas allowed in a cluster.
pub const replicas_max = 6;

/// The maximum number of clients allowed per cluster, where each client has a unique 128-bit ID.
/// This impacts the amount of memory allocated at initialization by the server.
/// This determines the size of the VR client table used to cache replies to clients by client ID.
/// Each client has one entry in the VR client table to store the latest `message_size_max` reply.
pub const clients_max = 32;

/// The minimum number of nodes required to form a quorum for replication:
/// Majority quorums are only required across view change and replication phases (not within).
/// As per Flexible Paxos, provided `quorum_replication + quorum_view_change > replicas`:
/// 1. you may increase `quorum_view_change` above a majority, so that
/// 2. you can decrease `quorum_replication` below a majority, to optimize the common case.
/// This improves latency by reducing the number of nodes required for synchronous replication.
/// This reduces redundancy only in the short term, asynchronous replication will still continue.
/// The size of the replication quorum is limited to the minimum of this value and actual majority.
/// The size of the view change quorum will then be automatically inferred from quorum_replication.
pub const quorum_replication_max = 3;

/// The default server port to listen on if not specified in `--addresses`:
pub const port = 3001;

/// The default network interface address to listen on if not specified in `--addresses`:
/// WARNING: Binding to all interfaces with "0.0.0.0" is dangerous and opens the server to anyone.
/// Bind to the "127.0.0.1" loopback address to accept local connections as a safe default only.
pub const address = "127.0.0.1";

/// The default maximum amount of memory to use.
pub const memory_size_max_default = 1024 * 1024 * 1024;

/// The maximum number of accounts to store in memory:
/// This impacts the amount of memory allocated at initialization by the server.
pub const cache_accounts_max = switch (deployment_environment) {
    .production => 64 * 1024,
    else => 8 * 1024,
};

/// The maximum number of transfers to store in memory:
/// This impacts the amount of memory allocated at initialization by the server.
/// We allocate more capacity than the number of transfers for a safe hash table load factor.
pub const cache_transfers_max = switch (deployment_environment) {
    .production => 1024 * 1024,
    else => 64 * 1024,
};

/// The maximum number of two-phase transfers to store in memory:
/// This impacts the amount of memory allocated at initialization by the server.
pub const cache_transfers_pending_max = cache_transfers_max;

/// The maximum number of batch entries in the journal file:
/// A batch entry may contain many transfers, so this is not a limit on the number of transfers.
/// We need this limit to allocate space for copies of batch headers at the start of the journal.
/// These header copies enable us to disentangle corruption from crashes and recover accordingly.
pub const journal_slot_count = switch (deployment_environment) {
    .production => 1024,
    else => 128,
};

/// The maximum size of the journal file:
/// This is pre-allocated and zeroed for performance when initialized.
/// Writes within this file never extend the filesystem inode size reducing the cost of fdatasync().
/// This enables static allocation of disk space so that appends cannot fail with ENOSPC.
/// This also enables us to detect filesystem inode corruption that would change the journal size.
// TODO remove this; just allocate a part of the total storage for the journal
pub const journal_size_max = journal_slot_count * (@sizeOf(vsr.Header) + message_size_max);

/// The maximum number of connections that can be held open by the server at any time:
pub const connections_max = replicas_max + clients_max;

/// The maximum size of a message in bytes:
/// This is also the limit of all inflight data across multiple pipelined requests per connection.
/// We may have one request of up to 2 MiB inflight or 2 pipelined requests of up to 1 MiB inflight.
/// This impacts sequential disk write throughput, the larger the buffer the better.
/// 2 MiB is 16,384 transfers, and a reasonable choice for sequential disk write throughput.
/// However, this impacts bufferbloat and head-of-line blocking latency for pipelined requests.
/// For a 1 Gbps NIC = 125 MiB/s throughput: 2 MiB / 125 * 1000ms = 16ms for the next request.
/// This impacts the amount of memory allocated at initialization by the server.
pub const message_size_max = switch (deployment_environment) {
    .simulation => message_size_max_min,
    else => 1 * 1024 * 1024,
};

/// The smallest possible message_size_max (for use in the simulator to improve performance).
/// The message body must have room for pipeline_max headers in the DVC.
const message_size_max_min = std.math.max(
    sector_size,
    std.mem.alignForward(
        @sizeOf(vsr.Header) + pipeline_max * @sizeOf(vsr.Header),
        sector_size,
    ),
);

/// The maximum number of Viewstamped Replication prepare messages that can be inflight at a time.
/// This is immutable once assigned per cluster, as replicas need to know how many operations might
/// possibly be uncommitted during a view change, and this must be constant for all replicas.
pub const pipeline_max = clients_max;

/// The minimum and maximum amount of time in milliseconds to wait before initiating a connection.
/// Exponential backoff and jitter are applied within this range.
pub const connection_delay_min_ms = 50;
pub const connection_delay_max_ms = 1000;

/// The maximum number of outgoing messages that may be queued on a replica connection.
pub const connection_send_queue_max_replica = std.math.max(std.math.min(clients_max, 4), 2);

/// The maximum number of outgoing messages that may be queued on a client connection.
/// The client has one in-flight request, and occasionally a ping.
pub const connection_send_queue_max_client = 2;

/// The maximum number of outgoing requests that may be queued on a client (including the in-flight request).
pub const client_request_queue_max = 32;

/// The maximum number of connections in the kernel's complete connection queue pending an accept():
/// If the backlog argument is greater than the value in `/proc/sys/net/core/somaxconn`, then it is
/// silently truncated to that value. Since Linux 5.4, the default in this file is 4096.
pub const tcp_backlog = 64;

/// The maximum size of a kernel socket receive buffer in bytes (or 0 to use the system default):
/// This sets SO_RCVBUF as an alternative to the auto-tuning range in /proc/sys/net/ipv4/tcp_rmem.
/// The value is limited by /proc/sys/net/core/rmem_max, unless the CAP_NET_ADMIN privilege exists.
/// The kernel doubles this value to allow space for packet bookkeeping overhead.
/// The receive buffer should ideally exceed the Bandwidth-Delay Product for maximum throughput.
/// At the same time, be careful going beyond 4 MiB as the kernel may merge many small TCP packets,
/// causing considerable latency spikes for large buffer sizes:
/// https://blog.cloudflare.com/the-story-of-one-latency-spike/
pub const tcp_rcvbuf = 4 * 1024 * 1024;

/// The maximum size of a kernel socket send buffer in bytes (or 0 to use the system default):
/// This sets SO_SNDBUF as an alternative to the auto-tuning range in /proc/sys/net/ipv4/tcp_wmem.
/// The value is limited by /proc/sys/net/core/wmem_max, unless the CAP_NET_ADMIN privilege exists.
/// The kernel doubles this value to allow space for packet bookkeeping overhead.
pub const tcp_sndbuf_replica = connection_send_queue_max_replica * message_size_max;
pub const tcp_sndbuf_client = connection_send_queue_max_client * message_size_max;

/// Whether to enable TCP keepalive:
pub const tcp_keepalive = true;

/// The time (in seconds) the connection needs to be idle before sending TCP keepalive probes:
/// Probes are not sent when the send buffer has data or the congestion window size is zero,
/// for these cases we also need tcp_user_timeout_ms below.
pub const tcp_keepidle = 5;

/// The time (in seconds) between individual keepalive probes:
pub const tcp_keepintvl = 4;

/// The maximum number of keepalive probes to send before dropping the connection:
pub const tcp_keepcnt = 3;

/// The time (in milliseconds) to timeout an idle connection or unacknowledged send:
/// This timer rides on the granularity of the keepalive or retransmission timers.
/// For example, if keepalive will only send a probe after 10s then this becomes the lower bound
/// for tcp_user_timeout_ms to fire, even if tcp_user_timeout_ms is 2s. Nevertheless, this would timeout
/// the connection at 10s rather than wait for tcp_keepcnt probes to be sent. At the same time, if
/// tcp_user_timeout_ms is larger than the max keepalive time then tcp_keepcnt will be ignored and
/// more keepalive probes will be sent until tcp_user_timeout_ms fires.
/// For a thorough overview of how these settings interact:
/// https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/
pub const tcp_user_timeout_ms = (tcp_keepidle + tcp_keepintvl * tcp_keepcnt) * 1000;

/// Whether to disable Nagle's algorithm to eliminate send buffering delays:
pub const tcp_nodelay = true;

/// Size of a CPU cache line in bytes
pub const cache_line_size = 64;

/// The minimum size of an aligned kernel page and an Advanced Format disk sector:
/// This is necessary for direct I/O without the kernel having to fix unaligned pages with a copy.
/// The new Advanced Format sector size is backwards compatible with the old 512 byte sector size.
/// This should therefore never be less than 4 KiB to be future-proof when server disks are swapped.
pub const sector_size = 4096;

/// Whether to perform direct I/O to the underlying disk device:
/// This enables several performance optimizations:
/// * A memory copy to the kernel's page cache can be eliminated for reduced CPU utilization.
/// * I/O can be issued immediately to the disk device without buffering delay for improved latency.
/// This also enables several safety features:
/// * Disk data can be scrubbed to repair latent sector errors and checksum errors proactively.
/// * Fsync failures can be recovered from correctly.
/// WARNING: Disabling direct I/O is unsafe; the page cache cannot be trusted after an fsync error,
/// even after an application panic, since the kernel will mark dirty pages as clean, even
/// when they were never written to disk.
pub const direct_io = switch (deployment_environment) {
    .development => true,
    .production => true,
    .simulation => false,
};

/// The maximum number of concurrent read I/O operations to allow at once.
pub const io_depth_read = 8;
/// The maximum number of concurrent write I/O operations to allow at once.
pub const io_depth_write = 8;

/// The number of redundant copies of the superblock in the superblock storage zone.
/// This must be either { 4, 6, 8 }, i.e. an even number, for more efficient flexible quorums.
/// This is further multiplied by two to support copy-on-write across copy sets.
///
/// The superblock contains local state for the replica and therefore cannot be replicated remotely.
/// Loss of the superblock would represent loss of the replica and so it must be protected.
/// Since each superblock copy also copies the superblock trailer (around 33 MiB), setting this
/// beyond 4 copies (or decreasing block_size < 64 KiB) can result in a superblock zone > 264 MiB.
///
/// This can mean checkpointing latencies in the rare extreme worst-case of at most 264ms, although
/// this would require EWAH compression of our block free set to have zero effective compression.
/// In practice, checkpointing latency should be an order of magnitude better due to compression,
/// because our block free set will fill holes when allocating.
///
/// The superblock only needs to be checkpointed every now and then, before the WAL wraps around,
/// or when a view change needs to take place to elect a new primary.
pub const superblock_copies = 4;

/// The maximum size of a local data file.
/// This should not be much larger than several TiB to limit:
/// * blast radius and recovery time when a whole replica is lost,
/// * replicated storage overhead, since all data files are mirrored,
/// * the size of the superblock storage zone, and
/// * the static memory allocation required for tracking LSM forest metadata in memory.
// TODO Remove, now that we have block_count_max.
pub const size_max = 16 * 1024 * 1024 * 1024 * 1024;

/// The unit of read/write access to LSM manifest and LSM table blocks in the block storage zone.
pub const block_size = 64 * 1024;

pub const block_count_max = @divExact(16 * 1024 * 1024 * 1024 * 1024, block_size);

// TODO Document and tune these LSM options:
pub const lsm_trees = 30;

/// The number of levels in an LSM tree.
/// A higher number of levels increases read amplification, as well as total storage capacity.
pub const lsm_levels = 7;

/// The number of tables at level i (0 ≤ i < lsm_levels) is `pow(lsm_growth_factor, i+1)`.
/// A higher growth factor increases write amplification (by increasing the number of tables in
/// level B that overlap a table in level A in a compaction), but decreases read amplification (by
/// reducing the height of the tree and thus the number of levels that must be probed). Since read
/// amplification can be optimized more easily (with filters and caching), we target a growth
/// factor of 8 for lower write amplification rather than the more typical growth factor of 10.
pub const lsm_growth_factor = 8;

/// The maximum key size for an LSM tree in bytes.
pub const lsm_key_size_max = 32;

pub const lsm_table_size_max = 64 * 1024 * 1024;

/// Size of nodes used by the LSM tree manifest implementation.
/// TODO Double-check this with our "LSM Manifest" spreadsheet.
pub const lsm_manifest_node_size = 16 * 1024;

/// A multiple of batch inserts that a mutable table can definitely accommodate before flushing.
/// For example, if a message_size_max batch can contain at most 8181 transfers then a multiple of 4
/// means that the transfer tree's mutable table will be sized to 8191 * 4 = 32764 transfers.
/// TODO Assert this relative to lsm_table_size_max.
/// We want to ensure that a mutable table can be converted to an immutable table without overflow.
pub const lsm_batch_multiple = 4;

pub const lsm_snapshots_max = 32;

pub const lsm_value_to_key_layout_ratio_min = 16;

/// The number of milliseconds between each replica tick, the basic unit of time in TigerBeetle.
/// Used to regulate heartbeats, retries and timeouts, all specified as multiples of a tick.
pub const tick_ms = 10;

/// The conservative round-trip time at startup when there is no network knowledge.
/// Adjusted dynamically thereafter for RTT-sensitive timeouts according to network congestion.
/// This should be set higher rather than lower to avoid flooding the network at startup.
pub const rtt_ticks = 300 / tick_ms;

/// The multiple of round-trip time for RTT-sensitive timeouts.
pub const rtt_multiple = 2;

/// The min/max bounds of exponential backoff (and jitter) to add to RTT-sensitive timeouts.
pub const backoff_min_ticks = 100 / tick_ms;
pub const backoff_max_ticks = 10000 / tick_ms;

/// The maximum skew between two clocks to allow when considering them to be in agreement.
/// The principle is that no two clocks tick exactly alike but some clocks more or less agree.
/// The maximum skew across the cluster as a whole is this value times the total number of clocks.
/// The cluster will be unavailable if the majority of clocks are all further than this value apart.
/// Decreasing this reduces the probability of reaching agreement on synchronized time.
/// Increasing this reduces the accuracy of synchronized time.
pub const clock_offset_tolerance_max_ms = 10000;

/// The amount of time before the clock's synchronized epoch is expired.
/// If the epoch is expired before it can be replaced with a new synchronized epoch, then this most
/// likely indicates either a network partition or else too many clock faults across the cluster.
/// A new synchronized epoch will be installed as soon as these conditions resolve.
pub const clock_epoch_max_ms = 60000;

/// The amount of time to wait for enough accurate samples before synchronizing the clock.
/// The more samples we can take per remote clock source, the more accurate our estimation becomes.
/// This impacts cluster startup time as the leader must first wait for synchronization to complete.
pub const clock_synchronization_window_min_ms = 2000;

/// The amount of time without agreement before the clock window is expired and a new window opened.
/// This happens where some samples have been collected but not enough to reach agreement.
/// The quality of samples degrades as they age so at some point we throw them away and start over.
/// This eliminates the impact of gradual clock drift on our clock offset (clock skew) measurements.
/// If a window expires because of this then it is likely that the clock epoch will also be expired.
pub const clock_synchronization_window_max_ms = 20000;

/// Whether to perform intensive online verification.
pub const verify = true;

// TODO Move these to a separate "internal computed constants" file.
pub const journal_size_headers = journal_slot_count * @sizeOf(vsr.Header);
pub const journal_size_prepares = journal_slot_count * message_size_max;

// TODO Move these into a separate `config_valid.zig` which we import here:
comptime {
    // vsr.parse_address assumes that config.address/config.port are valid.
    _ = std.net.Address.parseIp4(address, 0) catch unreachable;
    _ = @as(u16, port);

    // Avoid latency issues from setting sndbuf too high:
    assert(tcp_sndbuf_replica <= 16 * 1024 * 1024);
    assert(tcp_sndbuf_client <= 16 * 1024 * 1024);

    assert(journal_size_max == journal_size_headers + journal_size_prepares);

    // For the given WAL (lsm_batch_multiple=4):
    //
    //   A    B    C    D    E
    //   |····|····|····|····|
    //
    // - ("|" delineates measures, where a measure is a multiple of prepare batches.)
    // - ("·" is a prepare in the WAL.)
    // - The Replica triggers a checkpoint at "E".
    // - The entries between "A" and "D" are on-disk in level 0.
    // - The entries between "D" and "E" are in-memory in the immutable table.
    // - So the checkpoint only includes "A…D".
    //
    // The journal must have at least two measures (batches) to ensure at least one is checkpointed.
    assert(journal_slot_count >= lsm_batch_multiple * 2);
    assert(journal_slot_count % lsm_batch_multiple == 0);
    assert(journal_size_max == journal_size_headers + journal_size_prepares);

    // The WAL format requires messages to be a multiple of the sector size.
    assert(message_size_max % sector_size == 0);
    assert(message_size_max >= @sizeOf(vsr.Header));
    assert(message_size_max >= sector_size);

    assert(superblock_copies % 2 == 0);
    assert(superblock_copies >= 4);
    assert(superblock_copies <= 8);

    // ManifestLog serializes the level as a u7.
    assert(lsm_levels > 0);
    assert(lsm_levels <= std.math.maxInt(u7));

    assert(block_size % sector_size == 0);
    assert(lsm_table_size_max % sector_size == 0);
    assert(lsm_table_size_max % block_size == 0);

    // The LSM tree uses half-measures to balance compaction.
    assert(lsm_batch_multiple % 2 == 0);

    // SetAssociativeCache requires a power-of-two cardinality.
    assert(std.math.isPowerOfTwo(cache_accounts_max));
    assert(std.math.isPowerOfTwo(cache_transfers_max));
    assert(std.math.isPowerOfTwo(cache_transfers_pending_max));
}

pub const is_32_bit = @sizeOf(usize) == 4; // TODO Return a compile error if we are not 32-bit.
