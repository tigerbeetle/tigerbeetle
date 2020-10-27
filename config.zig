/// The minimum log level in increasing order of verbosity (emergency=0, debug=7):
pub const log_level = 7;

/// The server port to listen on:
pub const port = 3001;

/// Whether development, staging or production:
pub const deployment_environment = .development;

/// The maximum number of accounts to store in memory:
/// This impacts the amount of memory allocated at initialization by the server.
pub const accounts_max = switch (deployment_environment) {
    .production => 1_000_000,
    else => 100_000
};

/// The maximum number of transfers to store in memory before requiring cold data to be drained:
/// This impacts the amount of memory allocated at initialization by the server.
/// We allocate more capacity than the number of transfers for a safe hash table load factor.
pub const transfers_max = switch (deployment_environment) {
    .production => 100_000_000,
    else => 1_000_000
};

/// The maximum number of connections in the kernel's complete connection queue pending an accept():
pub const tcp_backlog = 64;

/// The maximum number of connections that can be accepted and held open by the server at any time:
pub const tcp_connections_max = 32;

/// The maximum size of a connection recv or send buffer (and a request or response) in bytes:
/// This impacts bufferbloat and head-of-line blocking latency for pipelined requests.
/// e.g. For a 1 Gbps NIC = 125 MiB/s throughput: 4 MiB / 125 * 1000ms = 32ms for the next request.
/// This also impacts the amount of memory allocated at initialization by the server.
/// e.g. tcp_connections_max * tcp_connection_buffer_max * 2
/// 4 MiB is enough for a batch of 32,768 transfers, and good for sequential disk write throughput.
pub const tcp_connection_buffer_max = 4 * 1024 * 1024;

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
pub const tcp_sndbuf = 4 * 1024 * 1024;

/// Whether to enable TCP keepalive:
pub const tcp_keepalive = true;

/// The time (in seconds) the connection needs to be idle before sending TCP keepalive probes:
/// Probes are not sent when the send buffer has data or the congestion window size is zero,
/// for these cases we also need tcp_user_timeout below.
pub const tcp_keepidle = 5;

/// The time (in seconds) between individual keepalive probes:
pub const tcp_keepintvl = 4;

/// The maximum number of keepalive probes to send before dropping the connection:
pub const tcp_keepcnt = 3;

/// The time (in milliseconds) to timeout an idle connection or unacknowledged send:
/// This timer rides on the granularity of the keepalive or retransmission timers.
/// For example, if keepalive will only send a probe after 10s then this becomes the lower bound
/// for tcp_user_timeout to fire, even if tcp_user_timeout is 2s. Nevertheless, this would timeout
/// the connection at 10s rather than wait for tcp_keepcnt probes to be sent. At the same time, if
/// tcp_user_timeout is larger than the max keepalive time then tcp_keepcnt will be ignored and
/// more keepalive probes will be sent until tcp_user_timeout fires.
/// For a thorough overview of how these settings interact:
/// https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/
pub const tcp_user_timeout = (tcp_keepidle + tcp_keepintvl * tcp_keepcnt) * 1000;

/// Whether to disable Nagle's algorithm to eliminate send buffering delays:
pub const tcp_nodelay = true;

/// The minimum size of an aligned kernel page and an Advanced Format disk sector:
/// This is necessary for direct I/O without the kernel having to fix unaligned pages with a copy.
/// The new Advanced Format sector size is backwards compatible with the old 512 byte sector size.
/// This should therefore never be less than 4 KiB to be future-proof when server disks are swapped.
pub const sector_size = 4096;
