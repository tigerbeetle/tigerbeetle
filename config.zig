/// The minimum log level in increasing order of verbosity (emergency=0, debug=7):
pub const log_level = 7;

/// The server port to listen on:
pub const port = 3001;

/// Whether development, staging or production:
pub const deployment_environment = .development;

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

/// The maximum size of a connection recv or send buffer.
/// This impacts the amount of memory allocated at initialization by the server.
/// e.g. tcp_connections_max * tcp_connection_buffer_max * 2
/// 4 MiB is enough for a batch of 32,768 transfers, and good for sequential disk write throughput.
pub const tcp_connection_buffer_max = 4 * 1024 * 1024;

/// Whether to enable TCP keepalive:
pub const tcp_keepalive = true;

/// The time (in seconds) the connection needs to be idle before sending TCP keepalive probes:
pub const tcp_keepidle = 10;

/// The time (in seconds) between individual TCP keepalive probes:
pub const tcp_keepintvl = 4;

/// The maximum number of TCP keepalive probes to send before dropping the connection:
pub const tcp_keepcnt = 3;

/// Whether to disable Nagle's algorithm to eliminate send buffering delays:
pub const tcp_nodelay = true;
