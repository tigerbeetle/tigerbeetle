/// The minimum log level in increasing order of verbosity (emergency=0, debug=7):
pub const log_level = 7;

/// The server port to listen on:
pub const port = 3001;

/// The maximum number of connections in the kernel's complete connection queue pending an accept():
pub const tcp_backlog = 64;

/// The maximum number of connections that can be accepted and held open by the server at any time:
pub const tcp_connections_max = 32;

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
