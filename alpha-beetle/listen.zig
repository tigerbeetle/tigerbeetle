fn listen(address: net.Address) !os.fd_t {
    const kernel_backlog = 10;
    const domain = address.any.family;
    const socket_type = os.SOCK_STREAM | os.SOCK_CLOEXEC;
    const protocol = os.IPPROTO_TCP;
    const fd = try os.socket(domain, socket_type, protocol);
    errdefer os.close(fd);
    try os.setsockopt(fd, os.SOL_SOCKET, os.SO_REUSEADDR, &mem.toBytes(@as(c_int, 1)));
    try os.bind(fd, &address.any, address.getOsSockLen());
    try os.listen(fd, kernel_backlog);
    return fd;
}

var addr = try net.Address.parseIp4("127.0.0.1", 3000);
std.debug.warn("\naddr={} {}\n", .{ @typeName(@TypeOf(addr)), addr });
var fd = try listen(addr);
try event_loop(&ring, fd);
