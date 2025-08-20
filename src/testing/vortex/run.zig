const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;

const log = std.log.scoped(.run);

pub fn main(allocator: std.mem.Allocator) !void {
    if (builtin.os.tag != .linux) {
        log.err("The `run` command to set up namespaces is only supported on Linux.", .{});
        return error.NotSupported;
    }

    var args_passthrough = try std.process.argsWithAllocator(allocator);
    defer args_passthrough.deinit();

    var vortex_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const vortex_path = try std.fs.selfExePath(&vortex_path_buffer);

    var argv: std.ArrayListUnmanaged([]const u8) = .{};
    defer argv.deinit(allocator);

    // Skip first two arguments ('vortex run').
    _ = args_passthrough.next();
    _ = args_passthrough.next();

    // Add the `vortex supervisor` command and passthrough args.
    try argv.append(allocator, vortex_path);
    try argv.append(allocator, "supervisor");

    while (args_passthrough.next()) |arg| {
        try argv.append(allocator, arg);
    }

    // Set up new net, pid, and user namespaces.
    try linux_unshare();
    // Set up loopback networking.
    try linux_ip_link_loopback();

    // Run the supervisor
    var child = std.process.Child.init(argv.items, allocator);
    child.stdin_behavior = .Inherit;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;

    try child.spawn();
    const result = try child.wait();
    std.process.exit(@intFromEnum(result));
}

/// Implementation of `unshare` somewhat like
///
/// ```
/// unshare --user --net --pid
/// ```
///
/// We're trying to accomplish two main things:
///
/// - creating a new pid namespace so that all subprocesses
///   are automatically terminated when pid 1 (the forked
///   vortex supervisor) is terminated.
/// - creating a network sandbox
///
/// Note that on recent Ubuntu's this only works if AppArmour
/// rules have been relaxed:
///
/// ```
/// sudo sysctl -w kernel.apparmor_restrict_unprivileged_unconfined=0
/// sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0
/// ```
fn linux_unshare() !void {
    // Create user namespace first
    const unshare_user_result = std.os.linux.unshare(linux.CLONE.NEWUSER);
    const unshare_user_errno = std.posix.errno(unshare_user_result);
    if (unshare_user_errno != .SUCCESS) {
        log.err("Failed to create user namespace: {}", .{unshare_user_errno});
        return error.UnshareFailure;
    }

    // Create PID namespace
    const unshare_pid_result = std.os.linux.unshare(linux.CLONE.NEWPID);
    const unshare_pid_errno = std.posix.errno(unshare_pid_result);
    if (unshare_pid_errno != .SUCCESS) {
        log.err("Failed to create pid namespace: {}", .{unshare_pid_errno});
        return error.UnshareFailure;
    }

    // Create network namespace
    const unshare_net_result = std.os.linux.unshare(linux.CLONE.NEWNET);
    const unshare_net_errno = std.posix.errno(unshare_net_result);
    if (unshare_net_errno != .SUCCESS) {
        log.err("Failed to create net namespace: {}", .{unshare_net_errno});
        return error.UnshareFailure;
    }
}

/// Implementation of `ip link` equivalent to
///
/// ```
/// ip link set up dev lo
/// ```
///
/// This brings up the loopback device so that networking
/// over 127.0.0.1 works.
fn linux_ip_link_loopback() !void {
    // Open a netlink socket with the NETLINK.ROUTE protocol.
    const sock = std.posix.socket(
        linux.AF.NETLINK,
        std.posix.SOCK.RAW,
        linux.NETLINK.ROUTE,
    ) catch |err| {
        log.err("failed to create netlink socket: {}", .{err});
        return error.IpLink;
    };
    defer std.posix.close(sock);

    const addr = linux.sockaddr.nl{
        .family = linux.AF.NETLINK,
        .pid = 0,
        .groups = 0,
    };
    std.posix.bind(sock, @ptrCast(&addr), @sizeOf(@TypeOf(addr))) catch |err| {
        log.err("failed to bind netlink socket: {}", .{err});
        return error.IpLink;
    };

    // Prepare netlink message to set loopback interface up
    const nlmsghdr = linux.nlmsghdr;
    const ifinfomsg = linux.ifinfomsg;

    const IFF_UP = 0x1;

    var msg_buf: [1024]u8 align(4) = std.mem.zeroes([1024]u8);
    const hdr = @as(*nlmsghdr, @ptrCast(@alignCast(&msg_buf[0])));
    const ifi = @as(*ifinfomsg, @ptrCast(@alignCast(&msg_buf[@sizeOf(nlmsghdr)])));

    hdr.* = .{
        .len = @sizeOf(nlmsghdr) + @sizeOf(ifinfomsg),
        .type = .RTM_NEWLINK,
        .flags = linux.NLM_F_REQUEST | linux.NLM_F_ACK,
        .seq = 0,
        .pid = 0,
    };

    ifi.* = .{
        .family = linux.AF.UNSPEC,
        .type = 0,
        // seems to be the loopback device, not sure how
        // to find this value the correct way.
        .index = 1,
        .flags = IFF_UP,
        // man pages say use this value
        .change = 0xFFFFFFFF,
    };

    _ = std.posix.send(sock, msg_buf[0..hdr.len], 0) catch |err| {
        log.err("failed to send netlink message: {}", .{err});
        return error.IpLink;
    };

    // Receive acknowledgment
    var ack_buf: [1024]u8 = undefined;
    const ack_len = std.posix.recv(sock, &ack_buf, 0) catch |err| {
        log.err("failed to receive netlink ack: {}", .{err});
        return error.IpLink;
    };

    if (ack_len < @sizeOf(nlmsghdr)) {
        log.err("netlink ack too short", .{});
        return error.IpLink;
    }

    const ack_hdr = @as(*const nlmsghdr, @ptrCast(@alignCast(&ack_buf[0])));
    if (ack_hdr.type == .ERROR) {
        if (ack_len >= @sizeOf(nlmsghdr) + @sizeOf(i32)) {
            const errno = @as(*const i32, @ptrCast(@alignCast(&ack_buf[@sizeOf(nlmsghdr)]))).*;
            if (errno != 0) {
                log.err("netlink operation failed with errno: {}", .{-errno});
                return error.IpLink;
            }
        }
    }
}
