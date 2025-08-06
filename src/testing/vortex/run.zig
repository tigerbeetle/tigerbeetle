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

    // Set up new net, pid, and user namespaces
    try linux_unshare();
    // Set up loopback networking
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

/// Implementation of `unshare` equivalent to
///
/// ```
///     unshare --net --pid --map-root-user --fork
/// ```
///
/// The fork is left to the caller.
fn linux_unshare() !void {
    // Get current uid/gid before creating namespaces so we can map them to root later
    const uid = linux.getuid();
    const gid = linux.getgid();

    // Create user namespace first (required for --map-root-user)
    const unshare_result = std.os.linux.unshare(linux.CLONE.NEWUSER);
    if (unshare_result != 0) {
        log.err("Failed to create user namespace: {}", .{unshare_result});
        return error.UnshareFailure;
    }

    // Disable setgroups (required for unprivileged uid/gid mapping)
    try write_to_file("/proc/self/setgroups", "deny");

    // Map current user to root (uid 0) in the new namespace.
    // This will allow the child supervisor's `--configure-namespace`
    // flag to set up networking with `ip link`.
    var uid_map_buf: [64]u8 = undefined;
    const uid_map = try std.fmt.bufPrint(&uid_map_buf, "0 {d} 1", .{uid});
    try write_to_file("/proc/self/uid_map", uid_map);

    var gid_map_buf: [64]u8 = undefined;
    const gid_map = try std.fmt.bufPrint(&gid_map_buf, "0 {d} 1", .{gid});
    try write_to_file("/proc/self/gid_map", gid_map);

    // Create network and PID namespaces
    const unshare_net_pid_result = std.os.linux.unshare(linux.CLONE.NEWNET | linux.CLONE.NEWPID);
    if (unshare_net_pid_result != 0) {
        log.err("Failed to create net/pid namespaces: {}", .{unshare_net_pid_result});
        return error.UnshareFailure;
    }
}

fn write_to_file(path: []const u8, content: []const u8) !void {
    const file = std.fs.openFileAbsolute(path, .{ .mode = .write_only }) catch |err| {
        log.err("Failed to open {s}: {}", .{ path, err });
        return err;
    };
    defer file.close();

    _ = file.writeAll(content) catch |err| {
        log.err("Failed to write to {s}: {}", .{ path, err });
        return err;
    };
}

/// Implementation of `ip link` equivalent to
///
/// ```
///     ip link set up dev lo
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
