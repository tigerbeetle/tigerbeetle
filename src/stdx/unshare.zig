//! Some tools for working with Linux `unshare` and namespaces.
//!
//! We use user, pid, and network namespaces for two purposes:
//!
//! - Processes namespaces enable all processes in the namespace
//!   to be killed when the namespace's init process is.
//! - Network namespaces allow us to create an isolated loopback network.
//!
//! This code uses the Linux `unshare` syscall to create new
//! namespaces.
//!
//! The main tool here is `maybe_unshare_and_relaunch`, which provides
//! a pattern for forking a new process that is an init process in
//! its own process namespace.

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const log = std.log.scoped(.unshare);
const assert = std.debug.assert;

/// Relaunch this process with new namespaces.
///
/// If the current process is already running with the namespaces configured as
/// requested then this function does nothing. Otherwise it configures the
/// namespaces and with them spawns a new process with the same arguments as
/// the current process, waits for it, then exits the process directly (not
/// returning from this function).
///
/// This should generally be called immediately from `main`.
///
/// If the `pid` option is provided then the spawned process will be the init
/// process in a new pid namespace. When it is terminated all subprocesses
/// transitively will also be terminated.
///
/// If the `network` option is provided then the spawned process and its
/// subprocesses will have loopback network access only.
pub fn maybe_unshare_and_relaunch(
    gpa: std.mem.Allocator,
    options: struct {
        pid: bool,
        network: bool,
    },
) !void {
    assert(builtin.os.tag == .linux);

    const should_unshare_and_fork = blk: {
        const unshared = std.process.getEnvVarOwned(gpa, "TB_UNSHARED") catch {
            break :blk true;
        };
        gpa.free(unshared);
        break :blk false;
    };

    if (should_unshare_and_fork) {
        try linux_unshare(.{
            .pid = options.pid,
            .network = options.network,
        });
        if (options.network) {
            try linux_ip_link_loopback();
        }
        try fork_and_exit(gpa);
    }
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
pub fn linux_unshare(options: struct {
    pid: bool,
    network: bool,
}) !void {
    assert(builtin.os.tag == .linux);

    // Create user namespace first
    const unshare_user_result = std.os.linux.unshare(linux.CLONE.NEWUSER);
    const unshare_user_errno = std.posix.errno(unshare_user_result);
    if (unshare_user_errno != .SUCCESS) {
        log.err("Failed to create user namespace: {}", .{unshare_user_errno});
        return error.UnshareFailure;
    }

    // Create PID namespace
    if (options.pid) {
        const unshare_pid_result = std.os.linux.unshare(linux.CLONE.NEWPID);
        const unshare_pid_errno = std.posix.errno(unshare_pid_result);
        if (unshare_pid_errno != .SUCCESS) {
            log.err("Failed to create pid namespace: {}", .{unshare_pid_errno});
            return error.UnshareFailure;
        }
    }

    // Create network namespace
    if (options.network) {
        const unshare_net_result = std.os.linux.unshare(linux.CLONE.NEWNET);
        const unshare_net_errno = std.posix.errno(unshare_net_result);
        if (unshare_net_errno != .SUCCESS) {
            log.err("Failed to create net namespace: {}", .{unshare_net_errno});
            return error.UnshareFailure;
        }
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
pub fn linux_ip_link_loopback() !void {
    assert(builtin.os.tag == .linux);

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

    // Prepare netlink message to set up loopback interface.
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
        // Seems to be the loopback device, not sure how
        // to find this value the correct way.
        .index = 1,
        .flags = IFF_UP,
        // man pages say use this value.
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

fn fork_and_exit(gpa: std.mem.Allocator) !void {
    var args_ours = try std.process.argsWithAllocator(gpa);
    defer args_ours.deinit();

    var args_new: std.ArrayListUnmanaged([]const u8) = .{};
    defer args_new.deinit(gpa);

    // We get a fresh path to the exe instead of using the original
    // first argument so that the exe path will be correct even if
    // this process's cwd has changed relative to the original exe.
    var exe_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const exe_path = try std.fs.selfExePath(&exe_path_buffer);

    // Skip old exe argument.
    _ = args_ours.next();

    // Add new exe argument.
    try args_new.append(gpa, exe_path);

    while (args_ours.next()) |arg| {
        try args_new.append(gpa, arg);
    }

    var env_map = try std.process.getEnvMap(gpa);
    defer env_map.deinit();

    try env_map.put("TB_UNSHARED", "1");

    var child = std.process.Child.init(args_new.items, gpa);

    child.stdin_behavior = .Inherit;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;
    child.env_map = &env_map;

    const result = try child.spawnAndWait();

    switch (result) {
        .Exited => |code| {
            std.process.exit(code);
        },
        .Signal => |signal| {
            log.info("sandboxed subprocesses exited with signal {}", .{signal});
            std.process.exit(1);
        },
        else => {
            log.err("sandboxed subprocesses exited abnormally", .{});
            std.process.exit(2);
        },
    }
}
