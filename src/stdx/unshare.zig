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

// The external pid of the init process of the unshare pid namespace.
// (Its pid within the namespace is always 1.)
var child_pid: ?std.process.Child.Id = null;

// On receiving SIGTERM, the parent must explicitly kill the pid namespace child process, since
// it would otherwise keep running. Since the child is the init process, that automatically kills
// all of its descendants too.
const trap_action = std.posix.Sigaction{
    .handler = .{ .handler = trap_handler },
    .mask = std.posix.empty_sigset,
    .flags = 0,
};

fn trap_handler(signal: i32) callconv(.c) void {
    if (child_pid) |child| {
        std.posix.kill(child, std.posix.SIG.KILL) catch |err| {
            log.err("error killing sandboxed process: {}", .{err});
        };
    }
    std.posix.exit(@intCast(@as(i32, 128) + signal));
}

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
    comptime assert(builtin.os.tag == .linux);

    if (std.os.linux.getpid() != 1) {
        try linux_unshare(.{
            .pid = options.pid,
            .network = options.network,
        });
        if (options.network) {
            try linux_ip_link_loopback();
        }
        if (options.pid) {
            std.posix.sigaction(std.posix.SIG.TERM, &trap_action, null);
            try fork_and_exit(gpa);
        }
    } else {
        // We are within the pid namespace.
        assert(options.pid);
        assert(std.os.linux.getpid() == 1);
        assert(child_pid == null);
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
    comptime assert(builtin.os.tag == .linux);

    // Create user namespace first.
    const unshare_user_result = std.os.linux.unshare(linux.CLONE.NEWUSER);
    const unshare_user_errno = std.os.linux.E.init(unshare_user_result);
    if (unshare_user_errno != .SUCCESS) {
        log.err("Failed to create user namespace: {}", .{unshare_user_errno});
        return error.UnshareFailure;
    }

    // Create PID namespace.
    if (options.pid) {
        const unshare_pid_result = std.os.linux.unshare(linux.CLONE.NEWPID);
        const unshare_pid_errno = std.os.linux.E.init(unshare_pid_result);
        if (unshare_pid_errno != .SUCCESS) {
            log.err("Failed to create pid namespace: {}", .{unshare_pid_errno});
            return error.UnshareFailure;
        }
    }

    // Create network namespace.
    if (options.network) {
        const unshare_net_result = std.os.linux.unshare(linux.CLONE.NEWNET);
        const unshare_net_errno = std.os.linux.E.init(unshare_net_result);
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
    comptime assert(builtin.os.tag == .linux);

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

    // Netlink definitions.
    const nlmsghdr = linux.nlmsghdr;
    const ifinfomsg = linux.ifinfomsg;

    const nlmsgerr = extern struct {
        @"error": c_int,
        msg: nlmsghdr,
    };

    const IFF_UP = 0x1;

    // Our message to the kernel - header plus interface info.
    const Message = extern struct {
        hdr: nlmsghdr,
        ifi: ifinfomsg,

        comptime {
            assert(@sizeOf(@This()) == @sizeOf(nlmsghdr) + @sizeOf(ifinfomsg));
        }
    };

    // Kernel's message to us - header plus error info.
    const Response = extern struct {
        hdr: nlmsghdr,
        err: nlmsgerr,

        comptime {
            assert(@sizeOf(@This()) == @sizeOf(nlmsghdr) + @sizeOf(nlmsgerr));
        }
    };

    var msg: Message = .{
        .hdr = .{
            .len = @sizeOf(nlmsghdr) + @sizeOf(ifinfomsg),
            .type = .RTM_NEWLINK,
            // ACK says to always send a response, even on success.
            .flags = linux.NLM_F_REQUEST | linux.NLM_F_ACK,
            .seq = 0,
            .pid = 0,
        },
        .ifi = .{
            .family = linux.AF.UNSPEC,
            .type = 0,
            // Seems to be the loopback device, not sure how
            // to find this value the correct way.
            .index = 1,
            .flags = IFF_UP,
            // man pages say use this value.
            .change = 0xFFFFFFFF,
        },
    };

    const msg_buf = std.mem.asBytes(&msg);
    const sent_len = std.posix.sendto(sock, msg_buf, 0, null, 0) catch |err| {
        log.err("failed to send netlink message: {}", .{err});
        return error.IpLink;
    };
    assert(sent_len == msg.hdr.len);

    var ack: Response = undefined;
    const ack_buf = std.mem.asBytes(&ack);
    const ack_len = std.posix.recv(sock, ack_buf, 0) catch |err| {
        log.err("failed to receive netlink ack: {}", .{err});
        return error.IpLink;
    };

    assert(ack_len == @sizeOf(Response));
    assert(ack.hdr.type == .ERROR);
    assert(ack.err.msg.pid == msg.hdr.pid);

    if (ack.err.@"error" != 0) {
        log.err("netlink operation failed with errno: {}", .{-ack.err.@"error"});
        return error.IpLink;
    }
}

fn fork_and_exit(gpa: std.mem.Allocator) !void {
    const args_ours = std.os.argv;

    // We get a fresh path to the exe instead of using the original
    // first argument so that the exe path will be correct even if
    // this process's cwd has changed relative to the original exe.
    var exe_path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const exe_path = try std.fs.selfExePath(&exe_path_buffer);

    const args_new = try gpa.alloc([]const u8, args_ours.len);
    defer gpa.free(args_new);

    args_new[0] = exe_path;

    for (1..args_ours.len) |arg_index| {
        args_new[arg_index] = std.mem.span(args_ours[arg_index]);
    }

    var child = std.process.Child.init(args_new, gpa);
    child.stdin_behavior = .Inherit;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;

    try child.spawn();

    // Set the global pid so that we can kill it if we receive a SIGTERM.
    assert(child_pid == null);
    child_pid = child.id;

    const result = try child.wait();
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
