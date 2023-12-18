const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const net = std.net;
const os = std.os;
const assert = std.debug.assert;
const log = std.log;

const simulator = @import("simulator.zig");
const vsr = @import("vsr.zig");
const stdx = @import("stdx.zig");

// TODO use DNS instead since hard-coding an IP address isn't ideal.
const default_send_address = net.Address.initIp4([4]u8{ 65, 21, 207, 251 }, 5555);

const usage = fmt.comptimePrint(
    \\Usage:
    \\
    \\  vopr [-h | --help]
    \\
    \\  vopr [--send] [--simulations=<count>]
    \\
    \\  vopr [--send] --seed=<int> [--build-mode=<mode>]
    \\
    \\Options:
    \\
    \\  -h, --help
    \\        Print this help message and exit.
    \\
    \\  --seed=<integer>
    \\        Set the seed to a provided 64-bit unsigned integer.
    \\        By default the VOPR will run a specified seed in debug mode.
    \\        If this option is omitted, a series of random seeds will be generated.
    \\
    \\  --send[=<address>]
    \\        When set, the send option opts in to send any bugs found by the VOPR to the VOPR Hub.
    \\        The VOPR Hub will then automatically create a GitHub issue if it can verify the bug.
    \\        The VOPR Hub's address is already present as the default address.
    \\        You can optionally supply an IPv4 address for the VOPR Hub if needed.
    \\        If this option is omitted, any bugs that are found will replay locally in Debug mode.
    \\
    \\  --build-mode=<mode>
    \\        Set the build mode for the VOPR. Accepts either ReleaseSafe or Debug.
    \\        By default when no seed is provided the VOPR will run in ReleaseSafe mode.
    \\        By default when a seed is provided the VOPR will run in Debug mode.
    \\        Debug mode is only a valid build mode if a seed is also provided.
    \\
    \\  --simulations=<integer>
    \\        Set the number of times for the simulator to run when using randomly generated seeds.
    \\        By default 1000 random seeds will be generated.
    \\        This flag can only be used with ReleaseSafe mode and when no seed has been specified.
    \\
    \\Example:
    \\
    \\  vopr --seed=123 --send=127.0.0.1:5555 --build-mode=ReleaseSafe
    \\  vopr --simulations=10 --send --build-mode=Debug
    \\
, .{});

// The Report struct contains all the information to be sent to the VOPR Hub.
const Report = struct {
    checksum: [16]u8,
    bug: u8,
    seed: [8]u8,
    commit: [20]u8,
};

const Flags = struct {
    seed: ?u64,
    send_address: ?net.Address, // A null value indicates that the "send" flag is not set.
    build_mode: std.builtin.Mode,
    simulations: u32,
};

const Bug = enum(u8) {
    crash = 127, // Any assertion crash will be given an exit code of 127 by default.
    liveness = 128,
    correctness = 129,
};

pub fn main() void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const args = parse_args(allocator) catch |err| {
        fatal("unable to parse the VOPR's arguments: {}", .{err});
    };

    if (args.send_address != null) {
        check_git_status(allocator);
    }

    // If a seed is provided as an argument then replay the seed, otherwise test a 1,000 seeds:
    if (args.seed) |seed| {
        // Build in fast ReleaseSafe mode if required, useful where you don't need debug logging:
        if (args.build_mode != .Debug) {
            // Build the simulator binary
            build_simulator(allocator, .ReleaseSafe);
            log.debug("Replaying seed {} in ReleaseSafe mode...\n", .{seed});
            _ = run_simulator(allocator, seed, .ReleaseSafe, args.send_address);
        } else {
            // Build the simulator binary
            build_simulator(allocator, .Debug);
            log.debug(
                "Replaying seed {} in Debug mode with full debug logging enabled...\n",
                .{seed},
            );
            _ = run_simulator(allocator, seed, .Debug, args.send_address);
        }
    } else if (args.build_mode == .Debug) {
        fatal("no seed provided: the VOPR must be run with --mode=ReleaseSafe", .{});
    } else {
        // Build the simulator binary
        build_simulator(allocator, .ReleaseSafe);
        // Run the simulator with randomly generated seeds.
        var i: u32 = 0;
        while (i < args.simulations) : (i += 1) {
            const seed_random = std.crypto.random.int(u64);
            const exit_code = run_simulator(
                allocator,
                seed_random,
                .ReleaseSafe,
                args.send_address,
            );
            if (exit_code != null) {
                // If a seed fails exit the loop.
                break;
            }
        }
    }
}

// Builds the simulator binary.
fn build_simulator(
    allocator: mem.Allocator,
    mode: std.builtin.Mode,
) void {
    const mode_str = switch (mode) {
        .Debug => "-Drelease=false",
        .ReleaseSafe => "-Drelease",
        else => unreachable,
    };

    const exec_result = std.ChildProcess.exec(.{
        .allocator = allocator,
        .argv = &.{ "zig/zig", "build", "simulator", mode_str },
    }) catch |err| {
        fatal("unable to build the simulator binary. Error: {}", .{err});
    };
    defer allocator.free(exec_result.stdout);
    defer allocator.free(exec_result.stderr);

    switch (exec_result.term) {
        .Exited => |code| {
            if (code != 0) {
                fatal("unable to build the simulator binary. Term: {}\n", .{exec_result.term});
            }
        },
        else => {
            fatal("unable to build the simulator binary. Term: {}\n", .{exec_result.term});
        },
    }
}

// Runs the simulator as a child process.
// Reruns the simulator in Debug mode if a seed fails in ReleaseSafe mode.
fn run_simulator(
    allocator: mem.Allocator,
    seed: u64,
    mode: std.builtin.Mode,
    send_address: ?net.Address,
) ?Bug {
    var seed_str = std.ArrayList(u8).init(allocator);
    defer seed_str.deinit();

    fmt.formatInt(seed, 10, .lower, .{}, seed_str.writer()) catch |err| switch (err) {
        error.OutOfMemory => fatal("unable to format seed as an int. Error: {}", .{err}),
    };

    // The child process executes zig run instead of zig build. Otherwise the build process is
    // interposed between the VOPR and the simulator and its exit code is returned instead of the
    // simulator's exit code.
    const exit_code = run_child_process(
        allocator,
        &.{ "./zig-out/bin/simulator", seed_str.items },
    );

    const result = switch (exit_code) {
        0 => null,
        127 => Bug.crash,
        128 => Bug.liveness,
        129 => Bug.correctness,
        else => {
            log.debug("unexpected simulator exit code: {}\n", .{exit_code});
            @panic("unexpected simulator exit code.");
        },
    };

    if (result) |bug| {
        if (send_address) |hub_address| {
            var report: Report = create_report(allocator, bug, seed);
            send_report(report, hub_address);
        }

        if (mode == .ReleaseSafe) {
            log.debug("simulator exited with exit code {}.\n", .{@intFromEnum(bug)});
            log.debug("rerunning seed {} in Debug mode.\n", .{seed});
            // Build the simulator binary in Debug mode instead.
            build_simulator(allocator, .Debug);
            assert(bug == run_simulator(allocator, seed, .Debug, null).?);
        }
    }
    return result;
}

// Initializes and executes the simulator as a child process.
// Terminates the VOPR if the simulator fails to run or exits without an exit code.
fn run_child_process(allocator: mem.Allocator, argv: []const []const u8) u8 {
    var child_process = std.ChildProcess.init(argv, allocator);
    child_process.stdout = std.io.getStdOut();
    child_process.stderr = std.io.getStdErr();

    // Using spawn instead of exec because spawn allows output to be streamed instead of buffered.
    const term = child_process.spawnAndWait() catch |err| {
        fatal("unable to run the simulator as a child process. Error: {}", .{err});
    };

    switch (term) {
        .Exited => |code| {
            log.debug("exit with code: {}\n", .{code});
            return code;
        },
        .Signal => |code| {
            switch (code) {
                6 => {
                    log.debug("exit with signal: {}. Indicates a crash bug.\n", .{code});
                    return @intFromEnum(Bug.crash);
                },
                else => {
                    fatal("the simulator exited with an unexpected signal. Term: {}\n", .{term});
                },
            }
        },
        else => {
            fatal("the simulator exited without an exit code. Term: {}\n", .{term});
        },
    }
}

fn check_git_status(allocator: mem.Allocator) void {
    // Running git status to determine whether there is any uncommitted code or local changes.
    var args = [2][]const u8{ "git", "status" };
    var exec_result = std.ChildProcess.exec(.{
        .allocator = allocator,
        .argv = &args,
    }) catch |err| {
        fatal("unable to determine TigerBeetle's git status. Error: {}", .{err});
    };
    defer allocator.free(exec_result.stdout);
    defer allocator.free(exec_result.stderr);

    var git_status = exec_result.stdout;
    var code_committed = mem.containsAtLeast(
        u8,
        git_status,
        1,
        "nothing to commit, working tree clean",
    );
    var code_up_to_date = (mem.containsAtLeast(
        u8,
        git_status,
        1,
        "Your branch is up to date",
    ) or
        mem.containsAtLeast(
        u8,
        git_status,
        1,
        "HEAD detached at",
    ));
    if (code_committed and code_up_to_date) {
        log.debug("All code has been committed and pushed.\n", .{});
    } else {
        fatal(
            "the VOPR cannot run with --send when your branch is ahead or there's uncommited code",
            .{},
        );
    }
}

// Sends a bug report to the VOPR Hub.
// The VOPR Hub will attempt to verify the bug and automatically create a GitHub issue.
fn send_report(report: Report, address: net.Address) void {
    // Bug type
    assert(report.bug == 1 or report.bug == 2 or report.bug == 3);

    const vopr_message_size = 45;
    var message: [vopr_message_size]u8 = undefined;
    message[0..16].* = report.checksum;
    message[16] = report.bug;
    message[17..25].* = report.seed;
    message[25..vopr_message_size].* = report.commit;

    const stream = net.tcpConnectToAddress(address) catch |err| {
        fatal("unable to create a connection to the VOPR Hub. Error: {}", .{err});
    };

    log.debug("Connected to VOPR Hub.\n", .{});

    var writer = stream.writer();
    writer.writeAll(&message) catch |err| {
        fatal("unable to send the report to the VOPR Hub. Error: {}", .{err});
    };

    // Receive reply
    var reply: [1]u8 = undefined;
    var reader = stream.reader();
    var bytes_read = reader.readAll(&reply) catch |err| {
        fatal("unable to read a reply from the VOPR Hub. Error: {}", .{err});
    };
    if (bytes_read > 0) {
        log.debug("Confirmation received from VOPR Hub: {s}.\n", .{reply});
    } else {
        log.debug("No reply received from VOPR Hub.\n", .{});
    }
}

// Creating a single report struct that contains all information required for the VOPR Hub.
fn create_report(allocator: mem.Allocator, bug: Bug, seed: u64) Report {
    log.debug("Collecting VOPR bug and seed, and the current git commit hash.\n", .{});

    // Setting the bug type.
    var bug_type: u8 = undefined;
    switch (bug) {
        .correctness => bug_type = 1,
        .liveness => bug_type = 2,
        .crash => bug_type = 3,
    }

    // Running git log to extract the current TigerBeetle git commit hash from stdout.
    var args = [3][]const u8{ "git", "log", "-1" };
    var exec_result = std.ChildProcess.exec(.{
        .allocator = allocator,
        .argv = &args,
    }) catch |err| {
        fatal("unable to extract TigerBeetle's git commit hash. Error: {}", .{err});
    };
    defer allocator.free(exec_result.stdout);
    defer allocator.free(exec_result.stderr);

    var git_log = exec_result.stdout;
    log.debug("git commit that was retrieved: {s}\n", .{git_log[7..47].*});

    var commit_string = git_log[7..47].*;
    var commit_byte_array: [20]u8 = undefined;
    _ = fmt.hexToBytes(&commit_byte_array, &commit_string) catch |err| {
        fatal("unable to cast the git commit hash to hex. Error: {}", .{err});
    };

    // Zig stores value as Little Endian when VOPR Hub is expecting Big Endian.
    assert(@import("builtin").target.cpu.arch.endian() == .Little);

    var message = Report{
        .checksum = undefined,
        .bug = bug_type,
        .seed = @as([8]u8, @bitCast(@byteSwap(seed))),
        .commit = commit_byte_array,
    };

    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(std.mem.asBytes(&message)[@sizeOf(u128)..45], hash[0..], .{});
    stdx.copy_disjoint(.exact, u8, message.checksum[0..], hash[0..message.checksum.len]);

    return message;
}

/// Format and print an error message followed by the usage string to stderr,
/// then exit with an exit code of 1.
fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
    os.exit(1);
}

/// Parse e.g. `--seed=123` into 123 with error handling.
fn parse_flag(comptime flag: []const u8, arg: [:0]const u8) [:0]const u8 {
    const value = arg[flag.len..];
    if (value.len < 2) {
        fatal("{s} argument requires a value", .{flag});
    }
    if (value[0] != '=') {
        fatal("expected '=' after {s} but found '{c}'", .{ flag, value[0] });
    }
    return value[1..];
}

// Parses the VOPR arguments to set flag values, otherwise uses default flag values.
fn parse_args(allocator: mem.Allocator) !Flags {
    // Set default values
    var seed: ?u64 = null;
    var send_address: ?net.Address = null;
    var build_mode: ?std.builtin.Mode = null;
    var simulations: u32 = 1000;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip argv[0] which is the name of this executable
    assert(args.skip());

    while (args.next()) |arg| {
        if (mem.startsWith(u8, arg, "--seed")) {
            const seed_string = parse_flag("--seed", arg);
            seed = simulator.parse_seed(seed_string);
            // If a seed is supplied Debug becomes the default mode.
            if (build_mode == null) {
                build_mode = .Debug;
            }
        } else if (mem.startsWith(u8, arg, "--send")) {
            if (mem.eql(u8, arg, "--send")) {
                // If --send is set and no address is supplied then use default address.
                send_address = default_send_address;
            } else {
                const str_address = parse_flag("--send", arg);
                send_address = try vsr.parse_address_and_port(str_address);
            }
        } else if (mem.startsWith(u8, arg, "--build-mode")) {
            if (mem.eql(u8, parse_flag("--build-mode", arg), "ReleaseSafe")) {
                build_mode = .ReleaseSafe;
            } else if (mem.eql(u8, parse_flag("--build-mode", arg), "Debug")) {
                build_mode = .Debug;
            } else {
                fatal(
                    "unsupported build mode: {s}. Use either ReleaseSafe or Debug mode.",
                    .{arg},
                );
            }
        } else if (mem.startsWith(u8, arg, "--simulations")) {
            const num_simulations_string = parse_flag("--simulations", arg);
            simulations = std.fmt.parseUnsigned(
                u32,
                num_simulations_string,
                10,
            ) catch |err| switch (err) {
                error.Overflow => @panic(
                    "the number of simulations exceeds a 16-bit unsigned integer",
                ),
                error.InvalidCharacter => @panic(
                    "the number of simulations contains an invalid character",
                ),
            };
        } else if (mem.eql(u8, arg, "-h") or mem.eql(u8, arg, "--help")) {
            std.io.getStdOut().writeAll(usage) catch os.exit(1);
            os.exit(0);
        } else if (mem.startsWith(u8, arg, "--")) {
            fatal("unexpected argument: '{s}'", .{arg});
        } else {
            fatal("unexpected argument: '{s}' (must start with '--')", .{arg});
        }
    }

    // Build mode is set last to ensure that if a seed is passed to the VOPR the Debug default
    // doesn't override a user specified mode.
    if (build_mode == null) {
        build_mode = .ReleaseSafe;
    }

    if (seed == null and build_mode.? != .ReleaseSafe) {
        fatal("random seeds must be run in ReleaseSafe mode", .{});
    }

    return Flags{
        .seed = seed,
        .send_address = send_address,
        .build_mode = build_mode.?,
        .simulations = simulations,
    };
}
