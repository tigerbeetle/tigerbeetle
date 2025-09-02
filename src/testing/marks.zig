//! This file piggy-backs on the logging infrastructure to implement explicit coverage marks:
//!     <https://ferrous-systems.com/blog/coverage-marks/>
//!
//! In production code, you can mark certain log lines as "this should be covered by a test".
//! In test code, you can then assert that a _specific_ test covers a specific log line. The two
//! benefits are:
//! - tests are more resilient to refactors
//! - production code is more readable (you can immediately jump to a specific test)
//!
//! At the surface level, this resembles usual code coverage, but the idea is closer to traceability
//! from safety-critical systems:
//!     <https://en.wikipedia.org/wiki/Requirements_traceability>
//!
//! That is, the important part is not that a log line is covered at all, but that we can trace
//! production code to a single minimal hand-written test which explains why the code needs to
//! exist.
test "tutorial" {
    // Import by a qualified name.
    const marks = @import("./marks.zig");

    const production_code = struct {
        // In production code, wrap the logger.
        const log = marks.wrap_log(std.log.scoped(.my_module));

        fn function_under_test(x: u32) void {
            if (x % 2 == 0) {
                // Both `log.info` and log.covered.info` are available.
                // Only second version records coverage.
                log.mark.info("x is even (x={})", .{x});
            }
        }
    };

    // Create a mark with the `mark` function...
    const mark = marks.check("x is even");
    production_code.function_under_test(92);
    try mark.expect_hit(); // ... and don't forget to assert at the end!
}

const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");

const GlobalStateType = if (builtin.is_test) struct {
    mark_name: ?[]const u8 = null,
    mark_hit_count: u32 = 0,
} else void;

/// Stores the currently active mark and its hit count. State is not synchronized and assumes
/// single threaded execution.
var global_state: GlobalStateType = .{};

pub const Mark = struct {
    name: []const u8,

    pub fn expect_hit(mark: Mark) !void {
        comptime assert(builtin.is_test);
        assert(global_state.mark_name.?.ptr == mark.name.ptr);
        defer global_state = .{};

        if (global_state.mark_hit_count == 0) {
            std.debug.print("mark '{s}' not hit", .{mark.name});
            return error.MarkNotHit;
        }
    }

    pub fn expect_not_hit(mark: Mark) !void {
        comptime assert(builtin.is_test);
        assert(global_state.mark_name.?.ptr == mark.name.ptr);
        defer global_state = .{};

        if (global_state.mark_hit_count != 0) {
            std.debug.print("mark '{s}' hit", .{mark.name});
            return error.MarkHit;
        }
    }
};

pub fn check(name: []const u8) Mark {
    comptime assert(builtin.is_test);
    assert(global_state.mark_name == null);
    assert(global_state.mark_hit_count == 0);

    global_state.mark_name = name;
    return Mark{ .name = name };
}

pub fn wrap_log(comptime base: type) type {
    return struct {
        pub const mark = if (builtin.is_test) struct {
            pub fn err(comptime fmt: []const u8, args: anytype) void {
                record(fmt);
                base.err(fmt, args);
            }

            pub fn warn(comptime fmt: []const u8, args: anytype) void {
                record(fmt);
                base.warn(fmt, args);
            }

            pub fn info(comptime fmt: []const u8, args: anytype) void {
                record(fmt);
                base.info(fmt, args);
            }

            pub fn debug(comptime fmt: []const u8, args: anytype) void {
                record(fmt);
                base.debug(fmt, args);
            }
        } else base;

        pub const err = base.err;
        pub const warn = base.warn;
        pub const info = base.info;
        pub const debug = base.debug;
    };
}

fn record(fmt: []const u8) void {
    comptime assert(builtin.is_test);
    if (global_state.mark_name) |mark_active| {
        if (std.mem.indexOf(u8, fmt, mark_active) != null) {
            global_state.mark_hit_count += 1;
        }
    }
}
