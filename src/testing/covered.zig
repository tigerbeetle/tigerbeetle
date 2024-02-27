//! We'll get you covered!
//!
//! This file piggy-backs on the logging infrastructure to implement explicit coverage marks:
//!     <https://ferrous-systems.com/blog/coverage-marks/>
//!
//! In production code, you can mark certain log lines as "this should be covered by a test".
//! In test code, you can then assert that a _specific_ test covers a specific log line. The two
//! benefits are:
//! - tests are more resilient to refactors
//! - production code is more readable (you can immediately jump to a specific test)
test "tutorial" {
    // Import by a qualified name.
    const covered = @import("./covered.zig");

    const production_code = struct {
        // In production code, wrap the logger.
        const log = covered.wrap_log(std.log.scoped(.my_module));

        fn function_under_test(x: u32) void {
            if (x % 2 == 0) {
                // Both `log.info` and log.covered.info` are available.
                // Only second version records coverage.
                log.covered.info("x is even (x={})", .{x});
            }
        }
    };

    // Create a mark with the `mark` function...
    const m = covered.mark("x is even");
    production_code.function_under_test(92);
    try m.expect_hit(); // ... and don't forget to assert at the end!
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

    pub fn expect_hit(m: Mark) !void {
        comptime assert(builtin.is_test);
        assert(global_state.mark_name.?.ptr == m.name.ptr);
        defer global_state = .{};

        if (global_state.mark_hit_count == 0) {
            std.debug.print("mark '{s}' not hit", .{m.name});
            return error.MarkNotHit;
        }
    }
};

pub fn mark(name: []const u8) Mark {
    comptime assert(builtin.is_test);
    assert(global_state.mark_name == null);
    assert(global_state.mark_hit_count == 0);

    global_state.mark_name = name;
    return Mark{ .name = name };
}

pub fn wrap_log(comptime base: anytype) type {
    if (builtin.is_test) {
        return struct {
            pub const err = warn;
            pub const warn = base.warn;
            pub const info = base.info;
            pub const debug = base.debug;

            pub const covered = struct {
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
            };
        };
    } else {
        return struct {
            pub const err = warn;
            pub const warn = base.warn;
            pub const info = base.info;
            pub const debug = base.debug;

            pub const covered = base;
        };
    }
}

fn record(fmt: []const u8) void {
    comptime assert(builtin.is_test);
    if (global_state.mark_name) |mark_active| {
        if (std.mem.indexOf(u8, fmt, mark_active) != null) {
            global_state.mark_hit_count += 1;
        }
    }
}
