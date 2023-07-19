//! Analyze LLVM IR to find large copies.
//!
//! To get a file with IR, use `-femit-llvm-ir` cli argument for `zig build-exe` or
//! `-Demit-llvm-ir` for `zig build`.
//!
//! Pass the resulting .ll file to copyhound:
//!
//!     zig run -Drelease-safe src/copyhound.zig -- --memcpy-bytes 128 < tigerbeetle.ll \
//!        | sort -n -k 1
//!
//! This only detects memory copies with comptime-know size (eg, when you copy a `T`, rather than a
//! `[]T`).

const std = @import("std");
const stdx = @import("./stdx.zig");
const assert = std.debug.assert;

const log = std.log;
pub const log_level: std.log.Level = .info;

const size_thershold = 1024;

const CliArgs = struct {
    memcpy_bytes: u32,
    code_size: bool,

    fn parse(arena: std.mem.Allocator) !CliArgs {
        var args = try std.process.argsWithAllocator(arena);
        assert(args.skip());

        var memcpy_bytes: ?u32 = null;
        var code_size: bool = false;
        while (args.next(arena)) |arg_or_err| {
            const arg = try arg_or_err;

            if (std.mem.eql(u8, arg, "--memcpy-bytes")) {
                const arg_value_or_err = args.next(arena) orelse
                    fatal("expected a value for --memcpy-bytes", .{});
                const arg_value = try arg_value_or_err;
                memcpy_bytes = std.fmt.parseInt(u32, arg_value, 10) catch
                    fatal("expected an integer value for --memcpy-bytes, got '{s}'", .{arg_value});
            } else if (std.mem.eql(u8, arg, "--code-size")) {
                code_size = true;
            } else {
                fatal("unexpected argument '{s}'", .{arg});
            }
        }
        if ((memcpy_bytes == null and !code_size) or (memcpy_bytes != null and code_size)) {
            fatal("expected one of --memcpy-bytes or --code-size", .{});
        }
        return CliArgs{
            .memcpy_bytes = memcpy_bytes orelse std.math.maxInt(u32),
            .code_size = code_size,
        };
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena.deinit();

    const allocator = arena.allocator();

    const cli_args = try CliArgs.parse(allocator);
    var line_buffer = try allocator.alloc(u8, 1024 * 1024);
    var func_buf = try allocator.alloc(u8, 4096);

    const stdin = std.io.getStdIn();
    var buf_reader = std.io.bufferedReader(stdin.reader());
    var in_stream = buf_reader.reader();

    const stdout = std.io.getStdOut();
    var buf_writer = std.io.bufferedWriter(stdout.writer());
    defer buf_writer.flush() catch {};

    var out_stream = buf_writer.writer();

    var current_function: ?[]const u8 = null;
    var current_function_size: u32 = 0;
    while (try in_stream.readUntilDelimiterOrEof(line_buffer, '\n')) |line| {
        if (std.mem.startsWith(u8, line, "define ")) {
            current_function = extract_function_name(line, func_buf) orelse {
                log.err("can't parse define line={s}", .{line});
                return error.BadDefine;
            };
            continue;
        }

        if (current_function) |func| {
            if (std.mem.eql(u8, line, "}")) {
                if (cli_args.code_size) {
                    try out_stream.print("size {} {s} \n", .{ current_function_size, func });
                }
                current_function = null;
                current_function_size = 0;
                continue;
            }
            current_function_size += 1;
            if (stdx.cut(line, "@llvm.memcpy")) |cut| {
                const size = extract_memcpy_size(cut.suffix) orelse {
                    log.err("can't parse memcpy call line={s}", .{line});
                    return error.BadMemcpy;
                };
                if (size > cli_args.memcpy_bytes) {
                    try out_stream.print("memcpy {:<8} {s}\n", .{ size, func });
                }
            }
        }
    }
}

/// Demangles function name by removing all comptime arguments (which are always inside `()`).
fn extract_function_name(define: []const u8, buf: []u8) ?[]const u8 {
    if (!std.mem.endsWith(u8, define, "{")) return null;

    const mangled_name = (stdx.cut(define, "@") orelse return null).suffix;
    var buf_count: usize = 0;
    var level: u32 = 0;
    for (mangled_name) |c| {
        switch (c) {
            '(' => level += 1,
            ')' => level -= 1,
            '"' => {},
            else => {
                if (level > 0) continue;
                if (c == ' ') return buf[0..buf_count];
                if (buf_count == buf.len) return null;
                buf[buf_count] = c;
                buf_count += 1;
            },
        }
    } else return null;
}

test "extract_function_name" {
    var buf: [1024]u8 = undefined;
    const func_name = extract_function_name(
        \\define internal fastcc i64 @".vsr.vsr.clock.ClockType(.vsr.time.Time).monotonic"
        ++
        \\(%.vsr.time.Time* %.0.1.val) unnamed_addr #1 !dbg !71485 {
    , &buf).?;
    try std.testing.expectEqualStrings(".vsr.vsr.clock.ClockType.monotonic", func_name);
}

/// Parses out the size argument of an memcpy call.
fn extract_memcpy_size(memcpy_call: []const u8) ?u32 {
    const call_args = (stdx.cut(memcpy_call, "(") orelse return null).suffix;
    var level: u32 = 0;
    var arg_count: u32 = 0;

    const args_after_size = for (call_args) |c, i| {
        switch (c) {
            '(' => level += 1,
            ')' => level -= 1,
            ',' => {
                if (level > 0) continue;
                arg_count += 1;
                if (!std.mem.startsWith(u8, call_args[i..], ", ")) return null;
                if (arg_count == 2) break call_args[i + 2 ..];
            },
            else => {},
        }
    } else return null;

    const size_arg = (stdx.cut(args_after_size, ",") orelse return null).prefix;

    const size_value = (stdx.cut(size_arg, " ") orelse return null).suffix;

    // Runtime-known memcpy size, assume that's OK.
    if (std.mem.startsWith(u8, size_value, "%")) return 0;

    return std.fmt.parseInt(u32, size_value, 10) catch null;
}

test "extract_memcpy_size" {
    const T = struct {
        fn check(
            line: []const u8,
            want: ?u32,
        ) !void {
            const got = extract_memcpy_size(line);
            try std.testing.expectEqual(want, got);
        }
    };

    // One argument is a nested expression with a function call.
    try T.check(
        "  call void @llvm.memcpy.p0i8.p0i8.i64(i8* align 8 %0, i8* align 8 bitcast(" ++
            "{ void (i32, %std.os.linux.siginfo_t*, i8*)*," ++
            " [32 x i32], <{ i32, [4 x i8] }>, void ()* }*" ++
            " @8 to i8*), i64 152, i1 false)",
        152,
    );

    // The argument is `%6` --- a runtime value.
    try T.check(
        \\   call void @llvm.memcpy.p0i8.p0i8.i64(i8* align 1 %8, i8* align 1 %4, i64 %6, i1 false)
    , 0);
}

/// Format and print an error message followed by the usage string to stderr,
/// then exit with an exit code of 1.
pub fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
    std.os.exit(1);
}
