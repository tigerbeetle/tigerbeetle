//! The purpose of `flags` is to define standard behavior for parsing CLI arguments and provide
//! a specific parsing library, implementing this behavior.
//!
//! These are TigerBeetle CLI guidelines:
//!
//!    - The main principle is robustness --- make operator errors harder to make.
//!    - For production usage, avoid defaults.
//!    - Thoroughly validate options.
//!    - In particular, check that no options are repeated.
//!    - Use only long options (`--addresses`).
//!    - Exception: `-h/--help` is allowed.
//!    - Use `--key=value` syntax for an option with an argument.
//!      Don't use `--key value`, as that can be ambiguous (e.g., `--key --verbose`).
//!    - Use subcommand syntax when appropriate.
//!    - Use positional arguments when appropriate.
//!
//! Design choices for this particular `flags` library:
//!
//! - Be a 80% solution. Parsing arguments is a surprisingly vast topic: auto-generated help,
//!   bash completions, typo correction. Rather than providing a definitive solution, `flags`
//!   is just one possible option. It is ok to re-implement arg parsing in a different way, as long
//!   as the CLI guidelines are observed.
//!
//! - No auto-generated help. Zig doesn't expose doc comments through `@typeInfo`, so its hard to
//!   implement auto-help nicely. Additionally, fully hand-crafted `--help` message can be of
//!   higher quality.
//!
//! - Fatal errors. It might be "cleaner" to use `try` to propagate the error to the caller, but
//!   during early CLI parsing, it is much simpler to terminate the process directly and save the
//!   caller the hassle of propagating errors. The `fatal` function is public, to allow the caller
//!   to run additional validation or parsing using the same error reporting mechanism.
//!
//! - Concise DSL. Most cli parsing is done for ad-hoc tools like benchmarking, where the ability to
//!   quickly add a new argument is valuable. As this is a 80% solution, production code may use
//!   more verbose approach if it gives better UX.
//!
//! - Caller manages ArgsIterator. ArgsIterator owns the backing memory of the args, so we let the
//!   caller to manage the lifetime. The caller should be skipping program name.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

/// Format and print an error message to stderr, then exit with an exit code of 1.
pub fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
    std.os.exit(1);
}

/// Parse CLI arguments for subcommands specified as Zig `struct` or `union(enum)`:
///
/// ```
/// const CliArgs = union(enum) {
///    start: struct { addresses: []const u8, replica: u32 },
///    format: struct {
///        verbose: bool = false,
///        positional: struct {
///            path: []const u8,
///        }
///    },
///
///    pub const help =
///        \\ tigerbeetle start --addresses=<addresses> --replica=<replica>
///        \\ tigerbeetle format [--verbose] <path>
/// }
///
/// const cli_args = parse_commands(&args, CliArgs);
/// ```
///
/// `positional` field is treated specially, it designates positional arguments.
///
/// If `pub const help` declaration is present, it is used to implement `-h/--help` argument.
pub fn parse(args: *std.process.ArgIterator, comptime CliArgs: type) CliArgs {
    return switch (@typeInfo(CliArgs)) {
        .Union => parse_commands(args, CliArgs),
        .Struct => parse_flags(args, CliArgs),
        else => unreachable,
    };
}

fn parse_commands(args: *std.process.ArgIterator, comptime Commands: type) Commands {
    comptime assert(@typeInfo(Commands) == .Union);
    comptime assert(std.meta.fields(Commands).len >= 2);

    const first_arg = args.next() orelse fatal(
        "subcommand required, expected {s}",
        .{comptime fields_to_comma_list(Commands)},
    );

    // NB: help must be declared as *pub* const to be visible here.
    if (@hasDecl(Commands, "help")) {
        if (std.mem.eql(u8, first_arg, "-h") or std.mem.eql(u8, first_arg, "--help")) {
            std.io.getStdOut().writeAll(Commands.help) catch std.os.exit(1);
            std.os.exit(0);
        }
    }

    inline for (comptime std.meta.fields(Commands)) |field| {
        comptime assert(std.mem.indexOf(u8, field.name, "_") == null);
        if (std.mem.eql(u8, first_arg, field.name)) {
            return @unionInit(Commands, field.name, parse_flags(args, field.type));
        }
    }
    fatal("unknown subcommand: '{s}'", .{first_arg});
}

fn parse_flags(args: *std.process.ArgIterator, comptime Flags: type) Flags {
    if (Flags == void) {
        if (args.next()) |arg| {
            fatal("unexpected argument: '{s}'", .{arg});
        }
        return {};
    }

    assert(@typeInfo(Flags) == .Struct);

    comptime var fields: [std.meta.fields(Flags).len]std.builtin.Type.StructField = undefined;
    comptime var field_count = 0;

    comptime var positional_fields: []const std.builtin.Type.StructField = &.{};

    comptime for (std.meta.fields(Flags)) |field| {
        if (std.mem.eql(u8, field.name, "positional")) {
            assert(@typeInfo(field.type) == .Struct);
            positional_fields = std.meta.fields(field.type);
            for (positional_fields) |positional_field| {
                assert(default_value(positional_field) == null);
                assert_valid_value_type(positional_field.type);
            }
        } else {
            fields[field_count] = field;
            field_count += 1;

            switch (@typeInfo(field.type)) {
                .Bool => {
                    assert(default_value(field).? == false); // boolean flags should have a default
                },
                .Optional => |optional| {
                    assert(default_value(field).? == null); // optional flags should have a default
                    assert_valid_value_type(optional.child);
                },
                else => {
                    assert_valid_value_type(field.type);
                },
            }
        }
    };

    var result: Flags = undefined;
    var counts: std.enums.EnumFieldStruct(Flags, u32, 0) = .{};

    // When parsing arguments, we must consider longer arguments first, such that `--foo-bar=92` is
    // not confused for a misspelled `--foo=92`. Using `std.sort` for comptime-only values does not
    // work, so open-code insertion sort, and comptime assert order during the actual parsing.
    comptime {
        for (fields[0..field_count], 0..) |*field_right, i| {
            for (fields[0..i]) |*field_left| {
                if (field_left.name.len < field_right.name.len) {
                    std.mem.swap(std.builtin.Type.StructField, field_left, field_right);
                }
            }
        }
    }

    next_arg: while (args.next()) |arg| {
        comptime var field_len_prev = std.math.maxInt(usize);
        inline for (fields[0..field_count]) |field| {
            const flag = comptime flag_name(field);

            comptime assert(field_len_prev >= field.name.len);
            field_len_prev = field.name.len;
            if (std.mem.startsWith(u8, arg, flag)) {
                @field(counts, field.name) += 1;
                const flag_value = parse_flag(field.type, flag, arg);
                @field(result, field.name) = flag_value;
                continue :next_arg;
            }
        }

        if (@hasField(Flags, "positional")) {
            counts.positional += 1;
            inline for (positional_fields, 0..) |positional_field, positional_index| {
                const flag = comptime flag_name_positional(positional_field);

                if (arg.len == 0) fatal("{s}: empty argument", .{flag});
                // Prevent ambiguity between a flag and positional argument value. We could add
                // support for bare ` -- ` as a disambiguation mechanism once we have a real
                // use-case.
                if (arg[0] == '-') fatal("unexpected argument: '{s}'", .{arg});

                @field(result.positional, positional_field.name) =
                    parse_value(positional_field.type, flag, arg);
                if (positional_index + 1 == counts.positional) {
                    continue :next_arg;
                }
            }
        }

        fatal("unexpected argument: '{s}'", .{arg});
    }

    inline for (fields[0..field_count]) |field| {
        const flag = flag_name(field);
        switch (@field(counts, field.name)) {
            0 => if (default_value(field)) |default| {
                @field(result, field.name) = default;
            } else {
                fatal("{s}: argument is required", .{flag});
            },
            1 => {},
            else => fatal("{s}: duplicate argument", .{flag}),
        }
    }

    if (@hasField(Flags, "positional")) {
        assert(counts.positional <= positional_fields.len);
        inline for (positional_fields, 0..) |positional_field, positional_index| {
            if (counts.positional == positional_index) {
                const flag = comptime flag_name_positional(positional_field);
                fatal("{s}: argument is required", .{flag});
            }
        }
    }

    return result;
}

fn assert_valid_value_type(comptime T: type) void {
    comptime {
        if (T == []const u8 or T == [:0]const u8 or T == ByteSize or @typeInfo(T) == .Int) return;

        if (@typeInfo(T) == .Enum) {
            const info = @typeInfo(T).Enum;
            assert(info.is_exhaustive);
            assert(info.fields.len >= 2);
            return;
        }

        @compileLog("unsupported type", T);
        unreachable;
    }
}

/// Parse, e.g., `--cluster=123` into `123` integer
fn parse_flag(comptime T: type, flag: []const u8, arg: [:0]const u8) T {
    assert(flag[0] == '-' and flag[1] == '-');

    if (T == bool) {
        if (!std.mem.eql(u8, arg, flag)) {
            fatal("{s}: argument does not require a value in '{s}'", .{ flag, arg });
        }
        return true;
    }

    const value = parse_flag_split_value(flag, arg);
    assert(value.len > 0);
    return parse_value(T, flag, value);
}

/// Splits the value part from a `--arg=value` syntax.
fn parse_flag_split_value(flag: []const u8, arg: [:0]const u8) [:0]const u8 {
    assert(flag[0] == '-' and flag[1] == '-');
    assert(std.mem.startsWith(u8, arg, flag));

    const value = arg[flag.len..];
    if (value.len == 0) {
        fatal("{s}: expected value separator '='", .{flag});
    }
    if (value[0] != '=') {
        fatal(
            "{s}: expected value separator '=', but found '{c}' in '{s}'",
            .{ flag, value[0], arg },
        );
    }
    if (value.len == 1) fatal("{s}: argument requires a value", .{flag});
    return value[1..];
}

fn parse_value(comptime T: type, flag: []const u8, value: [:0]const u8) T {
    comptime assert(T != bool);
    assert((flag[0] == '-' and flag[1] == '-') or flag[0] == '<');
    assert(value.len > 0);

    const V = switch (@typeInfo(T)) {
        .Optional => |optional| optional.child,
        else => T,
    };

    if (V == []const u8 or V == [:0]const u8) return value;
    if (V == ByteSize) return parse_value_size(flag, value);
    if (@typeInfo(V) == .Int) return parse_value_int(V, flag, value);
    if (@typeInfo(V) == .Enum) return parse_value_enum(V, flag, value);
    comptime unreachable;
}

pub const ByteSize = struct { bytes: u64 };
fn parse_value_size(flag: []const u8, value: []const u8) ByteSize {
    assert((flag[0] == '-' and flag[1] == '-') or flag[0] == '<');

    const units = .{
        .{ &[_][]const u8{ "TiB", "tib", "TB", "tb" }, 1024 * 1024 * 1024 * 1024 },
        .{ &[_][]const u8{ "GiB", "gib", "GB", "gb" }, 1024 * 1024 * 1024 },
        .{ &[_][]const u8{ "MiB", "mib", "MB", "mb" }, 1024 * 1024 },
        .{ &[_][]const u8{ "KiB", "kib", "KB", "kb" }, 1024 },
    };

    const unit: struct { suffix: []const u8, scale: u64 } = unit: inline for (units) |unit| {
        const suffixes = unit[0];
        const scale = unit[1];
        for (suffixes) |suffix| {
            if (std.mem.endsWith(u8, value, suffix)) {
                break :unit .{ .suffix = suffix, .scale = scale };
            }
        }
    } else break :unit .{ .suffix = "", .scale = 1 };

    assert(std.mem.endsWith(u8, value, unit.suffix));
    const value_numeric = value[0 .. value.len - unit.suffix.len];

    const amount = std.fmt.parseUnsigned(u64, value_numeric, 10) catch |err| {
        switch (err) {
            error.Overflow => fatal(
                "{s}: value exceeds 64-bit unsigned integer: '{s}'",
                .{ flag, value },
            ),
            error.InvalidCharacter => fatal(
                "{s}: expected a size, but found '{s}' (invalid digit or suffix)",
                .{ flag, value },
            ),
        }
    };

    const bytes = std.math.mul(u64, amount, unit.scale) catch fatal(
        "{s}: size in bytes exceeds 64-bit unsigned integer: '{s}'",
        .{ flag, value },
    );
    return ByteSize{ .bytes = bytes };
}

test parse_value_size {
    const kib = 1024;
    const mib = kib * 1024;
    const gib = mib * 1024;
    const tib = gib * 1024;

    const cases = .{
        .{ 0, "0" },
        .{ 1, "1" },
        .{ 140737488355328, "140737488355328" },
        .{ 140737488355328, "128TiB" },
        .{ 1 * tib, "1TiB" },
        .{ 10 * tib, "10tib" },
        .{ 100 * tib, "100TB" },
        .{ 1000 * tib, "1000tb" },
        .{ 1 * gib, "1GiB" },
        .{ 10 * gib, "10gib" },
        .{ 100 * gib, "100GB" },
        .{ 1000 * gib, "1000gb" },
        .{ 1 * mib, "1MiB" },
        .{ 10 * mib, "10mib" },
        .{ 100 * mib, "100MB" },
        .{ 1000 * mib, "1000mb" },
        .{ 1 * kib, "1KiB" },
        .{ 10 * kib, "10kib" },
        .{ 100 * kib, "100KB" },
        .{ 1000 * kib, "1000kb" },
    };

    inline for (cases) |case| {
        const want = case[0];
        const input = case[1];
        const got = parse_value_size("--size", input);
        assert(want == got.bytes);
    }
}

/// Parse string value into an integer, providing a nice error message for the user.
fn parse_value_int(comptime T: type, flag: []const u8, value: [:0]const u8) T {
    assert((flag[0] == '-' and flag[1] == '-') or flag[0] == '<');

    return std.fmt.parseInt(T, value, 10) catch |err| {
        switch (err) {
            error.Overflow => fatal(
                "{s}: value exceeds {d}-bit {s} integer: '{s}'",
                .{ flag, @typeInfo(T).Int.bits, @tagName(@typeInfo(T).Int.signedness), value },
            ),
            error.InvalidCharacter => fatal(
                "{s}: expected an integer value, but found '{s}' (invalid digit)",
                .{ flag, value },
            ),
        }
    };
}

fn parse_value_enum(comptime E: type, flag: []const u8, value: [:0]const u8) E {
    assert((flag[0] == '-' and flag[1] == '-') or flag[0] == '<');
    comptime assert(@typeInfo(E).Enum.is_exhaustive);

    return std.meta.stringToEnum(E, value) orelse fatal(
        "{s}: expected one of {s}, but found '{s}'",
        .{ flag, comptime fields_to_comma_list(E), value },
    );
}

fn fields_to_comma_list(comptime E: type) []const u8 {
    comptime {
        const field_count = std.meta.fields(E).len;
        assert(field_count >= 2);

        var result: []const u8 = "";
        for (std.meta.fields(E), 0..) |field, field_index| {
            const separator = switch (field_index) {
                0 => "",
                else => ", ",
                field_count - 1 => if (field_count == 2) " or " else ", or ",
            };
            result = result ++ separator ++ "'" ++ field.name ++ "'";
        }
        return result;
    }
}

fn flag_name(comptime field: std.builtin.Type.StructField) []const u8 {
    // TODO(Zig): Cleanup when this is fixed after Zig 0.11.
    // Without comptime blk, the compiler thinks the result is a runtime slice returning a UAF.
    return comptime blk: {
        assert(!std.mem.eql(u8, field.name, "positional"));

        var result: []const u8 = "--";
        var index = 0;
        while (std.mem.indexOf(u8, field.name[index..], "_")) |i| {
            result = result ++ field.name[index..][0..i] ++ "-";
            index = index + i + 1;
        }
        result = result ++ field.name[index..];
        break :blk result;
    };
}

test flag_name {
    const field = @typeInfo(struct { statsd: bool }).Struct.fields[0];
    try std.testing.expectEqualStrings(flag_name(field), "--statsd");
}

fn flag_name_positional(comptime field: std.builtin.Type.StructField) []const u8 {
    comptime assert(std.mem.indexOf(u8, field.name, "_") == null);
    return "<" ++ field.name ++ ">";
}

/// This is essentially `field.default_value`, but with a useful type instead of `?*anyopaque`.
fn default_value(comptime field: std.builtin.Type.StructField) ?field.type {
    return if (field.default_value) |default_opaque|
        @as(*const field.type, @ptrCast(@alignCast(default_opaque))).*
    else
        null;
}

// CLI parsing makes a liberal use of `fatal`, so testing it within the process is impossible. We
// test it our of process by:
//   - using Zig compiler to build this vey file as an executable in a temporary directory,
//   - running the following main with various args and capturing stdout, stderr, and the exit code.
//   - asserting that the captured values are correct.
pub usingnamespace if (@import("root") != @This()) struct {
    // For production builds, don't include the main function.
    // This is `if __name__ == "__main__":` at comptime!
} else struct {
    const CliArgs = union(enum) {
        empty,
        prefix: struct {
            foo: u8 = 0,
            foo_bar: u8 = 0,
            opt: bool = false,
            option: bool = false,
        },
        pos: struct { flag: bool = false, positional: struct {
            p1: []const u8,
            p2: []const u8,
        } },
        required: struct {
            foo: u8,
            bar: u8,
        },
        values: struct {
            int: u32 = 0,
            size: ByteSize = .{ .bytes = 0 },
            boolean: bool = false,
            path: []const u8 = "not-set",
            optional: ?[]const u8 = null,
            choice: enum { marlowe, shakespeare } = .marlowe,
        },

        pub const help =
            \\ flags-test-program [flags]
            \\
        ;
    };

    pub fn main() !void {
        var gpa_allocator = std.heap.GeneralPurposeAllocator(.{}){};
        const gpa = gpa_allocator.allocator();

        var args = try std.process.argsWithAllocator(gpa);
        defer args.deinit();

        assert(args.skip());

        const cli_args = parse_commands(&args, CliArgs);

        const stdout = std.io.getStdOut();
        const out_stream = stdout.writer();
        switch (cli_args) {
            .empty => try out_stream.print("empty\n", .{}),
            .prefix => |values| {
                try out_stream.print("foo: {}\n", .{values.foo});
                try out_stream.print("foo-bar: {}\n", .{values.foo_bar});
                try out_stream.print("opt: {}\n", .{values.opt});
                try out_stream.print("option: {}\n", .{values.option});
            },
            .pos => |values| {
                try out_stream.print("p1: {s}\n", .{values.positional.p1});
                try out_stream.print("p2: {s}\n", .{values.positional.p2});
                try out_stream.print("flag: {}\n", .{values.flag});
            },
            .required => |required| {
                try out_stream.print("foo: {}\n", .{required.foo});
                try out_stream.print("bar: {}\n", .{required.bar});
            },
            .values => |values| {
                try out_stream.print("int: {}\n", .{values.int});
                try out_stream.print("size: {}\n", .{values.size.bytes});
                try out_stream.print("boolean: {}\n", .{values.boolean});
                try out_stream.print("path: {s}\n", .{values.path});
                try out_stream.print("optional: {?s}\n", .{values.optional});
                try out_stream.print("choice: {?s}\n", .{@tagName(values.choice)});
            },
        }
    }
};

test "flags" {
    const Snap = @import("./testing/snaptest.zig").Snap;
    const snap = Snap.snap;

    const T = struct {
        const T = @This();

        gpa: std.mem.Allocator,
        tmp_dir: std.testing.TmpDir,
        output_buf: std.ArrayList(u8),
        flags_exe_buf: *[std.fs.MAX_PATH_BYTES]u8,
        flags_exe: []const u8,

        fn init(gpa: std.mem.Allocator) !T {
            // TODO: Avoid std.os.getenv() as it currently causes a linker error on windows.
            // See: https://github.com/ziglang/zig/issues/8456
            const zig_exe = std.process.getEnvVarOwned(gpa, "ZIG_EXE") catch |e| switch (e) {
                error.EnvironmentVariableNotFound => return error.SkipZigTest,
                error.InvalidUtf8 => unreachable,
                error.OutOfMemory => return error.OutOfMemory,
            };
            defer gpa.free(zig_exe);

            var tmp_dir = std.testing.tmpDir(.{});
            errdefer tmp_dir.cleanup();

            const output_buf = std.ArrayList(u8).init(gpa);
            errdefer output_buf.deinit();

            const flags_exe_buf = try gpa.create([std.fs.MAX_PATH_BYTES]u8);
            errdefer gpa.destroy(flags_exe_buf);

            const argv = [_][]const u8{ zig_exe, "build-exe", @src().file };
            const exec_result = try std.ChildProcess.exec(.{
                .allocator = gpa,
                .argv = &argv,
                .cwd_dir = tmp_dir.dir,
            });
            defer gpa.free(exec_result.stdout);
            defer gpa.free(exec_result.stderr);

            if (exec_result.term.Exited != 0) {
                std.debug.print("{s}{s}", .{ exec_result.stdout, exec_result.stderr });
                return error.FailedToCompile;
            }

            const flags_exe = try tmp_dir.dir.realpath(
                "flags" ++ if (builtin.os.tag == .windows) ".exe" else "",
                flags_exe_buf,
            );

            const sanity_check = try std.fs.openFileAbsolute(flags_exe, .{});
            sanity_check.close();

            return .{
                .gpa = gpa,
                .tmp_dir = tmp_dir,
                .output_buf = output_buf,
                .flags_exe_buf = flags_exe_buf,
                .flags_exe = flags_exe,
            };
        }

        fn deinit(t: *T) void {
            t.gpa.destroy(t.flags_exe_buf);
            t.output_buf.deinit();
            t.tmp_dir.cleanup();
            t.* = undefined;
        }

        fn check(t: *T, cli: []const []const u8, want: Snap) !void {
            const argv = try t.gpa.alloc([]const u8, cli.len + 1);
            defer t.gpa.free(argv);

            argv[0] = t.flags_exe;
            for (argv[1..], 0..) |*arg, i| {
                arg.* = cli[i];
            }
            if (cli.len > 0) {
                assert(argv[argv.len - 1].ptr == cli[cli.len - 1].ptr);
            }

            const exec_result = try std.ChildProcess.exec(.{
                .allocator = t.gpa,
                .argv = argv,
            });
            defer t.gpa.free(exec_result.stdout);
            defer t.gpa.free(exec_result.stderr);

            t.output_buf.clearRetainingCapacity();

            if (exec_result.term.Exited != 0) {
                try t.output_buf.writer().print("status: {}\n", .{exec_result.term.Exited});
            }
            if (exec_result.stdout.len > 0) {
                try t.output_buf.writer().print("stdout:\n{s}", .{exec_result.stdout});
            }
            if (exec_result.stderr.len > 0) {
                try t.output_buf.writer().print("stderr:\n{s}", .{exec_result.stderr});
            }

            try want.diff(t.output_buf.items);
        }
    };

    var t = try T.init(std.testing.allocator);
    defer t.deinit();

    // Test-cases are roughly in the source order of the corresponding features.

    try t.check(&.{"empty"}, snap(@src(),
        \\stdout:
        \\empty
        \\
    ));

    try t.check(&.{}, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: subcommand required, expected 'empty', 'prefix', 'pos', 'required', or 'values'
        \\
    ));

    try t.check(&.{"-h"}, snap(@src(),
        \\stdout:
        \\ flags-test-program [flags]
        \\
    ));

    try t.check(&.{"--help"}, snap(@src(),
        \\stdout:
        \\ flags-test-program [flags]
        \\
    ));

    try t.check(&.{""}, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: unknown subcommand: ''
        \\
    ));

    try t.check(&.{"bogus"}, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: unknown subcommand: 'bogus'
        \\
    ));

    try t.check(&.{"--int=92"}, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: unknown subcommand: '--int=92'
        \\
    ));

    try t.check(&.{ "empty", "--help" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: unexpected argument: '--help'
        \\
    ));

    try t.check(&.{ "prefix", "--foo=92" }, snap(@src(),
        \\stdout:
        \\foo: 92
        \\foo-bar: 0
        \\opt: false
        \\option: false
        \\
    ));

    try t.check(&.{ "prefix", "--foo-bar=92" }, snap(@src(),
        \\stdout:
        \\foo: 0
        \\foo-bar: 92
        \\opt: false
        \\option: false
        \\
    ));

    try t.check(&.{ "prefix", "--foo-baz=92" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --foo: expected value separator '=', but found '-' in '--foo-baz=92'
        \\
    ));

    try t.check(&.{ "prefix", "--opt" }, snap(@src(),
        \\stdout:
        \\foo: 0
        \\foo-bar: 0
        \\opt: true
        \\option: false
        \\
    ));

    try t.check(&.{ "prefix", "--option" }, snap(@src(),
        \\stdout:
        \\foo: 0
        \\foo-bar: 0
        \\opt: false
        \\option: true
        \\
    ));

    try t.check(&.{ "prefix", "--optx" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --opt: argument does not require a value in '--optx'
        \\
    ));

    try t.check(&.{ "pos", "x", "y" }, snap(@src(),
        \\stdout:
        \\p1: y
        \\p2: y
        \\flag: false
        \\
    ));

    try t.check(&.{"pos"}, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: <p1>: argument is required
        \\
    ));

    try t.check(&.{ "pos", "x" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: <p2>: argument is required
        \\
    ));

    try t.check(&.{ "pos", "x", "y", "z" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: unexpected argument: 'z'
        \\
    ));

    try t.check(&.{ "pos", "" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: <p1>: empty argument
        \\
    ));

    try t.check(&.{ "pos", "--flag", "x", "y" }, snap(@src(),
        \\stdout:
        \\p1: y
        \\p2: y
        \\flag: true
        \\
    ));

    try t.check(&.{ "pos", "--flak", "x", "y" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: unexpected argument: '--flak'
        \\
    ));

    try t.check(&.{ "required", "--foo=1", "--bar=2" }, snap(@src(),
        \\stdout:
        \\foo: 1
        \\bar: 2
        \\
    ));

    try t.check(&.{ "required", "--surprise" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: unexpected argument: '--surprise'
        \\
    ));

    try t.check(&.{ "required", "--foo=1" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --bar: argument is required
        \\
    ));

    try t.check(&.{ "required", "--foo=1", "--bar=2", "--foo=3" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --foo: duplicate argument
        \\
    ));

    try t.check(&.{
        "values",
        "--int=92",
        "--size=1GiB",
        "--boolean",
        "--path=/home",
        "--optional=some",
        "--choice=shakespeare",
    }, snap(@src(),
        \\stdout:
        \\int: 92
        \\size: 1073741824
        \\boolean: true
        \\path: /home
        \\optional: some
        \\choice: shakespeare
        \\
    ));

    try t.check(&.{"values"}, snap(@src(),
        \\stdout:
        \\int: 0
        \\size: 0
        \\boolean: false
        \\path: not-set
        \\optional: null
        \\choice: marlowe
        \\
    ));

    try t.check(&.{ "values", "--boolean=true" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --boolean: argument does not require a value in '--boolean=true'
        \\
    ));

    try t.check(&.{ "values", "--int" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --int: expected value separator '='
        \\
    ));

    try t.check(&.{ "values", "--int:" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --int: expected value separator '=', but found ':' in '--int:'
        \\
    ));

    try t.check(&.{ "values", "--int=" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --int: argument requires a value
        \\
    ));

    try t.check(&.{ "values", "--int=92" }, snap(@src(),
        \\stdout:
        \\int: 92
        \\size: 0
        \\boolean: false
        \\path: not-set
        \\optional: null
        \\choice: marlowe
        \\
    ));

    try t.check(&.{ "values", "--int=XCII" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --int: expected an integer value, but found 'XCII' (invalid digit)
        \\
    ));

    try t.check(&.{ "values", "--int=44444444444444444444" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --int: value exceeds 32-bit unsigned integer: '44444444444444444444'
        \\
    ));

    try t.check(&.{ "values", "--size=3MiB" }, snap(@src(),
        \\stdout:
        \\int: 0
        \\size: 3145728
        \\boolean: false
        \\path: not-set
        \\optional: null
        \\choice: marlowe
        \\
    ));

    try t.check(&.{ "values", "--size=44444444444444444444" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --size: value exceeds 64-bit unsigned integer: '44444444444444444444'
        \\
    ));

    try t.check(&.{ "values", "--size=100000000000000000" }, snap(@src(),
        \\stdout:
        \\int: 0
        \\size: 100000000000000000
        \\boolean: false
        \\path: not-set
        \\optional: null
        \\choice: marlowe
        \\
    ));

    try t.check(&.{ "values", "--size=100000000000000000kb" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --size: size in bytes exceeds 64-bit unsigned integer: '100000000000000000kb'
        \\
    ));

    try t.check(&.{ "values", "--size=3Mib" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --size: expected a size, but found '3Mib' (invalid digit or suffix)
        \\
    ));

    try t.check(&.{ "values", "--path=" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --path: argument requires a value
        \\
    ));

    try t.check(&.{ "values", "--optional=" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --optional: argument requires a value
        \\
    ));

    try t.check(&.{ "values", "--choice=molière" }, snap(@src(),
        \\status: 1
        \\stderr:
        \\error: --choice: expected one of 'marlowe' or 'shakespeare', but found 'molière'
        \\
    ));
}
