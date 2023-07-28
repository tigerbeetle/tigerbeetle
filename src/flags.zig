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
//!   Fatal errors make testing awkward, we'll need to wait for this Zig issue to test this code:
//!   <https://github.com/ziglang/zig/issues/1356>.
//!
//! - Concise DSL. Most cli parsing is done for ad-hoc tools like benchmarking, where the ability to
//!   quickly add a new argument is valuable. As this is a 80% solution, production code may use
//!   more verbose approach if it gives better UX.
//!
//! - Caller manages ArgsIterator. ArgsIterator owns the backing memory of the args, so we let the
//!   caller to manage the lifetime. The caller should be skipping program name.

const std = @import("std");
const assert = std.debug.assert;

/// Format and print an error message to stderr, then exit with an exit code of 1.
pub fn fatal(comptime fmt_string: []const u8, args: anytype) noreturn {
    const stderr = std.io.getStdErr().writer();
    stderr.print("error: " ++ fmt_string ++ "\n", args) catch {};
    std.os.exit(1);
}

/// Parse CLI arguments for subcommands specified as Zig `union(enum)`:
///
/// ```
/// const cli_args = parse_commands(&args, union(enum) {
///    help,
///    start: struct { addresses: []const u8, replica: u32 },
///    format: struct { verbose: bool = false },
/// })
/// ```
pub fn parse_commands(args: *std.process.ArgIterator, comptime Commands: type) Commands {
    assert(@typeInfo(Commands) == .Union);

    const subcommand = args.next() orelse fatal("subcommand required", .{});
    inline for (comptime std.meta.fields(Commands)) |field| {
        comptime assert(std.mem.indexOf(u8, field.name, "_") == null);
        if (std.mem.eql(u8, subcommand, field.name)) {
            return @unionInit(Commands, field.name, parse_flags(args, field.field_type));
        }
    }
    fatal("unknown subcommand: '{s}'", .{subcommand});
}

/// Parse CLI arguments for a single command specified as Zig `struct`:
///
/// ```
/// const cli_args = parse_commands(&args, struct {
///    verbose: bool = false,
///    replica: u32,
///    path: []const u8 = "0_0.tigerbeetle"
/// })
/// ```
pub fn parse_flags(args: *std.process.ArgIterator, comptime Flags: type) Flags {
    if (Flags == void) {
        if (args.next()) |arg| {
            fatal("unexpected argument: '{s}'", .{arg});
        }
        return {};
    }

    assert(@typeInfo(Flags) == .Struct);

    const fields = comptime std.meta.fields(Flags);
    comptime assert(fields.len >= 1);

    comptime for (fields) |field| {
        const T = field.field_type;
        if (T == bool) {
            assert(default_value(field) == false); // boolean flags should have explicit default
        } else {
            assert(T == []const u8 or @typeInfo(T) == .Int); // unsupported CLI argument type
        }
    };

    var result: Flags = undefined;
    var counts: std.enums.EnumFieldStruct(Flags, u32, 0) = .{};

    // When parsing arguments, we must consider longer arguments first, such that `--foo-bar=92` is
    // not confused for a misspelled `--foo=92`. Using `std.sort` for comptime-only values does not
    // work, so open-code insertion sort by indexes, and comptime assert order during the actual
    // parsing.
    comptime var fields_longer_first: [fields.len]usize = undefined;
    comptime {
        for (fields_longer_first) |*index, i| index.* = i;
        for (fields_longer_first) |*index_right, i| {
            for (fields_longer_first[0..i]) |*index_left| {
                if (fields[index_left.*].name.len < fields[index_right.*].name.len) {
                    std.mem.swap(usize, index_left, index_right);
                }
            }
        }
    }

    next_arg: while (args.next()) |arg| {
        comptime var field_len_prev = std.math.maxInt(usize);
        inline for (fields_longer_first) |field_index| {
            const field = comptime fields[field_index];
            const flag = comptime field_to_flag(field.name);

            comptime assert(field_len_prev >= field.name.len);
            field_len_prev = field.name.len;
            if (std.mem.startsWith(u8, arg, flag)) {
                @field(counts, field.name) += 1;
                const flag_value = parse_flag(field.field_type, flag, arg);
                @field(result, field.name) = flag_value;
                continue :next_arg;
            }
        }

        fatal("unexpected argument: '{s}'", .{arg});
    }

    inline for (fields) |field| {
        const flag = comptime field_to_flag(field.name);
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

    return result;
}

/// Parse, e.g., `--cluster=123` into `123` integer
fn parse_flag(comptime T: type, comptime flag: []const u8, arg: [:0]const u8) T {
    comptime assert(flag[0] == '-' and flag[1] == '-');

    if (T == bool) {
        if (!std.mem.eql(u8, arg, flag)) {
            fatal("{s}: argument does not require a value in '{s}'", .{ flag, arg });
        }
        return true;
    }

    const value = parse_flag_value(flag, arg);
    assert(value.len > 0);

    if (T == []const u8) {
        return value;
    }
    assert(@typeInfo(T) == .Int);
    return parse_flag_value_int(T, flag, value);
}

/// Splits the value part from a `--arg=value` syntax.
fn parse_flag_value(comptime flag: []const u8, arg: [:0]const u8) [:0]const u8 {
    assert(std.mem.startsWith(u8, arg, flag));
    const value = arg[flag.len..];
    if (value.len < 2) {
        fatal("{s}: argument requires a value", .{flag});
    }
    if (value[0] != '=') {
        fatal(
            "{s}: expected value separator '=', but found '{c}' in '{s}'",
            .{ flag, value[0], arg },
        );
    }
    return value[1..];
}

/// Parse string value into an integer, providing a nice error message for the user.
fn parse_flag_value_int(comptime T: type, comptime flag: []const u8, value: [:0]const u8) T {
    return std.fmt.parseInt(T, value, 10) catch |err| {
        fatal("{s}: expected an integer value, but found '{s}' ({s})", .{
            flag, value, switch (err) {
                error.Overflow => "value too large",
                error.InvalidCharacter => "invalid digit",
            },
        });
    };
}

fn field_to_flag(comptime field: []const u8) []const u8 {
    comptime var result: []const u8 = "--";
    comptime {
        var index = 0;
        while (std.mem.indexOf(u8, field[index..], "_")) |i| {
            result = result ++ field[index..][0..i] ++ "-";
            index = index + i + 1;
        }
        result = result ++ field[index..];
    }
    return result;
}

test field_to_flag {
    try std.testing.expectEqualStrings(field_to_flag("enable_statsd"), "--enable-statsd");
}

/// This is essentially `field.default_value`, but with a useful type instead of `?*anyopaque`.
fn default_value(comptime field: std.builtin.Type.StructField) ?field.field_type {
    return if (field.default_value) |default_opaque|
        @ptrCast(
            *const field.field_type,
            @alignCast(@alignOf(field.field_type), default_opaque),
        ).*
    else
        null;
}
