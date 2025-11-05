const std = @import("std");
const vsr = @import("vsr");
const exports = vsr.tb_client.exports;
const assert = std.debug.assert;

const tb = vsr.tigerbeetle;

/// VSR type mappings: these will always be the same regardless of state machine.
const mappings_vsr = .{
    .{ exports.tb_operation, "Operation" },
    .{ exports.tb_packet_status, "PacketStatus" },
    .{ exports.tb_packet_t, "Packet" },
    .{ exports.tb_client_t, "Client" },
    .{ exports.tb_init_status, "InitStatus" },
    .{ exports.tb_client_status, "ClientStatus" },
    .{ exports.tb_log_level, "LogLevel" },
    .{ exports.tb_register_log_callback_status, "RegisterLogCallbackStatus" },
};

/// State machine specific mappings: in future, these should be pulled automatically from the state
/// machine.
const mappings_state_machine = .{
    .{ tb.AccountFlags, "AccountFlags" },
    .{ tb.TransferFlags, "TransferFlags" },
    .{ tb.AccountFilterFlags, "AccountFilterFlags" },
    .{ tb.QueryFilterFlags, "QueryFilterFlags" },
    .{ tb.Account, "Account" },
    .{ tb.Transfer, "Transfer" },
    .{ tb.CreateAccountResult, "CreateAccountResult" },
    .{ tb.CreateTransferResult, "CreateTransferResult" },
    .{ tb.CreateAccountsResult, "CreateAccountsResult" },
    .{ tb.CreateTransfersResult, "CreateTransfersResult" },
    .{ tb.AccountFilter, "AccountFilter" },
    .{ tb.AccountBalance, "AccountBalance" },
    .{ tb.QueryFilter, "QueryFilter" },
};

const mappings_all = mappings_vsr ++ mappings_state_machine;

const Buffer = struct {
    inner: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) Buffer {
        return .{
            .inner = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn print(self: *Buffer, comptime format: []const u8, args: anytype) void {
        self.inner.writer().print(format, args) catch unreachable;
    }
};

fn mapping_name_from_type(mappings: anytype, Type: type) ?[]const u8 {
    comptime for (mappings) |mapping| {
        const ZigType, const python_name = mapping;

        if (Type == ZigType) {
            return python_name;
        }
    };
    return null;
}

/// Resolves a Zig Type into a string representing the name of a corresponding Python ctype. This
/// resolves both VSR and state machine specific mappings, as both are needed when interfacing via
/// FFI.
fn zig_to_ctype(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .array => |info| {
            return std.fmt.comptimePrint("{s} * {d}", .{
                comptime zig_to_ctype(info.child),
                info.len,
            });
        },
        .@"enum" => |info| return zig_to_ctype(info.tag_type),
        .@"struct" => return zig_to_ctype(std.meta.Int(.unsigned, @bitSizeOf(Type))),
        .bool => return "ctypes.c_bool",
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "ctypes.c_uint8",
                16 => "ctypes.c_uint16",
                32 => "ctypes.c_uint32",
                64 => "ctypes.c_uint64",
                128 => "c_uint128",
                else => @compileError("invalid int type"),
            };
        },
        .optional => |info| switch (@typeInfo(info.child)) {
            .pointer => return zig_to_ctype(info.child),
            else => @compileError("Unsupported optional type: " ++ @typeName(Type)),
        },
        .pointer => |info| {
            assert(info.size == .one);
            assert(!info.is_allowzero);

            if (Type == *anyopaque) {
                return "ctypes.c_void_p";
            }

            return comptime "ctypes.POINTER(C" ++
                mapping_name_from_type(mappings_all, info.child).? ++
                ")";
        },
        .void => return "None",
        else => @compileError("Unhandled type: " ++ @typeName(Type)),
    }
}

/// Resolves a Zig Type into a string representing the name of a corresponding Python dataclass.
/// Unlike zig_to_ctype, this only resolves state machine specific mappings: VSR mappings are
/// internal to the client, and not exposed to calling code.
fn zig_to_python(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .@"enum" => return comptime mapping_name_from_type(mappings_state_machine, Type).?,
        .array => |info| {
            return std.fmt.comptimePrint("{s}[{d}]", .{
                comptime zig_to_python(info.child),
                info.len,
            });
        },
        .@"struct" => return comptime mapping_name_from_type(mappings_state_machine, Type).?,
        .bool => return "bool",
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "int",
                16 => "int",
                32 => "int",
                64 => "int",
                128 => "int",
                else => @compileError("invalid int type"),
            };
        },
        .void => return "None",
        else => @compileError("Unhandled type: " ++ @typeName(Type)),
    }
}

fn to_uppercase(comptime input: []const u8) [input.len]u8 {
    comptime var output: [input.len]u8 = undefined;
    inline for (&output, 0..) |*char, i| {
        char.* = input[i];
        char.* -= 32 * @as(u8, @intFromBool(char.* >= 'a' and char.* <= 'z'));
    }
    return output;
}

fn emit_enum(
    buffer: *Buffer,
    comptime Type: type,
    comptime type_info: anytype,
    comptime python_name: []const u8,
    comptime skip_fields: []const []const u8,
) !void {
    if (@typeInfo(Type) == .@"enum") {
        buffer.print("class {s}(enum.IntEnum):\n", .{python_name});
    } else {
        // Packed structs.
        assert(@typeInfo(Type) == .@"struct" and @typeInfo(Type).@"struct".layout == .@"packed");

        buffer.print("class {s}(enum.IntFlag):\n", .{python_name});
        buffer.print("    NONE = 0\n", .{});
    }

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        comptime var skip = false;
        inline for (skip_fields) |sf| {
            skip = skip or comptime std.mem.eql(u8, sf, field.name);
        }

        if (!skip) {
            const field_name = to_uppercase(field.name);
            if (@typeInfo(Type) == .@"enum") {
                buffer.print("    {s} = {}\n", .{
                    @as([]const u8, &field_name),
                    @intFromEnum(@field(Type, field.name)),
                });
            } else {
                // Packed structs.
                buffer.print("    {s} = 1 << {}\n", .{
                    @as([]const u8, &field_name),
                    i,
                });
            }
        }
    }

    buffer.print("\n\n", .{});
}

fn emit_struct_ctypes(
    buffer: *Buffer,
    comptime type_info: anytype,
    comptime c_name: []const u8,
    generate_ctypes_to_python: bool,
) !void {
    buffer.print(
        \\class C{[type_name]s}(ctypes.Structure):
        \\    @classmethod
        \\    def from_param(cls, obj: Any) -> Self:
        \\
    , .{
        .type_name = c_name,
    });

    inline for (type_info.fields) |field| {
        const field_type_info = @typeInfo(field.type);

        // Emit a bounds check for all integer types that aren't using the custom c_uint128 class.
        // That has an explicit check built in, but the standard Python ctypes ones (eg,
        // ctypes.c_uint64) don't and will happily overflow otherwise.
        if (comptime !std.mem.eql(u8, field.name, "reserved") and field_type_info == .int) {
            buffer.print("        validate_uint(bits={[int_bits]}, name=\"{[field_name]s}\", " ++
                "number=obj.{[field_name]s})\n", .{
                .field_name = field.name,
                .int_bits = field_type_info.int.bits,
            });
        }
    }

    buffer.print("        return cls(\n", .{});

    inline for (type_info.fields) |field| {
        const field_type_info = @typeInfo(field.type);
        const field_is_u128 = field_type_info == .int and field_type_info.int.bits == 128;
        const convert_prefix = if (field_is_u128) "c_uint128.from_param(" else "";
        const convert_suffix = if (field_is_u128) ")" else "";

        if (comptime !std.mem.eql(u8, field.name, "reserved")) {
            buffer.print("            {[field_name]s}={[convert_prefix]s}" ++
                "obj.{[field_name]s}{[convert_suffix]s},\n", .{
                .field_name = field.name,
                .convert_prefix = convert_prefix,
                .convert_suffix = convert_suffix,
            });
        }
    }
    buffer.print("        )\n\n", .{});

    if (generate_ctypes_to_python) {
        buffer.print(
            \\
            \\    def to_python(self) -> {[type_name]s}:
            \\        return {[type_name]s}(
            \\
        , .{
            .type_name = c_name,
        });

        inline for (type_info.fields) |field| {
            if (comptime !std.mem.eql(u8, field.name, "reserved")) {
                buffer.print("            {s}={s},\n", .{
                    field.name,
                    convert_ctypes_to_python("self." ++ field.name, field.type),
                });
            }
        }
        buffer.print("        )\n\n", .{});
    }

    buffer.print("C{s}._fields_ = [ # noqa: SLF001\n", .{c_name});

    inline for (type_info.fields) |field| {
        buffer.print("    (\"{s}\", {s}),", .{
            field.name,
            zig_to_ctype(field.type),
        });

        buffer.print("\n", .{});
    }

    buffer.print("]\n\n\n", .{});
}

fn convert_ctypes_to_python(comptime name: []const u8, comptime Type: type) []const u8 {
    inline for (mappings_state_machine) |type_mapping| {
        const ZigType, const python_name = type_mapping;

        if (ZigType == Type) {
            return python_name ++ "(" ++ name ++ ")";
        }
    }
    if (@typeInfo(Type) == .int and @typeInfo(Type).int.bits == 128) {
        return name ++ ".to_python()";
    }

    return name;
}

fn emit_struct_dataclass(
    buffer: *Buffer,
    comptime type_info: anytype,
    comptime c_name: []const u8,
) !void {
    buffer.print("@dataclass\n", .{});
    buffer.print("class {s}:\n", .{c_name});

    inline for (type_info.fields) |field| {
        const field_type_info = @typeInfo(field.type);
        if (comptime !std.mem.eql(u8, field.name, "reserved")) {
            const python_type = zig_to_python(field.type);
            buffer.print("    {[name]s}: {[python_type]s} = ", .{
                .name = field.name,
                .python_type = python_type,
            });

            if (field_type_info == .@"struct" and field_type_info.@"struct".layout == .@"packed") {
                // Flags:
                buffer.print("{s}.NONE\n", .{python_type});
            } else {
                if (field_type_info == .@"enum") {
                    // Enums - the only ones exposed by the client call `.0` as `.OK`:
                    buffer.print("{s}.OK\n", .{python_type});
                } else {
                    // Simple integer types:
                    buffer.print("0\n", .{});
                }
            }
        }
    }

    buffer.print("\n\n", .{});
}

fn ctype_type_name(comptime Type: type) []const u8 {
    if (Type == u128) {
        return "c_uint128";
    }

    return comptime "C" ++ mapping_name_from_type(mappings_all, Type).?;
}

fn emit_method(
    buffer: *Buffer,
    comptime operation: tb.Operation,
    options: struct { is_async: bool },
) void {
    const event_type = comptime if (operation.is_batchable())
        "list[" ++ zig_to_python(operation.EventType()) ++ "]"
    else
        zig_to_python(operation.EventType());

    const result_type =
        comptime "list[" ++ zig_to_python(operation.ResultType()) ++ "]";

    // For ergonomics, the client allows calling things like .query_accounts(filter) even
    // though the _submit function requires a list for everything. Wrap them here.
    const event_name_or_list = comptime if (!operation.is_batchable())
        "[" ++ event_name(operation) ++ "]"
    else
        event_name(operation);

    // NB: _submit is loosely annotated, the operations define interfaces for the Python developer.
    buffer.print(
        \\    {[prefix_fn]s}def {[fn_name]s}(self, {[event_name]s}: {[event_type]s}) -> {[result_type]s}:
        \\        return {[prefix_call]s}self._submit(  # type: ignore[no-any-return]
        \\            Operation.{[uppercase_name]s},
        \\            {[event_name_or_list]s},
        \\            {[event_type_c]s},
        \\            {[result_type_c]s},
        \\        )
        \\
        \\
    ,
        .{
            .prefix_fn = if (options.is_async) "async " else "",
            .fn_name = @tagName(operation),
            .event_name = event_name(operation),
            .event_type = event_type,
            .result_type = result_type,
            .event_name_or_list = event_name_or_list,
            .prefix_call = if (options.is_async) "await " else "",
            .uppercase_name = to_uppercase(@tagName(operation)),
            .event_type_c = ctype_type_name(operation.EventType()),
            .result_type_c = ctype_type_name(operation.ResultType()),
        },
    );
}

pub fn main() !void {
    @setEvalBranchQuota(100_000);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = Buffer.init(allocator);
    buffer.print(
        \\##########################################################
        \\## This file was auto-generated by tb_client_header.zig ##
        \\##              Do not manually modify.                 ##
        \\##########################################################
        \\from __future__ import annotations
        \\
        \\import ctypes
        \\import enum
        \\import sys
        \\from dataclasses import dataclass
        \\from collections.abc import Callable # noqa: TCH003
        \\from typing import Any
        \\if sys.version_info >= (3, 11):
        \\    from typing import Self
        \\else:
        \\    from typing_extensions import Self
        \\
        \\from .lib import c_uint128, tbclient, validate_uint
        \\
        \\# Use slots=True if the version of Python is new enough (3.10+) to support it.
        \\if sys.version_info >= (3, 10):
        \\    # mypy: ignore assignment (3.10+) and unused-ignore (pre 3.10)
        \\    dataclass = dataclass(slots=True) # type: ignore[assignment, unused-ignore]
        \\
        \\
        \\
    , .{});

    // Emit enum and direct declarations.
    inline for (mappings_all) |type_mapping| {
        const ZigType, const python_name = type_mapping;

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError("Invalid C struct type: " ++ @typeName(ZigType)),
                .@"packed" => try emit_enum(&buffer, ZigType, info, python_name, &.{"padding"}),
                .@"extern" => continue,
            },
            .@"enum" => |info| {
                comptime var skip: []const []const u8 = &.{};
                if (ZigType == exports.tb_operation) {
                    skip = &.{ "reserved", "root", "register" };
                }

                try emit_enum(&buffer, ZigType, info, python_name, skip);
            },
            else => buffer.print("{s} = {s}\n\n", .{
                python_name,
                zig_to_ctype(ZigType),
            }),
        }
    }

    // Emit dataclass declarations
    inline for (mappings_state_machine) |type_mapping| {
        const ZigType, const python_name = type_mapping;

        // Enums, non-extern structs and everything else have been emitted by the first pass.
        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .@"extern" => try emit_struct_dataclass(&buffer, info, python_name),
                else => {},
            },
            else => {},
        }
    }

    // Emit ctype struct and enum type declarations.
    inline for (mappings_all) |type_mapping| {
        const ZigType, const python_name = type_mapping;

        // VSR ctype structs don't have a corresponding Python dataclass - so don't generate the
        // `def to_python(self):` method for them.
        const generate_ctypes_to_python = comptime mapping_name_from_type(
            mappings_state_machine,
            ZigType,
        ) != null;

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError("Invalid C struct type: " ++ @typeName(ZigType)),
                .@"packed" => continue,
                .@"extern" => try emit_struct_ctypes(
                    &buffer,
                    info,
                    python_name,
                    generate_ctypes_to_python,
                ),
            },
            else => continue,
        }
    }

    // Emit function declarations corresponding to the underlying libtbclient exported functions.
    // TODO: use `std.meta.declaractions` and generate with pub + export functions.
    buffer.print(
        \\# Don't be tempted to use c_char_p for bytes_ptr - it's for null terminated strings only.
        \\OnCompletion = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(CPacket),
        \\                                ctypes.c_uint64, ctypes.c_void_p, ctypes.c_uint32)
        \\LogHandler = ctypes.CFUNCTYPE(None, ctypes.c_uint, ctypes.c_void_p, ctypes.c_uint)
        \\
        \\class InitParameters(ctypes.Structure):
        \\    _fields_ = [("cluster_id", c_uint128), ("client_id", c_uint128),
        \\                ("addresses_ptr", ctypes.c_void_p), ("addresses_len", ctypes.c_uint64)]
        \\
        \\# Initialize a new TigerBeetle client which connects to the addresses provided and
        \\# completes submitted packets by invoking the callback with the given context.
        \\tb_client_init = tbclient.tb_client_init
        \\tb_client_init.restype = InitStatus
        \\tb_client_init.argtypes = [ctypes.POINTER(CClient), ctypes.POINTER(ctypes.c_uint8 * 16),
        \\                           ctypes.c_char_p, ctypes.c_uint32, ctypes.c_void_p,
        \\                           OnCompletion]
        \\
        \\# Initialize a new TigerBeetle client which echos back any data submitted.
        \\tb_client_init_echo = tbclient.tb_client_init_echo
        \\tb_client_init_echo.restype = InitStatus
        \\tb_client_init_echo.argtypes = [ctypes.POINTER(CClient), ctypes.POINTER(ctypes.c_uint8 * 16),
        \\                                ctypes.c_char_p, ctypes.c_uint32, ctypes.c_void_p,
        \\                                OnCompletion]
        \\
        \\# Returns the cluster_id and addresses passed in to either tb_client_init or
        \\# tb_client_init_echo.
        \\tb_client_init_parameters = tbclient.tb_client_init_parameters
        \\tb_client_init_parameters.restype = ClientStatus
        \\tb_client_init_parameters.argtypes = [ctypes.POINTER(CClient),
        \\                                      ctypes.POINTER(InitParameters)]
        \\
        \\# Closes the client, causing any previously submitted packets to be completed with
        \\# `TB_PACKET_CLIENT_SHUTDOWN` before freeing any allocated client resources from init.
        \\# It is undefined behavior to use any functions on the client once deinit is called.
        \\tb_client_deinit = tbclient.tb_client_deinit
        \\tb_client_deinit.restype = ClientStatus
        \\tb_client_deinit.argtypes = [ctypes.POINTER(CClient)]
        \\
        \\# Submit a packet with its operation, data, and data_size fields set.
        \\# Once completed, `on_completion` will be invoked with `on_completion_ctx` and the given
        \\# packet on the `tb_client` thread (separate from caller's thread).
        \\tb_client_submit = tbclient.tb_client_submit
        \\tb_client_submit.restype = ClientStatus
        \\tb_client_submit.argtypes = [ctypes.POINTER(CClient), ctypes.POINTER(CPacket)]
        \\
        \\tb_client_register_log_callback = tbclient.tb_client_register_log_callback
        \\tb_client_register_log_callback.restype = RegisterLogCallbackStatus
        \\# Need to pass in None to clear - ctypes will error if argtypes is set.
        \\# tb_client_register_log_callback.argtypes = [LogHandler, ctypes.c_bool]
        \\
        \\
        \\
    , .{});

    inline for (.{ true, false }) |is_async| {
        const prefix_class = if (is_async) "Async" else "";

        // This is annotated loosely, the operations calling it will contain their
        // own annotations so the interface is clear to Python as well.
        buffer.print(
            \\class {s}StateMachineMixin:
            \\    _submit: Callable[[Operation, Any, Any, Any], Any]
            \\
        , .{prefix_class});

        const operations: []const tb.Operation = &.{
            .create_accounts,
            .create_transfers,
            .lookup_accounts,
            .lookup_transfers,
            .get_account_transfers,
            .get_account_balances,
            .query_accounts,
            .query_transfers,
        };
        inline for (operations) |operation| {
            emit_method(&buffer, operation, .{ .is_async = is_async });
        }

        buffer.print("\n\n", .{});
    }

    try std.io.getStdOut().writeAll(buffer.inner.items);
}

/// Used by client code generation to make clearer APIs: the name of the Event parameter,
/// when used as a variable.
/// Inline function so that `operation` can be known at comptime.
fn event_name(comptime operation: tb.Operation) []const u8 {
    return switch (operation) {
        .create_accounts => "accounts",
        .create_transfers => "transfers",
        .lookup_accounts => "accounts",
        .lookup_transfers => "transfers",
        .get_account_transfers => "filter",
        .get_account_balances => "filter",
        .query_accounts => "query_filter",
        .query_transfers => "query_filter",
        else => comptime unreachable,
    };
}
