const std = @import("std");
const vsr = @import("vsr");
const assert = std.debug.assert;

const stdx = vsr.stdx;
const tb = vsr.tigerbeetle;

const type_mappings = .{
    .{ tb.AccountFlags, "AccountFlags" },
    .{ tb.TransferFlags, "TransferFlags" },
    .{ tb.AccountFilterFlags, "AccountFilterFlags" },
    .{ tb.QueryFilterFlags, "QueryFilterFlags" },
    .{ tb.Account, "Account" },
    .{ tb.Transfer, "Transfer" },
    .{ tb.CreateAccountStatus, "CreateAccountStatus", "Account" },
    .{ tb.CreateTransferStatus, "CreateTransferStatus", "Transfer" },
    .{ tb.CreateAccountResult, "CreateAccountResult" },
    .{ tb.CreateTransferResult, "CreateTransferResult" },
    .{ tb.AccountFilter, "AccountFilter" },
    .{ tb.AccountBalance, "AccountBalance" },
    .{ tb.QueryFilter, "QueryFilter" },
    .{ tb.ChangeEvent, "ChangeEvent" },
    .{ tb.ChangeEventType, "ChangeEventType", "ChangeEvent" },
    .{ tb.ChangeEventsFilter, "ChangeEventsFilter" },
};

fn go_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .bool => return "bool",
        .@"enum" => return comptime get_mapped_type_name(Type) orelse
            @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        .@"struct" => |info| switch (info.layout) {
            .@"packed" => return comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
            else => return comptime get_mapped_type_name(Type) orelse
                @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        },
        .int => |info| {
            assert(info.signedness == .unsigned);
            return switch (info.bits) {
                1 => "bool",
                8 => "uint8",
                16 => "uint16",
                32 => "uint32",
                64 => "uint64",
                128 => "Uint128",
                else => @compileError("invalid int type"),
            };
        },
        else => @compileError("Unhandled type: " ++ @typeName(Type)),
    }
}

fn get_mapped_type_name(comptime Type: type) ?[]const u8 {
    inline for (type_mappings) |type_mapping| {
        if (Type == type_mapping[0]) {
            return type_mapping[1];
        }
    } else return null;
}

fn to_pascal_case(comptime input: []const u8, comptime min_len: ?usize) []const u8 {
    return comptime blk: {
        var len: usize = 0;
        var output = [_]u8{' '} ** (min_len orelse input.len);
        var iterator = std.mem.tokenizeScalar(u8, input, '_');
        while (iterator.next()) |word| {
            if (is_upper_case(word)) {
                _ = std.ascii.upperString(output[len..], word);
            } else {
                @memcpy(output[len..][0..word.len], word); // Bypass tidy's ban, for go_bindings.
                output[len] = std.ascii.toUpper(output[len]);
            }
            len += word.len;
        }

        break :blk stdx.comptime_slice(&output, min_len orelse len);
    };
}

fn calculate_min_len(comptime type_info: anytype) comptime_int {
    comptime {
        var min_len: comptime_int = 0;
        for (type_info.fields) |field| {
            const field_len = to_pascal_case(field.name, null).len;
            if (field_len > min_len) {
                min_len = field_len;
            }
        }
        return min_len;
    }
}

fn is_upper_case(comptime word: []const u8) bool {
    // https://github.com/golang/go/wiki/CodeReviewComments#initialisms
    const initialisms = .{ "id", "ok" };
    inline for (initialisms) |initialism| {
        if (std.ascii.eqlIgnoreCase(initialism, word)) {
            return true;
        }
    } else return false;
}

fn emit_enum(
    buffer: *std.ArrayList(u8),
    comptime Type: type,
    comptime name: []const u8,
    comptime prefix: []const u8,
    comptime tag_type: []const u8,
) !void {
    try buffer.writer().print("type {s} {s}\n\n" ++
        "const (\n", .{
        name,
        tag_type,
    });

    const type_info = @typeInfo(Type).@"enum";
    const min_len = calculate_min_len(type_info);
    inline for (type_info.fields) |field| {
        if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
        const enum_name = prefix ++ comptime to_pascal_case(field.name, min_len);
        if (type_info.tag_type == u1) {
            try buffer.writer().print("\t{s} {s} = {s}\n", .{
                enum_name,
                name,
                if (@intFromEnum(@field(Type, field.name)) == 1) "true" else "false",
            });
        } else {
            try buffer.writer().print("\t{s} {s} = {d}\n", .{
                enum_name,
                name,
                @intFromEnum(@field(Type, field.name)),
            });
        }
    }

    try buffer.writer().print(")\n\n" ++
        "func (i {s}) String() string {{\n", .{
        name,
    });

    if (type_info.tag_type == u1) {
        const enum_zero_name = prefix ++ comptime to_pascal_case(
            @tagName(@as(Type, @enumFromInt(0))),
            null,
        );
        const enum_one_name = prefix ++ comptime to_pascal_case(
            @tagName(@as(Type, @enumFromInt(1))),
            null,
        );

        try buffer.writer().print("\tif (i == {s}) {{\n" ++
            "\t\treturn \"{s}\"\n" ++
            "\t}} else {{\n" ++
            "\t\treturn \"{s}\"\n" ++
            "\t}}\n", .{
            enum_one_name,
            enum_one_name,
            enum_zero_name,
        });
    } else {
        try buffer.writer().print("\tswitch i {{\n", .{});

        inline for (type_info.fields) |field| {
            if (comptime std.mem.startsWith(u8, field.name, "deprecated_")) continue;
            const enum_name = prefix ++ comptime to_pascal_case(field.name, null);
            try buffer.writer().print("\tcase {s}:\n" ++
                "\t\treturn \"{s}\"\n", .{
                enum_name,
                enum_name,
            });
        }

        try buffer.writer().print(
            "\t}}\n" ++
                "\treturn \"{s}(\" + strconv.FormatInt(int64(i+1), 10) + \")\"\n",
            .{name},
        );
    }

    try buffer.writer().print("}}\n\n", .{});
}

fn emit_packed_struct(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime name: []const u8,
    comptime int_type: []const u8,
) !void {
    try buffer.writer().print("type {s} struct {{\n", .{
        name,
    });

    const min_len = calculate_min_len(type_info);
    inline for (type_info.fields) |field| {
        if (comptime std.mem.eql(u8, "padding", field.name)) continue;
        try buffer.writer().print("\t{s} {s}\n", .{
            to_pascal_case(field.name, min_len),
            go_type(field.type),
        });
    }

    // Conversion from struct to packed (e.g. AccountFlags.ToUint16())
    try buffer.writer().print("}}\n\n" ++
        "func (f {s}) To{s}() {s} {{\n" ++
        "\tvar ret {s} = 0\n\n", .{
        name,
        to_pascal_case(int_type, null),
        int_type,
        int_type,
    });

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime std.mem.eql(u8, "padding", field.name)) continue;

        try buffer.writer().print("\tif f.{s} {{\n" ++
            "\t\tret |= (1 << {d})\n" ++
            "\t}}\n\n", .{
            to_pascal_case(field.name, null),
            i,
        });
    }

    try buffer.writer().print("\treturn ret\n" ++
        "}}\n\n", .{});
}

fn emit_struct(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime name: []const u8,
) !void {
    try buffer.writer().print("type {s} struct {{\n", .{
        name,
    });

    const min_len = calculate_min_len(type_info);
    comptime var flagsField = false;
    inline for (type_info.fields) |field| {
        switch (@typeInfo(field.type)) {
            .array => |array| {
                try buffer.writer().print("\t{s} [{d}]{s}\n", .{
                    to_pascal_case(field.name, min_len),
                    array.len,
                    go_type(array.child),
                });
            },
            else => {
                if (comptime std.mem.eql(u8, field.name, "flags")) {
                    flagsField = true;
                }

                try buffer.writer().print(
                    "\t{s} {s}\n",
                    .{
                        to_pascal_case(field.name, min_len),
                        go_type(field.type),
                    },
                );
            },
        }
    }

    try buffer.writer().print("}}\n\n", .{});

    if (flagsField) {
        const flagType = if (comptime std.mem.eql(u8, name, "Account"))
            tb.AccountFlags
        else if (comptime std.mem.eql(u8, name, "Transfer"))
            tb.TransferFlags
        else if (comptime std.mem.eql(u8, name, "AccountFilter"))
            tb.AccountFilterFlags
        else if (comptime std.mem.eql(u8, name, "QueryFilter"))
            tb.QueryFilterFlags
        else
            unreachable;
        // Conversion from packed to struct (e.g. Account.AccountFlags())
        try buffer.writer().print(
            "func (o {s}) {s}Flags() {s}Flags {{\n" ++
                "\tvar f {s}Flags\n",
            .{
                name,
                name,
                name,
                name,
            },
        );

        switch (@typeInfo(flagType)) {
            .@"struct" => |info| switch (info.layout) {
                .@"packed" => inline for (info.fields, 0..) |field, i| {
                    if (comptime std.mem.eql(u8, "padding", field.name)) continue;

                    try buffer.writer().print("\tf.{s} = ((o.Flags >> {}) & 0x1) == 1\n", .{
                        to_pascal_case(field.name, null),
                        i,
                    });
                },
                else => unreachable,
            },
            else => unreachable,
        }

        try buffer.writer().print("\treturn f\n" ++
            "}}\n\n", .{});
    }
}

pub fn generate_bindings(buffer: *std.ArrayList(u8)) !void {
    @setEvalBranchQuota(100_000);

    try buffer.writer().print(
        \\///////////////////////////////////////////////////////
        \\// This file was auto-generated by go_bindings.zig   //
        \\//              Do not manually modify.              //
        \\///////////////////////////////////////////////////////
        \\
        \\package types
        \\
        \\/*
        \\#include "../native/tb_client.h"
        \\*/
        \\import "C"
        \\import "strconv"
        \\
        \\
    , .{});

    // Emit Go declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const name = type_mapping[1];

        switch (@typeInfo(ZigType)) {
            .@"struct" => |info| switch (info.layout) {
                .auto => @compileError(
                    "Only packed or extern structs are supported: " ++ @typeName(ZigType),
                ),
                .@"packed" => try emit_packed_struct(
                    buffer,
                    info,
                    name,
                    comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
                ),
                .@"extern" => try emit_struct(buffer, info, name),
            },
            .@"enum" => try emit_enum(
                buffer,
                ZigType,
                name,
                type_mapping[2],
                comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
            ),
            else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
        }
    }
    assert(buffer.pop() == '\n');
    assert(std.mem.endsWith(u8, buffer.items, "\n"));
    assert(!std.mem.endsWith(u8, buffer.items, "\n\n"));
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = std.ArrayList(u8).init(allocator);
    try generate_bindings(&buffer);
    try std.io.getStdOut().writeAll(buffer.items);
}
