const std = @import("std");
const tb = @import("../../tigerbeetle.zig");
const tb_client = @import("../c/tb_client.zig");

const type_mappings = .{
    .{ tb.AccountFlags, "AccountFlags" },
    .{ tb.TransferFlags, "TransferFlags" },
    .{ tb.Account, "Account" },
    .{ tb.Transfer, "Transfer" },
    .{ tb.CreateAccountResult, "CreateAccountResult", "Account" },
    .{ tb.CreateTransferResult, "CreateTransferResult", "Transfer" },
    .{ tb.CreateAccountsResult, "AccountEventResult" },
    .{ tb.CreateTransfersResult, "TransferEventResult" },
};

fn go_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Enum => return comptime get_mapped_type_name(Type) orelse @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        .Struct => |info| switch (info.layout) {
            .Packed => return comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
            else => return comptime get_mapped_type_name(Type) orelse @compileError("Type " ++ @typeName(Type) ++ " not mapped."),
        },
        .Int => |info| {
            std.debug.assert(info.signedness == .unsigned);
            return switch (info.bits) {
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
    comptime {
        var len: usize = 0;
        var output = [_]u8{' '} ** (min_len orelse input.len);
        var iterator = std.mem.tokenize(u8, input, "_");
        while (iterator.next()) |word| {
            if (is_upper_case(word)) {
                _ = std.ascii.upperString(output[len..], word);
            } else {
                std.mem.copy(u8, output[len..], word);
                output[len] = std.ascii.toUpper(output[len]);
            }
            len += word.len;
        }

        return output[0 .. min_len orelse len];
    }
}

fn calculate_min_len(comptime type_info: anytype) comptime_int {
    comptime {
        comptime var min_len: comptime_int = 0;
        inline for (type_info.fields) |field| {
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
    comptime type_info: anytype,
    comptime name: []const u8,
    comptime prefix: []const u8,
    comptime int_type: []const u8,
) !void {
    try buffer.writer().print("type {s} {s}\n\n" ++
        "const (\n", .{
        name,
        int_type,
    });

    const min_len = calculate_min_len(type_info);
    inline for (type_info.fields) |field, i| {
        const enum_name = prefix ++ to_pascal_case(field.name, min_len);
        try buffer.writer().print("\t{s} {s} = {d}\n", .{
            enum_name,
            name,
            i,
        });
    }

    try buffer.writer().print(")\n\n" ++
        "func (i {s}) String() string {{\n" ++
        "\tswitch i {{\n", .{
        name,
    });

    inline for (type_info.fields) |field| {
        const enum_name = prefix ++ to_pascal_case(field.name, null);
        try buffer.writer().print("\tcase {s}:\n" ++
            "\t\treturn \"{s}\"\n", .{
            enum_name,
            enum_name,
        });
    }

    try buffer.writer().print("\t}}\n" ++
        "\treturn \"{s}(\" + strconv.FormatInt(int64(i+1), 10) + \")\"\n" ++
        "}}\n\n", .{
        name,
    });
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
        try buffer.writer().print("\t{s} bool\n", .{
            to_pascal_case(field.name, min_len),
        });
    }

    try buffer.writer().print("}}\n\n" ++
        "func (f {s}) To{s}() {s} {{\n" ++
        "\tvar ret {s} = 0\n\n", .{
        name,
        to_pascal_case(int_type, null),
        int_type,
        int_type,
    });

    inline for (type_info.fields) |field, i| {
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
    inline for (type_info.fields) |field| {
        switch (@typeInfo(field.field_type)) {
            .Array => |array| {
                try buffer.writer().print("\t{s} [{d}]{s}\n", .{
                    to_pascal_case(field.name, min_len),
                    array.len,
                    go_type(array.child),
                });
            },
            else => try buffer.writer().print(
                "\t{s} {s}\n",
                .{
                    to_pascal_case(field.name, min_len),
                    go_type(field.field_type),
                },
            ),
        }
    }

    try buffer.writer().print("}}\n\n", .{});
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
            .Struct => |info| switch (info.layout) {
                .Auto => @compileError("Only packed or extern structs are supported: " ++ @typeName(ZigType)),
                .Packed => try emit_packed_struct(buffer, info, name, comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType)))),
                .Extern => try emit_struct(buffer, info, name),
            },
            .Enum => |info| try emit_enum(buffer, info, name, type_mapping[2], comptime go_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType)))),
            else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
        }
    }
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var buffer = std.ArrayList(u8).init(allocator);
    try generate_bindings(&buffer);
    try std.fs.cwd().writeFile("src/clients/go/pkg/types/bindings.go", buffer.items);
}
