//! Prints the parsed conformance test model back as pseudo-DSL, for `zig build conformance:dump`.
//! This file may eventually go away, it's primarily useful while working on the conformance parser.
const std = @import("std");

const ast = @import("conformance_test_ast.zig");

var stdout_buffer: [4096]u8 = undefined;
var stdout: std.Io.File.Writer = undefined;

fn print(comptime format: []const u8, arguments: anytype) void {
    stdout.interface.print(format, arguments) catch @panic("failed to write to stdout");
}

pub fn dump(io: std.Io, tests: ast.ConformanceTests) void {
    stdout = std.Io.File.stdout().writerStreaming(io, &stdout_buffer);
    for (tests.suites) |suite| {
        print("suite {s}\n", .{suite.name});
        for (suite.cases) |case| {
            print("  \"{s}\"\n", .{case.description});
            if (case.requirement) |requirement| {
                print("    {s}()\n", .{@tagName(requirement)});
            }
            for (case.steps) |step| {
                print("    ", .{});
                dump_step(step);
                print("\n", .{});
            }
        }
    }
    stdout.interface.flush() catch @panic("failed to write to stdout");
}

fn dump_step(step: ast.Step) void {
    switch (step) {
        .binding => |binding| {
            print("const {s} = ", .{binding.name});
            dump_expression(binding.value);
        },
        .call => |call| dump_call(call),
        .assertion => |assertion| dump_assertion(assertion),
    }
}

fn dump_call(call: ast.Call) void {
    if (call.concurrency > 1) print("concurrently({d}, ", .{call.concurrency});
    print("{s}(", .{@tagName(call.name)});
    for (call.arguments, 0..) |argument, index| {
        if (index > 0) print(", ", .{});
        dump_expression(argument);
    }
    print(")", .{});
    if (call.concurrency > 1) print(")", .{});
}

fn dump_expression(expression: ast.Expression) void {
    switch (expression) {
        .generate_id => print("generate_id()", .{}),
        .generate_ids => |count| print("generate_ids({d})", .{count}),
        .call => |call| dump_call(call),
        .record => |record| dump_record(record),
        .integer => |text| print("{s}", .{text}),
        .boolean => |value| print("{}", .{value}),
        .enum_literal => |name| print(".{s}", .{name}),
        .reference => |name| print("{s}", .{name}),
        .index => |index| print("{s}[{d}]", .{ index.reference, index.index }),
    }
}

fn dump_record(record: ast.Record) void {
    print("{s}{{", .{@tagName(record.type)});
    for (record.fields, 0..) |field, index| {
        if (index > 0) print(",", .{});
        print(" .{s} = ", .{field.name});
        dump_expression(field.value);
    }
    print(" }}", .{});
}

fn dump_assertion(assertion: ast.Assertion) void {
    switch (assertion) {
        .equal => |equal| {
            print("assert_equal({s}, .{{ ", .{equal.actual});
            for (equal.expected, 0..) |record, index| {
                if (index > 0) print(", ", .{});
                dump_record(record);
            }
            print(" }})", .{});
        },
        .empty => |actual| print("assert_empty({s})", .{actual}),
        .unique => |ids| print("assert_unique({s})", .{ids}),
        .ascending => |ids| print("assert_ascending({s})", .{ids}),
        .equal_field => |equal_field| {
            print("assert_equal({s}.{s}, ", .{
                equal_field.reference,
                equal_field.field.name,
            });
            dump_expression(equal_field.field.value);
            print(")", .{});
        },
        .greater_than => |greater_than| {
            print("assert_greater_than({s}.{s}, ", .{
                greater_than.reference,
                greater_than.field.name,
            });
            dump_expression(greater_than.field.value);
            print(")", .{});
        },
        .fail => |call| {
            print("assert_fail(", .{});
            dump_call(call);
            print(")", .{});
        },
    }
}
