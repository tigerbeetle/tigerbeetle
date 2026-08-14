//! Prints the parsed conformance test model back as pseudo-DSL, for `zig build conformance:dump`.
const std = @import("std");

const types = @import("conformance_test_types.zig");

pub fn dump(tests: types.ConformanceTests) void {
    for (tests.suites) |suite| {
        std.debug.print("suite {s}\n", .{suite.name});
        for (suite.cases) |case| {
            std.debug.print("  \"{s}\"\n", .{case.description});
            if (case.requirement) |requirement| {
                std.debug.print("    {s}()\n", .{@tagName(requirement)});
            }
            for (case.steps) |step| {
                std.debug.print("    ", .{});
                dump_step(step);
                std.debug.print("\n", .{});
            }
        }
    }
}

fn dump_step(step: types.Step) void {
    switch (step) {
        .binding => |binding| {
            std.debug.print("const {s} = ", .{binding.name});
            dump_expression(binding.value);
        },
        .call => |call| dump_call(call),
        .assertion => |assertion| dump_assertion(assertion),
    }
}

fn dump_call(call: types.Call) void {
    if (call.concurrency > 1) std.debug.print("concurrently({d}, ", .{call.concurrency});
    std.debug.print("{s}(", .{@tagName(call.name)});
    for (call.arguments, 0..) |argument, index| {
        if (index > 0) std.debug.print(", ", .{});
        dump_expression(argument);
    }
    std.debug.print(")", .{});
    if (call.concurrency > 1) std.debug.print(")", .{});
}

fn dump_expression(expression: types.Expression) void {
    switch (expression) {
        .generate_id => std.debug.print("generate_id()", .{}),
        .generate_ids => |count| std.debug.print("generate_ids({d})", .{count}),
        .call => |call| dump_call(call),
        .record => |record| dump_record(record),
        .integer => |text| std.debug.print("{s}", .{text}),
        .boolean => |value| std.debug.print("{}", .{value}),
        .enum_literal => |name| std.debug.print(".{s}", .{name}),
        .reference => |name| std.debug.print("{s}", .{name}),
        .index => |index| std.debug.print("{s}[{d}]", .{ index.reference, index.index }),
    }
}

fn dump_record(record: types.Record) void {
    std.debug.print("{s}{{", .{@tagName(record.type)});
    for (record.fields, 0..) |field, index| {
        if (index > 0) std.debug.print(",", .{});
        std.debug.print(" .{s} = ", .{field.name});
        dump_expression(field.value);
    }
    std.debug.print(" }}", .{});
}

fn dump_assertion(assertion: types.Assertion) void {
    switch (assertion) {
        .equal => |equal| {
            std.debug.print("assert_equal({s}, .{{ ", .{equal.actual});
            for (equal.expected, 0..) |record, index| {
                if (index > 0) std.debug.print(", ", .{});
                dump_record(record);
            }
            std.debug.print(" }})", .{});
        },
        .empty => |actual| std.debug.print("assert_empty({s})", .{actual}),
        .unique => |ids| std.debug.print("assert_unique({s})", .{ids}),
        .ascending => |ids| std.debug.print("assert_ascending({s})", .{ids}),
        .equal_field => |equal_field| {
            std.debug.print("assert_equal({s}.{s}, ", .{
                equal_field.reference,
                equal_field.field.name,
            });
            dump_expression(equal_field.field.value);
            std.debug.print(")", .{});
        },
        .greater_than => |greater_than| {
            std.debug.print("assert_greater_than({s}.{s}, ", .{
                greater_than.reference,
                greater_than.field.name,
            });
            dump_expression(greater_than.field.value);
            std.debug.print(")", .{});
        },
        .fail => |call| {
            std.debug.print("assert_fail(", .{});
            dump_call(call);
            std.debug.print(")", .{});
        },
    }
}
