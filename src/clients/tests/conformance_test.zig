const std = @import("std");
const stdx = @import("stdx");

const Ast = std.zig.Ast;

const api = @import("conformance_test_api.zig");
pub const types = @import("conformance_test_types.zig");

// In the generated test files, suites appear in the order defined here. We are
// trying to structure this like a walkthrough for readers of the conformance
// test suite.  // The second tuple item is used in the generated test identifiers,
// e.g. test_get_account_balances_*.
pub const suites: []const struct { []const u8, []const u8 } = &.{
    .{ "ids_generate", "generate_ids" },
    .{ "accounts_create", "create_accounts" },
    .{ "accounts_lookup", "lookup_accounts" },
    .{ "transfers_create", "create_transfers" },
    .{ "transfers_lookup", "lookup_transfers" },
    .{ "account_transfers_get", "get_account_transfers" },
    .{ "account_balances_get", "get_account_balances" },
    .{ "accounts_query", "query_accounts" },
    .{ "transfers_query", "query_transfers" },
    .{ "transfers_two_phase", "two_phase_transfer" },
    .{ "uint128_range", "uint128_range" },
    .{ "transfers_create_concurrent", "create_transfers_concurrent" },
    .{ "client_close", "close_client" },
};

pub fn parse(arena: std.mem.Allocator) !types.ConformanceTests {
    var suites_parsed: std.ArrayList(types.Suite) = .empty;
    inline for (suites) |suite| {
        const file, const name = suite;
        const path = "suites/" ++ file ++ ".zig";
        var parser = try Parser.init(arena, path, @embedFile(path));
        try suites_parsed.append(arena, try parse_suite(&parser, name));
    }
    return .{ .suites = suites_parsed.items };
}

const Parser = struct {
    arena: std.mem.Allocator,
    path: []const u8,
    tree: Ast,
    bindings: std.StringArrayHashMapUnmanaged(types.Expression) = .empty,

    fn init(arena: std.mem.Allocator, path: []const u8, source: [:0]const u8) !Parser {
        const tree = try Ast.parse(arena, source, .zig);
        if (tree.errors.len > 0) {
            for (tree.errors) |parse_error| {
                const location = tree.tokenLocation(0, parse_error.token);
                std.debug.print(
                    "{s}:{d}:{d}: syntax error\n",
                    .{ path, location.line + 1, location.column + 1 },
                );
            }
            return error.ParseFailed;
        }
        return .{
            .arena = arena,
            .path = path,
            .tree = tree,
        };
    }

    fn fail(
        parser: *const Parser,
        token: ?Ast.TokenIndex,
        comptime format: []const u8,
        arguments: anytype,
    ) error{ParseFailed} {
        if (token) |token_index| {
            const location = parser.tree.tokenLocation(0, token_index);
            std.debug.print(
                "{s}:{d}:{d}: " ++ format ++ "\n",
                .{ parser.path, location.line + 1, location.column + 1 } ++ arguments,
            );
        } else {
            std.debug.print("{s}: " ++ format ++ "\n", .{parser.path} ++ arguments);
        }
        return error.ParseFailed;
    }

    fn fail_node(
        parser: *const Parser,
        node: Ast.Node.Index,
        comptime message: []const u8,
    ) error{ParseFailed} {
        return parser.fail(parser.tree.firstToken(node), message, .{});
    }

    fn fail_arguments(
        parser: *const Parser,
        call: Ast.full.Call,
        comptime expected: []const u8,
    ) error{ParseFailed} {
        return parser.fail(call.ast.lparen, "expected arguments " ++ expected, .{});
    }
};

fn parse_suite(parser: *Parser, name: []const u8) !types.Suite {
    const tree = &parser.tree;
    var cases: std.ArrayList(types.Case) = .empty;

    for (tree.rootDecls()) |decl| {
        if (tree.nodeTag(decl) != .test_decl) continue;
        try cases.append(parser.arena, try parse_case(parser, decl));
    }
    if (cases.items.len == 0) return parser.fail(null, "no cases", .{});
    return .{ .name = name, .cases = cases.items };
}

fn parse_case(parser: *Parser, node: Ast.Node.Index) !types.Case {
    const tree = &parser.tree;
    parser.bindings.clearRetainingCapacity();

    const name_token_optional, const block = tree.nodeData(node).opt_token_and_node;
    const name_token = name_token_optional.unwrap() orelse
        return parser.fail(tree.nodeMainToken(node), "test needs a description", .{});
    if (tree.tokenTag(name_token) != .string_literal) {
        return parser.fail(tree.nodeMainToken(node), "test needs a description", .{});
    }
    const literal = tree.tokenSlice(name_token);
    const description = literal[1 .. literal.len - 1];
    for (description) |char| {
        const allowed = std.ascii.isAlphanumeric(char) or switch (char) {
            ' ', '_', ',', '-', '\'', '.' => true,
            else => false,
        };
        if (!allowed) {
            return parser.fail(name_token, "invalid character in description", .{});
        }
    }

    var steps: std.ArrayList(types.Step) = .empty;
    var buffer: [2]Ast.Node.Index = undefined;
    var statements = tree.blockStatements(&buffer, block).?;
    // NOTE: For now we only support a single requirement and it needs to come first.
    const requirement: ?types.Case.Requirement = if (statements.len > 0)
        try parse_requirement(parser, statements[0])
    else
        null;
    if (requirement != null) statements = statements[1..];
    for (statements) |statement| {
        try steps.append(parser.arena, try parse_step(parser, statement));
    }
    if (steps.items.len == 0) return parser.fail(name_token, "empty case", .{});
    return .{
        .description = description,
        .steps = steps.items,
        .requirement = requirement,
    };
}

fn parse_requirement(parser: *Parser, node: Ast.Node.Index) !?types.Case.Requirement {
    const tree = &parser.tree;
    var buffer: [1]Ast.Node.Index = undefined;
    const call = tree.fullCall(&buffer, node) orelse return null;
    if (tree.nodeTag(call.ast.fn_expr) != .field_access) return null;
    const name = tree.tokenSlice(tree.nodeData(call.ast.fn_expr).node_and_token[1]);
    const requirement = std.meta.stringToEnum(types.Case.Requirement, name) orelse return null;
    try zero_arguments(parser, call);
    return requirement;
}

fn parse_step(parser: *Parser, node: Ast.Node.Index) !types.Step {
    const tree = &parser.tree;
    if (tree.fullVarDecl(node)) |var_decl| {
        return .{ .binding = try parse_binding(parser, var_decl) };
    }

    var buffer: [1]Ast.Node.Index = undefined;
    const call = tree.fullCall(&buffer, node) orelse
        return parser.fail_node(node, "invalid statement");
    const name = try parse_call_name(parser, call);
    if (std.meta.stringToEnum(types.Call.Name, name)) |call_name| {
        return .{ .call = try parse_call(parser, call_name, call) };
    }
    if (std.mem.eql(u8, name, "concurrently")) {
        return .{ .call = try parse_concurrently(parser, call) };
    }
    if (std.mem.startsWith(u8, name, "assert_")) {
        return .{ .assertion = try parse_assertion(parser, name, call) };
    }
    return parser.fail(tree.firstToken(node), "'{s}' cannot be a statement", .{name});
}

fn parse_binding(parser: *Parser, var_decl: Ast.full.VarDecl) !types.Binding {
    const tree = &parser.tree;
    // Skip over const/var
    const name_token = var_decl.ast.mut_token + 1;
    if (tree.tokenTag(var_decl.ast.mut_token) != .keyword_const) {
        return parser.fail(var_decl.ast.mut_token, "bindings must be `const`", .{});
    }
    // No type annotations allowed. Emitters pick the correct types, u128 has special handling.
    if (var_decl.ast.type_node.unwrap()) |type_node| {
        return parser.fail_node(type_node, "invalid type annotation");
    }

    const name = tree.tokenSlice(name_token);
    const value = try parse_expression(parser, var_decl.ast.init_node.unwrap().?);
    // NOTE: This may need to change, it mirrors current DSL use.
    switch (value) {
        .generate_id, .generate_ids, .index => {},
        // TODO: Revisit this limitation later.
        .record => |record| if (types.Record.is_flags(record.type)) {
            return parser.fail(name_token, "flags cannot be bound on their own", .{});
        },
        .call => |call| if (operation_result(call.name) == null) {
            return parser.fail(name_token, "'{s}' has no result", .{@tagName(call.name)});
        },
        .integer, .boolean, .enum_literal, .reference => {
            return parser.fail(
                tree.firstToken(var_decl.ast.init_node.unwrap().?),
                "invalid binding: {s}",
                .{@tagName(value)},
            );
        },
    }

    const entry = try parser.bindings.getOrPut(parser.arena, name);
    if (entry.found_existing) {
        return parser.fail(name_token, "duplicate binding '{s}'", .{name});
    }
    entry.value_ptr.* = value;
    return .{ .name = name, .value = value };
}

fn parse_expression(parser: *Parser, node: Ast.Node.Index) anyerror!types.Expression {
    const tree = &parser.tree;
    const main_token = tree.nodeMainToken(node);
    switch (tree.nodeTag(node)) {
        .number_literal => return .{ .integer = tree.tokenSlice(main_token) },
        .enum_literal => return .{ .enum_literal = tree.tokenSlice(main_token) },
        .identifier => {
            const name = tree.tokenSlice(main_token);
            if (std.mem.eql(u8, name, "true")) return .{ .boolean = true };
            if (std.mem.eql(u8, name, "false")) return .{ .boolean = false };
            if (!parser.bindings.contains(name)) {
                return parser.fail(main_token, "unknown binding '{s}'", .{name});
            }
            return .{ .reference = name };
        },
        .array_access => return try parse_index(parser, node),
        else => {},
    }

    var call_buffer: [1]Ast.Node.Index = undefined;
    if (tree.fullCall(&call_buffer, node)) |call| {
        const name = try parse_call_name(parser, call);
        if (std.mem.eql(u8, name, "generate_id")) {
            try zero_arguments(parser, call);
            return .generate_id;
        }
        if (std.mem.eql(u8, name, "generate_ids")) {
            const count_node = try one_argument(parser, call, "(count)");
            return .{ .generate_ids = try parse_u32(parser, count_node) };
        }
        if (std.meta.stringToEnum(types.Call.Name, name)) |call_name| {
            return .{ .call = try parse_call(parser, call_name, call) };
        }
        return parser.fail(tree.firstToken(node), "'{s}' cannot be a value", .{name});
    }

    var init_buffer: [2]Ast.Node.Index = undefined;
    if (tree.fullStructInit(&init_buffer, node)) |struct_init| {
        if (struct_init.ast.type_expr == .none) {
            return parser.fail_node(node, "record literal needs a type here");
        }
        const record_type = try parse_record_type(parser, struct_init.ast.type_expr.unwrap().?);
        return .{ .record = try parse_record(parser, record_type, struct_init) };
    }

    return parser.fail_node(node, "invalid expression");
}

// Extracts the trailing name from a `Namespace.name` field access.
fn parse_field_access_name(
    parser: *Parser,
    node: Ast.Node.Index,
    comptime message: []const u8,
) ![]const u8 {
    const tree = &parser.tree;
    if (tree.nodeTag(node) != .field_access) {
        return parser.fail_node(node, message);
    }
    const namespace, const name_token = tree.nodeData(node).node_and_token;
    if (tree.nodeTag(namespace) != .identifier) {
        return parser.fail_node(node, message);
    }
    return tree.tokenSlice(name_token);
}

fn parse_call_name(parser: *Parser, call: Ast.full.Call) ![]const u8 {
    return parse_field_access_name(parser, call.ast.fn_expr, "invalid call");
}

fn parse_record_type(parser: *Parser, node: Ast.Node.Index) !types.Record.Type {
    const name = try parse_field_access_name(parser, node, "invalid type");
    return std.meta.stringToEnum(types.Record.Type, name) orelse
        parser.fail(parser.tree.nodeData(node).node_and_token[1], "unknown type '{s}'", .{name});
}

fn parse_call(
    parser: *Parser,
    call_name: types.Call.Name,
    call: Ast.full.Call,
) anyerror!types.Call {
    switch (call_name) {
        .create_accounts => return parse_batch(parser, call_name, call, .Account),
        .create_transfers => return parse_batch(parser, call_name, call, .Transfer),
        .lookup_accounts, .lookup_transfers => return parse_lookup(parser, call_name, call),
        .get_account_transfers,
        .get_account_balances,
        .query_accounts,
        .query_transfers,
        => return parse_filtered(parser, call_name, call),
        .close_client => {
            try zero_arguments(parser, call);
            return .{ .name = call_name };
        },
        .sleep_ms => {
            const node = try one_argument(parser, call, "(ms)");
            const arguments = try parser.arena.alloc(types.Expression, 1);
            arguments[0] = try parse_expression(parser, node);
            return .{ .name = call_name, .arguments = arguments };
        },
    }
}

// An empty `.{}` parses as a struct initializer, not an array one.
fn parse_tuple(
    parser: *Parser,
    node: Ast.Node.Index,
    buffer: *[2]Ast.Node.Index,
    comptime message: []const u8,
) ![]const Ast.Node.Index {
    const tree = &parser.tree;
    if (tree.fullArrayInit(buffer, node)) |array_init| return array_init.ast.elements;
    if (tree.fullStructInit(buffer, node)) |struct_init| {
        if (struct_init.ast.type_expr == .none and struct_init.ast.fields.len == 0) return &.{};
    }
    return parser.fail_node(node, message);
}

fn parse_batch(
    parser: *Parser,
    call_name: types.Call.Name,
    call: Ast.full.Call,
    record_type: types.Record.Type,
) !types.Call {
    const tree = &parser.tree;
    const events_node = try one_argument(parser, call, "(events)");
    var buffer: [2]Ast.Node.Index = undefined;
    const elements = try parse_tuple(parser, events_node, &buffer, "expected a tuple of events");

    var arguments: std.ArrayList(types.Expression) = .empty;
    for (elements) |element| {
        var init_buffer: [2]Ast.Node.Index = undefined;
        if (tree.fullStructInit(&init_buffer, element)) |struct_init| {
            if (struct_init.ast.type_expr != .none) {
                return parser.fail_node(element, "event type is implied, use .{{...}}");
            }
            try arguments.append(parser.arena, .{
                .record = try parse_record(parser, record_type, struct_init),
            });
            continue;
        }
        const expression = try parse_expression(parser, element);
        if (expression != .reference) {
            return parser.fail_node(element, "expected a record or binding");
        }
        const bound = parser.bindings.get(expression.reference).?;
        if (bound != .record or bound.record.type != record_type) {
            return parser.fail(tree.firstToken(element), "'{s}' is not a {s}", .{
                expression.reference,
                @tagName(record_type),
            });
        }
        try arguments.append(parser.arena, expression);
    }
    return .{ .name = call_name, .arguments = arguments.items };
}

fn parse_lookup(parser: *Parser, call_name: types.Call.Name, call: Ast.full.Call) !types.Call {
    const ids_node = try one_argument(parser, call, "(ids)");
    var buffer: [2]Ast.Node.Index = undefined;
    const elements = try parse_tuple(parser, ids_node, &buffer, "expected a tuple of ids");

    var arguments: std.ArrayList(types.Expression) = .empty;
    for (elements) |element| {
        const expression = try parse_expression(parser, element);
        switch (expression) {
            .reference, .integer, .generate_id => try arguments.append(parser.arena, expression),
            else => return parser.fail_node(element, "expected an id"),
        }
    }
    return .{ .name = call_name, .arguments = arguments.items };
}

fn parse_filtered(parser: *Parser, call_name: types.Call.Name, call: Ast.full.Call) !types.Call {
    const tree = &parser.tree;
    const record_type: types.Record.Type = switch (call_name) {
        inline else => |comptime_call_name| (comptime filter_type(comptime_call_name)) orelse
            unreachable,
    };
    const filter_node = try one_argument(parser, call, "(filter)");
    var buffer: [2]Ast.Node.Index = undefined;
    const struct_init = tree.fullStructInit(&buffer, filter_node) orelse
        return parser.fail_node(filter_node, "expected a filter literal");
    if (struct_init.ast.type_expr != .none) {
        return parser.fail_node(filter_node, "filter type is implied, use .{{...}}");
    }
    const arguments = try parser.arena.alloc(types.Expression, 1);
    arguments[0] = .{ .record = try parse_record(parser, record_type, struct_init) };
    return .{ .name = call_name, .arguments = arguments };
}

fn parse_concurrently(parser: *Parser, call: Ast.full.Call) !types.Call {
    const tree = &parser.tree;
    const args = try two_arguments(parser, call, "(count, call)");
    const count = try parse_u32(parser, args[0]);
    var buffer: [1]Ast.Node.Index = undefined;
    const inner = tree.fullCall(&buffer, args[1]) orelse
        return parser.fail_node(args[1], "expected a call");
    const name = try parse_call_name(parser, inner);
    const call_name = std.meta.stringToEnum(types.Call.Name, name) orelse
        return parser.fail_node(inner.ast.fn_expr, "unknown call");
    var parsed = try parse_call(parser, call_name, inner);
    parsed.concurrency = count;
    return parsed;
}

const AssertionCall = stdx.EnumType(types.api_decl_names(types.is_assertion));

fn parse_assertion(parser: *Parser, name: []const u8, call: Ast.full.Call) !types.Assertion {
    const tree = &parser.tree;
    const assertion = std.meta.stringToEnum(AssertionCall, name) orelse
        return parser.fail(tree.firstToken(call.ast.fn_expr), "unknown assertion '{s}'", .{name});
    switch (assertion) {
        .assert_equal => {
            const args = try two_arguments(parser, call, "(actual, expected)");
            if (tree.nodeTag(args[0]) == .field_access) {
                const value = try parse_expression(parser, args[1]);
                const comparison =
                    try parse_field_comparison(parser, args[0], args[1], value);
                return .{ .equal_field = comparison };
            }

            const actual_token = tree.firstToken(args[0]);
            const actual = try parse_expression(parser, args[0]);
            const record_type = try result_record_type(parser, actual, actual_token);

            var buffer: [2]Ast.Node.Index = undefined;
            const array_init = tree.fullArrayInit(&buffer, args[1]) orelse
                return parser.fail_node(args[1], "expected a tuple of records");
            var expected: std.ArrayList(types.Record) = .empty;
            for (array_init.ast.elements) |element| {
                var init_buffer: [2]Ast.Node.Index = undefined;
                const struct_init = tree.fullStructInit(&init_buffer, element) orelse
                    return parser.fail_node(element, "expected a record");
                if (struct_init.ast.type_expr != .none) {
                    return parser.fail_node(element, "record type is implied, use .{{...}}");
                }
                try expected.append(parser.arena, try parse_record(parser, record_type, struct_init));
            }
            if (expected.items.len == 0) {
                return parser.fail(call.ast.lparen, "use assert_empty", .{});
            }
            return .{ .equal = .{ .actual = actual.reference, .expected = expected.items } };
        },
        .assert_empty => {
            const node = try one_argument(parser, call, "(actual)");
            return .{ .empty = try parse_reference(parser, node) };
        },
        .assert_unique => {
            const node = try one_argument(parser, call, "(ids)");
            return .{ .unique = try parse_reference(parser, node) };
        },
        .assert_ascending => {
            const node = try one_argument(parser, call, "(ids)");
            return .{ .ascending = try parse_reference(parser, node) };
        },
        .assert_greater_than => {
            const args = try two_arguments(parser, call, "(actual, value)");
            if (tree.nodeTag(args[1]) != .number_literal) {
                return parser.fail_node(args[1], "expected an integer");
            }
            const integer = tree.tokenSlice(tree.nodeMainToken(args[1]));
            const comparison = try parse_field_comparison(parser, args[0], args[1], .{
                .integer = integer,
            });
            return .{ .greater_than = comparison };
        },
        .assert_fail => {
            const inner_node = try one_argument(parser, call, "(call)");
            var buffer: [1]Ast.Node.Index = undefined;
            const inner = tree.fullCall(&buffer, inner_node) orelse
                return parser.fail_node(inner_node, "expected a call");
            const inner_name = try parse_call_name(parser, inner);
            const operation = std.meta.stringToEnum(types.Call.Name, inner_name) orelse
                return parser.fail_node(inner.ast.fn_expr, "expected an operation");
            return .{ .fail = try parse_call(parser, operation, inner) };
        },
    }
}

fn parse_record(
    parser: *Parser,
    record_type: types.Record.Type,
    struct_init: Ast.full.StructInit,
) anyerror!types.Record {
    const tree = &parser.tree;
    var fields: std.ArrayList(types.Field) = .empty;
    var seen = std.bit_set.IntegerBitSet(64).initEmpty();
    for (struct_init.ast.fields) |field_init| {
        const name_token = tree.firstToken(field_init) - 2;
        const field_name = tree.tokenSlice(name_token);
        const field_index = record_field_index(record_type, field_name) orelse
            return parser.fail(name_token, "no field '{s}' on {s}", .{
                field_name,
                @tagName(record_type),
            });
        if (seen.isSet(field_index)) {
            return parser.fail(name_token, "duplicate field '{s}'", .{field_name});
        }
        seen.set(field_index);

        var buffer: [2]Ast.Node.Index = undefined;
        if (tree.fullStructInit(&buffer, field_init)) |nested| {
            if (nested.ast.type_expr == .none) {
                const nested_type = record_field_record_type(record_type, field_name) orelse
                    return parser.fail(name_token, "field '{s}' takes no record", .{field_name});
                try fields.append(parser.arena, .{
                    .name = field_name,
                    .value = .{ .record = try parse_record(parser, nested_type, nested) },
                    .type = types.Field.Type.resolve(record_type, field_name),
                });
                continue;
            }
        }

        const value = try parse_expression(parser, field_init);
        if (types.Record.is_flags(record_type) and (value != .boolean or !value.boolean)) {
            return parser.fail(name_token, "flag '{s}' must be true (omit to leave false)", .{
                field_name,
            });
        }
        if (value == .enum_literal and
            !record_field_enum_has(record_type, field_name, value.enum_literal))
        {
            return parser.fail(name_token, "field '{s}' has no member '.{s}'", .{
                field_name,
                value.enum_literal,
            });
        }
        try fields.append(parser.arena, .{
            .name = field_name,
            .value = value,
            .type = types.Field.Type.resolve(record_type, field_name),
        });
    }
    if (fields.items.len == 0) {
        return parser.fail(struct_init.ast.lbrace, "empty record", .{});
    }
    return .{ .type = record_type, .fields = fields.items };
}

fn result_record_type(
    parser: *Parser,
    actual: types.Expression,
    token: Ast.TokenIndex,
) !types.Record.Type {
    if (actual == .reference) {
        const value = parser.bindings.get(actual.reference).?;
        if (value == .call) {
            if (operation_result(value.call.name)) |record_type| return record_type;
        }
    }
    return parser.fail(token, "expected a value, not an operation", .{});
}

fn parse_reference(parser: *Parser, node: Ast.Node.Index) ![]const u8 {
    const expression = try parse_expression(parser, node);
    if (expression != .reference) return parser.fail_node(node, "expected a binding");
    return expression.reference;
}

// Resolves an identifier node to its binding, failing if the name is unbound.
fn resolve_reference(
    parser: *Parser,
    node: Ast.Node.Index,
) !struct { reference: []const u8, value: types.Expression } {
    const tree = &parser.tree;
    if (tree.nodeTag(node) != .identifier) {
        return parser.fail_node(node, "expected a binding");
    }
    const reference = tree.tokenSlice(tree.nodeMainToken(node));
    const value = parser.bindings.get(reference) orelse
        return parser.fail(tree.firstToken(node), "unknown binding '{s}'", .{reference});
    return .{ .reference = reference, .value = value };
}

fn parse_index(parser: *Parser, node: Ast.Node.Index) !types.Expression {
    const tree = &parser.tree;
    const base_node, const index_node = tree.nodeData(node).node_and_node;
    const resolved = try resolve_reference(parser, base_node);
    const record_type = switch (resolved.value) {
        .call => |call| operation_result(call.name),
        else => null,
    } orelse return parser.fail_node(base_node, "expected an operation result");

    const index = try parse_u32(parser, index_node);
    return .{ .index = .{
        .reference = resolved.reference,
        .index = index,
        .record_type = record_type,
    } };
}

// Only legal on a binding already narrowed to one record by indexing (`const foo = foos[0];`),
// never a slice-bound reference directly. This matches how asserts elsewhere only take bindings.
fn parse_field_comparison(
    parser: *Parser,
    node: Ast.Node.Index,
    value_node: Ast.Node.Index,
    value: types.Expression,
) !types.Assertion.FieldComparison {
    const tree = &parser.tree;
    if (tree.nodeTag(node) != .field_access) {
        return parser.fail_node(node, "expected a field access");
    }
    const base_node, const field_token = tree.nodeData(node).node_and_token;
    const field_name = tree.tokenSlice(field_token);

    const resolved = try resolve_reference(parser, base_node);
    if (resolved.value != .index) {
        return parser.fail_node(base_node, "expected an indexed binding");
    }

    const record_type = resolved.value.index.record_type;
    if (record_field_index(record_type, field_name) == null) {
        return parser.fail(field_token, "no field '{s}' on {s}", .{
            field_name,
            @tagName(record_type),
        });
    }
    if (value == .enum_literal and
        !record_field_enum_has(record_type, field_name, value.enum_literal))
    {
        return parser.fail(tree.firstToken(value_node), "field '{s}' has no member '.{s}'", .{
            field_name,
            value.enum_literal,
        });
    }
    return .{
        .reference = resolved.reference,
        .field = .{
            .name = field_name,
            .value = value,
            .type = types.Field.Type.resolve(record_type, field_name),
        },
    };
}

fn check_arguments(
    parser: *Parser,
    call: Ast.full.Call,
    count: usize,
    comptime expected: []const u8,
) !void {
    if (call.ast.params.len != count) {
        return parser.fail_arguments(call, expected);
    }
}

fn zero_arguments(parser: *Parser, call: Ast.full.Call) !void {
    return check_arguments(parser, call, 0, "()");
}

fn one_argument(
    parser: *Parser,
    call: Ast.full.Call,
    comptime expected: []const u8,
) !Ast.Node.Index {
    try check_arguments(parser, call, 1, expected);
    return call.ast.params[0];
}

fn two_arguments(
    parser: *Parser,
    call: Ast.full.Call,
    comptime expected: []const u8,
) ![2]Ast.Node.Index {
    try check_arguments(parser, call, 2, expected);
    return call.ast.params[0..2].*;
}

fn parse_u32(parser: *Parser, node: Ast.Node.Index) !u32 {
    const tree = &parser.tree;
    if (tree.nodeTag(node) != .number_literal) {
        return parser.fail_node(node, "expected an integer");
    }
    const token = tree.nodeMainToken(node);
    return stdx.parse_int(u32, tree.tokenSlice(token), .{}) catch
        parser.fail(token, "invalid integer", .{});
}

fn api_record_type(comptime T: type) ?types.Record.Type {
    inline for (std.enums.values(types.Record.Type)) |record_type| {
        if (types.Record.Struct(record_type) == T) return record_type;
    }
    return null;
}

fn filter_type(comptime operation: types.Call.Name) ?types.Record.Type {
    const params = @typeInfo(@TypeOf(@field(api, @tagName(operation)))).@"fn".params;
    if (params.len != 1) return null;
    const Param = params[0].type orelse return null;
    return api_record_type(Param);
}

fn operation_result(operation: types.Call.Name) ?types.Record.Type {
    switch (operation) {
        inline else => |comptime_operation| return comptime result: {
            const function = @typeInfo(
                @TypeOf(@field(api, @tagName(comptime_operation))),
            ).@"fn";
            if (function.return_type.? == void) break :result null;
            break :result api_record_type(@typeInfo(function.return_type.?).pointer.child);
        },
    }
}

fn field_is_reserved(name: []const u8) bool {
    return std.mem.eql(u8, name, "padding") or std.mem.eql(u8, name, "reserved");
}

fn record_field_index(record_type: types.Record.Type, name: []const u8) ?usize {
    if (field_is_reserved(name)) return null;
    switch (record_type) {
        inline else => |comptime_type| {
            inline for (std.meta.fields(types.Record.Struct(comptime_type)), 0..) |field, index| {
                if (std.mem.eql(u8, field.name, name)) return index;
            }
            return null;
        },
    }
}

fn record_field_record_type(
    record_type: types.Record.Type,
    name: []const u8,
) ?types.Record.Type {
    switch (record_type) {
        inline else => |comptime_type| {
            inline for (std.meta.fields(types.Record.Struct(comptime_type))) |field| {
                if (std.mem.eql(u8, field.name, name)) {
                    if (@typeInfo(field.type) == .@"struct") {
                        return comptime api_record_type(field.type);
                    }
                    return null;
                }
            }
            return null;
        },
    }
}

fn record_field_enum_has(
    record_type: types.Record.Type,
    field_name: []const u8,
    member: []const u8,
) bool {
    switch (record_type) {
        inline else => |comptime_type| {
            inline for (std.meta.fields(types.Record.Struct(comptime_type))) |field| {
                if (std.mem.eql(u8, field.name, field_name)) {
                    switch (@typeInfo(field.type)) {
                        .@"enum" => |enum_info| {
                            inline for (enum_info.fields) |enum_field| {
                                if (std.mem.eql(u8, enum_field.name, member)) return true;
                            }
                            return false;
                        },
                        else => return false,
                    }
                }
            }
            return false;
        },
    }
}

fn dump(tests: types.ConformanceTests) void {
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

pub fn main(init: std.process.Init) !void {
    var arena_instance = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_instance.deinit();
    const arena = arena_instance.allocator();

    const tests = try parse(arena);

    const args = try init.minimal.args.toSlice(arena);
    if (args.len > 1) {
        if (args.len > 2 or !std.mem.eql(u8, args[1], "--debug")) {
            std.debug.print("usage: conformance_test [--debug]\n", .{});
            return error.InvalidArguments;
        }
        dump(tests);
    }
}
