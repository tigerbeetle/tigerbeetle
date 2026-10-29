const std = @import("std");
const stdx = @import("stdx");

const api = @import("api.zig");
const ast = @import("ast.zig");

const log = std.log;

pub const Source = struct {
    path: []const u8,
    name: []const u8,
    text: [:0]const u8,
};

const ParseError = error{ ParseFailed, OutOfMemory };

pub fn parse(arena: std.mem.Allocator, source: Source) !ast.Suite {
    var parser = try Parser.init(arena, source.path, source.text);
    return parser.parse_suite(source.name);
}

const Parser = struct {
    arena: std.mem.Allocator,
    path: []const u8,
    tree: std.zig.Ast,
    bindings: std.StringArrayHashMapUnmanaged(Bound) = .empty,
    client: ?[]const u8 = null,

    const Bound = union(enum) {
        client,
        data: ast.Expression,
    };

    fn init(arena: std.mem.Allocator, path: []const u8, source: [:0]const u8) !Parser {
        const tree = try std.zig.Ast.parse(arena, source, .zig);
        if (tree.errors.len > 0) {
            for (tree.errors) |parse_error| {
                const location = tree.tokenLocation(0, parse_error.token);
                log.err(
                    "{s}:{d}:{d}: syntax error",
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
        token: ?std.zig.Ast.TokenIndex,
        comptime format: []const u8,
        arguments: anytype,
    ) error{ParseFailed} {
        if (token) |token_index| {
            const location = parser.tree.tokenLocation(0, token_index);
            log.err(
                "{s}:{d}:{d}: " ++ format,
                .{ parser.path, location.line + 1, location.column + 1 } ++ arguments,
            );
        } else {
            log.err("{s}: " ++ format, .{parser.path} ++ arguments);
        }
        return error.ParseFailed;
    }

    fn fail_node(
        parser: *const Parser,
        node: std.zig.Ast.Node.Index,
        comptime message: []const u8,
    ) error{ParseFailed} {
        return parser.fail(parser.tree.firstToken(node), message, .{});
    }

    fn fail_arguments(
        parser: *const Parser,
        call: std.zig.Ast.full.Call,
        comptime expected: []const u8,
    ) error{ParseFailed} {
        return parser.fail(call.ast.lparen, "expected arguments " ++ expected, .{});
    }

    fn parse_suite(parser: *Parser, suite_name: []const u8) !ast.Suite {
        const tree = &parser.tree;
        var cases: std.ArrayList(ast.Case) = .empty;

        for (tree.rootDecls()) |decl| {
            if (tree.fullVarDecl(decl)) |var_decl| {
                if (parser.client_alias(var_decl)) |name| parser.client = name;
                continue;
            }
            if (tree.nodeTag(decl) != .test_decl) continue;
            try cases.append(parser.arena, try parser.parse_case(decl));
        }
        if (cases.items.len == 0) return parser.fail(null, "no cases", .{});
        return .{ .name = suite_name, .cases = cases.items };
    }

    fn client_alias(parser: *const Parser, var_decl: std.zig.Ast.full.VarDecl) ?[]const u8 {
        const init_node = var_decl.ast.init_node.unwrap() orelse return null;
        const name = parser.parse_api_name(init_node) orelse return null;
        if (!std.mem.eql(u8, name, "default_client")) return null;
        return parser.tree.tokenSlice(var_decl.ast.mut_token + 1);
    }

    fn parse_case(parser: *Parser, node: std.zig.Ast.Node.Index) !ast.Case {
        const tree = &parser.tree;
        parser.bindings.clearRetainingCapacity();
        if (parser.client) |client| try parser.bindings.put(parser.arena, client, .client);

        const name_token_optional, const block = tree.nodeData(node).opt_token_and_node;
        const name_token = name_token_optional.unwrap() orelse
            return parser.fail(tree.nodeMainToken(node), "test needs a description", .{});
        if (tree.tokenTag(name_token) != .string_literal) {
            return parser.fail(tree.nodeMainToken(node), "test needs a description", .{});
        }
        const literal = tree.tokenSlice(name_token);
        const description = literal[1 .. literal.len - 1];
        var alphanumeric = false;
        for (description) |char| {
            const allowed = std.ascii.isAlphanumeric(char) or switch (char) {
                ' ', '_', ',', '-', '\'', '.' => true,
                else => false,
            };
            if (!allowed) {
                return parser.fail(name_token, "invalid character in description", .{});
            }
            alphanumeric = alphanumeric or std.ascii.isAlphanumeric(char);
        }
        if (!alphanumeric) {
            return parser.fail(name_token, "generated description cannot be empty", .{});
        }

        var buffer: [2]std.zig.Ast.Node.Index = undefined;
        var statements = tree.blockStatements(&buffer, block).?;
        // NOTE: We only support a single requirement and it needs to come first.
        const requirement: ?ast.Case.Requirement = if (statements.len > 0)
            try parser.parse_requirement(statements[0])
        else
            null;
        if (requirement != null) statements = statements[1..];
        const steps = try parser.arena.alloc(ast.Step, statements.len);
        for (statements, 0..) |statement, index| {
            steps[index] = try parser.parse_step(statement);
        }
        if (steps.len == 0) return parser.fail(name_token, "empty case", .{});
        return .{
            .description = description,
            .steps = steps,
            .requirement = requirement,
        };
    }

    fn parse_requirement(
        parser: *Parser,
        node: std.zig.Ast.Node.Index,
    ) !?ast.Case.Requirement {
        const tree = &parser.tree;
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const call = tree.fullCall(&buffer, node) orelse return null;
        const name = parser.parse_api_name(call.ast.fn_expr) orelse return null;
        const requirement = std.meta.stringToEnum(ast.Case.Requirement, name) orelse return null;
        try parser.assert_zero_arguments(call);
        return requirement;
    }

    fn parse_step(parser: *Parser, node: std.zig.Ast.Node.Index) !ast.Step {
        const tree = &parser.tree;
        if (tree.fullVarDecl(node)) |var_decl| {
            return .{ .binding = try parser.parse_binding(var_decl) };
        }

        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const call = tree.fullCall(&buffer, node) orelse
            return parser.fail_node(node, "invalid statement");
        const name = try parser.parse_call_name(call);
        if (std.meta.stringToEnum(ast.Call.Name, name)) |call_name| {
            return .{ .call = try parser.parse_call(call_name, call) };
        }
        if (std.mem.eql(u8, name, "concurrently")) {
            return .{ .directive = try parser.parse_concurrently(call) };
        }
        if (std.mem.eql(u8, name, "create_client")) {
            return .{ .create_client = try parser.parse_client_construction(call) };
        }
        if (std.mem.eql(u8, name, "sleep_ms")) {
            const ms_node = try parser.assert_one_argument(call, "(ms)");
            return .{ .directive = .{ .sleep_ms = try parser.parse_integer(u32, ms_node) } };
        }
        if (std.mem.startsWith(u8, name, "assert_")) {
            return .{ .assertion = try parser.parse_assertion(name, call) };
        }
        return parser.fail(tree.firstToken(node), "'{s}' cannot be a statement", .{name});
    }

    fn parse_binding(parser: *Parser, var_decl: std.zig.Ast.full.VarDecl) !ast.Binding {
        const tree = &parser.tree;
        // Skip over const/var
        const name_token = var_decl.ast.mut_token + 1;
        if (tree.tokenTag(var_decl.ast.mut_token) != .keyword_const) {
            return parser.fail(var_decl.ast.mut_token, "bindings must be `const`", .{});
        }
        // No type annotations allowed. Emitters pick the correct types, u128
        // has special handling.
        if (var_decl.ast.type_node.unwrap()) |type_node| {
            return parser.fail_node(type_node, "invalid type annotation");
        }

        const name = tree.tokenSlice(name_token);
        const value = try parser.parse_expression(var_decl.ast.init_node.unwrap().?);
        switch (value) {
            .generate_id, .generate_ids, .index, .field_access => {},
            .call => |call| if (operation_result(call.name) == null) {
                return parser.fail(name_token, "'{s}' has no result", .{@tagName(call.name)});
            },
            .integer, .boolean, .enum_literal, .record, .reference, .increment, .decrement => {
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
        entry.value_ptr.* = .{ .data = value };
        return .{ .name = name, .value = value };
    }

    fn parse_expression(parser: *Parser, node: std.zig.Ast.Node.Index) ParseError!ast.Expression {
        const tree = &parser.tree;
        const main_token = tree.nodeMainToken(node);
        switch (tree.nodeTag(node)) {
            .number_literal => return .{ .integer = tree.tokenSlice(main_token) },
            .negation => {
                const operand = tree.nodeData(node).node;
                if (tree.nodeTag(operand) != .number_literal) {
                    return parser.fail_node(operand, "expected an integer");
                }
                const text = tree.tokenSlice(tree.nodeMainToken(operand));
                return .{ .integer = try std.fmt.allocPrint(parser.arena, "-{s}", .{text}) };
            },
            .enum_literal => return .{ .enum_literal = tree.tokenSlice(main_token) },
            .identifier => {
                const name = tree.tokenSlice(main_token);
                if (std.mem.eql(u8, name, "true")) return .{ .boolean = true };
                if (std.mem.eql(u8, name, "false")) return .{ .boolean = false };
                const bound = parser.bindings.get(name) orelse
                    return parser.fail(main_token, "unknown binding '{s}'", .{name});
                if (bound == .client) {
                    return parser.fail(main_token, "'{s}' is a client", .{name});
                }
                return .{ .reference = name };
            },
            .array_access => return try parser.parse_index(node),
            .field_access => {
                if (parser.parse_api_name(node)) |name| {
                    if (std.mem.eql(u8, name, "uint128_max")) {
                        return .{ .integer = std.fmt.comptimePrint("{d}", .{api.uint128_max}) };
                    }
                }
                const resolved = try parser.parse_field_reference(node);
                return .{ .field_access = resolved.reference };
            },
            else => {},
        }

        var call_buffer: [1]std.zig.Ast.Node.Index = undefined;
        if (tree.fullCall(&call_buffer, node)) |call| {
            const name = try parser.parse_call_name(call);
            if (std.mem.eql(u8, name, "generate_id")) {
                try parser.assert_zero_arguments(call);
                return .generate_id;
            }
            if (std.mem.eql(u8, name, "generate_ids")) {
                const count_node = try parser.assert_one_argument(call, "(count)");
                const count = try parser.parse_integer(u32, count_node);
                if (count == 0) {
                    return parser.fail(tree.firstToken(count_node), "count must be positive", .{});
                }
                return .{ .generate_ids = count };
            }
            if (std.mem.eql(u8, name, "increment")) {
                return .{ .increment = try parser.parse_arithmetic(call) };
            }
            if (std.mem.eql(u8, name, "decrement")) {
                return .{ .decrement = try parser.parse_arithmetic(call) };
            }
            if (std.meta.stringToEnum(ast.Call.Name, name)) |call_name| {
                return .{ .call = try parser.parse_call(call_name, call) };
            }
            return parser.fail(tree.firstToken(node), "'{s}' cannot be a value", .{name});
        }

        return parser.fail_node(node, "invalid expression");
    }

    fn parse_api_name(parser: *const Parser, node: std.zig.Ast.Node.Index) ?[]const u8 {
        const tree = &parser.tree;
        if (tree.nodeTag(node) != .field_access) return null;
        const namespace, const name_token = tree.nodeData(node).node_and_token;
        if (tree.nodeTag(namespace) != .identifier) return null;
        if (!std.mem.eql(u8, tree.tokenSlice(tree.nodeMainToken(namespace)), "ct")) return null;
        return tree.tokenSlice(name_token);
    }

    // Returns the name of a `ct.name(...)` or a `client.operation(...)` call.
    fn parse_call_name(parser: *Parser, call: std.zig.Ast.full.Call) ![]const u8 {
        if (parser.parse_api_name(call.ast.fn_expr)) |name| return name;
        const tree = &parser.tree;
        if (tree.nodeTag(call.ast.fn_expr) != .field_access) {
            return parser.fail_node(call.ast.fn_expr, "invalid call");
        }
        const receiver, const name_token = tree.nodeData(call.ast.fn_expr).node_and_token;
        if (tree.nodeTag(receiver) != .identifier) {
            return parser.fail_node(call.ast.fn_expr, "invalid call");
        }
        const name = tree.tokenSlice(name_token);
        if (std.meta.stringToEnum(ast.Call.Name, name) == null) {
            return parser.fail(name_token, "unknown operation '{s}'", .{name});
        }
        return name;
    }

    fn parse_call(
        parser: *Parser,
        call_name: ast.Call.Name,
        call: std.zig.Ast.full.Call,
    ) ParseError!ast.Call {
        const receiver = try parser.parse_receiver(call);
        const arguments: []const ast.Expression = switch (call_name) {
            .create_accounts => try parser.parse_batch(call, .Account),
            .create_transfers => try parser.parse_batch(call, .Transfer),
            .lookup_accounts, .lookup_transfers => try parser.parse_lookup(call),
            .get_account_transfers,
            .get_account_balances,
            .query_accounts,
            .query_transfers,
            => try parser.parse_filtered(call_name, call),
            .close_client => arguments: {
                try parser.assert_zero_arguments(call);
                break :arguments &.{};
            },
        };
        return .{ .receiver = receiver, .name = call_name, .arguments = arguments };
    }

    fn parse_receiver(parser: *Parser, call: std.zig.Ast.full.Call) ![]const u8 {
        const tree = &parser.tree;
        const node = tree.nodeData(call.ast.fn_expr).node_and_token[0];
        const token = tree.nodeMainToken(node);
        const receiver = tree.tokenSlice(token);
        const bound = parser.bindings.get(receiver) orelse
            return parser.fail(token, "unknown binding '{s}'", .{receiver});
        if (bound != .client) return parser.fail(token, "'{s}' is not a client", .{receiver});
        return receiver;
    }

    // An empty `.{}` parses as a struct initializer, not an array one.
    fn parse_tuple(
        parser: *Parser,
        node: std.zig.Ast.Node.Index,
        buffer: *[2]std.zig.Ast.Node.Index,
        comptime message: []const u8,
    ) ![]const std.zig.Ast.Node.Index {
        const tree = &parser.tree;
        if (tree.fullArrayInit(buffer, node)) |array_init| return array_init.ast.elements;
        if (tree.fullStructInit(buffer, node)) |struct_init| {
            if (struct_init.ast.type_expr == .none and struct_init.ast.fields.len == 0) return &.{};
        }
        return parser.fail_node(node, message);
    }

    fn parse_batch(
        parser: *Parser,
        call: std.zig.Ast.full.Call,
        record_type: ast.Record.Type,
    ) ![]const ast.Expression {
        const tree = &parser.tree;
        const events_node = try parser.assert_one_argument(call, "(events)");
        var buffer: [2]std.zig.Ast.Node.Index = undefined;
        const elements = try parser.parse_tuple(
            events_node,
            &buffer,
            "expected a tuple of events",
        );

        const arguments = try parser.arena.alloc(ast.Expression, elements.len);
        for (elements, 0..) |element, index| {
            var init_buffer: [2]std.zig.Ast.Node.Index = undefined;
            const struct_init = tree.fullStructInit(&init_buffer, element) orelse
                return parser.fail_node(element, "expected a record");
            if (struct_init.ast.type_expr != .none) {
                return parser.fail_node(element, "event type is implied, use .{{...}}");
            }
            arguments[index] = .{
                .record = try parser.parse_record(record_type, struct_init),
            };
        }
        return arguments;
    }

    fn parse_lookup(parser: *Parser, call: std.zig.Ast.full.Call) ![]const ast.Expression {
        const ids_node = try parser.assert_one_argument(call, "(ids)");
        var buffer: [2]std.zig.Ast.Node.Index = undefined;
        const elements = try parser.parse_tuple(ids_node, &buffer, "expected a tuple of ids");

        const arguments = try parser.arena.alloc(ast.Expression, elements.len);
        for (elements, 0..) |element, index| {
            const expression = try parser.parse_expression(element);
            switch (expression) {
                .reference, .integer, .generate_id => arguments[index] = expression,
                else => return parser.fail_node(element, "expected an id"),
            }
        }
        return arguments;
    }

    fn parse_filtered(
        parser: *Parser,
        call_name: ast.Call.Name,
        call: std.zig.Ast.full.Call,
    ) ![]const ast.Expression {
        const record_type: ast.Record.Type = switch (call_name) {
            inline else => |comptime_call_name| (comptime filter_type(comptime_call_name)) orelse
                unreachable,
        };
        const filter_node = try parser.assert_one_argument(call, "(filter)");
        var buffer: [2]std.zig.Ast.Node.Index = undefined;
        const struct_init = parser.tree.fullStructInit(&buffer, filter_node) orelse
            return parser.fail_node(filter_node, "expected a filter literal");
        if (struct_init.ast.type_expr != .none) {
            return parser.fail_node(filter_node, "filter type is implied, use .{{...}}");
        }
        const arguments = try parser.arena.alloc(ast.Expression, 1);
        arguments[0] = .{ .record = try parser.parse_record(record_type, struct_init) };
        return arguments;
    }

    fn parse_concurrently(parser: *Parser, call: std.zig.Ast.full.Call) !ast.HarnessDirective {
        const tree = &parser.tree;
        const args = try parser.assert_two_arguments(call, "(count, call)");
        const count = try parser.parse_integer(u32, args[0]);
        if (count < 2) {
            return parser.fail(tree.firstToken(args[0]), "count must be at least 2", .{});
        }
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const inner = tree.fullCall(&buffer, args[1]) orelse
            return parser.fail_node(args[1], "expected a call");
        const name = try parser.parse_call_name(inner);
        const call_name = std.meta.stringToEnum(ast.Call.Name, name) orelse
            return parser.fail_node(inner.ast.fn_expr, "unknown call");
        return .{ .concurrently = .{
            .n = count,
            .call = try parser.parse_call(call_name, inner),
        } };
    }

    fn parse_failing_action(
        parser: *Parser,
        node: std.zig.Ast.Node.Index,
    ) !ast.Assertion.Action {
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const inner = parser.tree.fullCall(&buffer, node) orelse
            return parser.fail_node(node, "expected a call");
        const name = try parser.parse_call_name(inner);
        if (std.mem.eql(u8, name, "create_client")) {
            return .{ .create_client = try parser.parse_client_construction(inner) };
        }
        const call_name = std.meta.stringToEnum(ast.Call.Name, name) orelse
            return parser.fail_node(inner.ast.fn_expr, "unknown call");
        return .{ .call = try parser.parse_call(call_name, inner) };
    }

    fn parse_client_construction(
        parser: *Parser,
        call: std.zig.Ast.full.Call,
    ) !ast.ClientInitArgs {
        const tree = &parser.tree;
        const arguments_node = try parser.assert_one_argument(call, "(arguments)");
        var buffer: [2]std.zig.Ast.Node.Index = undefined;
        const struct_init = tree.fullStructInit(&buffer, arguments_node) orelse
            return parser.fail_node(arguments_node, "expected an arguments literal");
        if (struct_init.ast.type_expr != .none) {
            return parser.fail_node(arguments_node, "arguments type is implied, use .{{...}}");
        }

        var cluster_id: ?[]const u8 = null;
        var replica_addresses: ?ast.ClientInitArgs.ReplicaAddresses = null;
        for (struct_init.ast.fields) |field_init| {
            const name_token = tree.firstToken(field_init) - 2;
            const field_name = tree.tokenSlice(name_token);
            if (std.mem.eql(u8, field_name, "cluster_id")) {
                if (cluster_id != null) {
                    return parser.fail(name_token, "duplicate field 'cluster_id'", .{});
                }
                const value = try parser.parse_expression(field_init);
                if (value != .integer) return parser.fail_node(field_init, "expected an integer");
                cluster_id = value.integer;
            } else if (std.mem.eql(u8, field_name, "replica_addresses")) {
                if (replica_addresses != null) {
                    return parser.fail(name_token, "duplicate field 'replica_addresses'", .{});
                }
                replica_addresses = try parser.parse_replica_addresses(field_init);
            } else {
                return parser.fail(name_token, "no argument '{s}'", .{field_name});
            }
        }
        return .{
            .cluster_id = cluster_id orelse
                return parser.fail(struct_init.ast.lbrace, "missing 'cluster_id'", .{}),
            .replica_addresses = replica_addresses orelse
                return parser.fail(struct_init.ast.lbrace, "missing 'replica_addresses'", .{}),
        };
    }

    fn parse_replica_addresses(
        parser: *Parser,
        node: std.zig.Ast.Node.Index,
    ) !ast.ClientInitArgs.ReplicaAddresses {
        const tree = &parser.tree;
        if (tree.nodeTag(node) == .string_literal) {
            const literal = tree.tokenSlice(tree.nodeMainToken(node));
            const text = literal[1 .. literal.len - 1];
            if (std.mem.indexOfScalar(u8, text, '\\') != null) {
                return parser.fail_node(node, "escapes are not supported");
            }
            return .{ .literal = text };
        }
        var buffer: [1]std.zig.Ast.Node.Index = undefined;
        const call = tree.fullCall(&buffer, node) orelse
            return parser.fail_node(node, "expected a string or ct.replica_addresses()");
        const name = try parser.parse_call_name(call);
        if (!std.mem.eql(u8, name, "replica_addresses")) {
            return parser.fail_node(node, "expected a string or ct.replica_addresses()");
        }
        try parser.assert_zero_arguments(call);
        return .server;
    }

    fn parse_assertion(
        parser: *Parser,
        name: []const u8,
        call: std.zig.Ast.full.Call,
    ) !ast.Assertion {
        const tree = &parser.tree;
        const assertion = std.meta.stringToEnum(ast.Assertion.Name, name) orelse
            return parser.fail(
                tree.firstToken(call.ast.fn_expr),
                "unknown assertion '{s}'",
                .{name},
            );
        switch (assertion) {
            .assert_equal => {
                const args = try parser.assert_two_arguments(call, "(actual, expected)");
                if (tree.nodeTag(args[0]) == .field_access) {
                    if (tree.nodeTag(args[1]) == .field_access) {
                        return .{
                            .equal_field = try parser.parse_field_equality(args[0], args[1]),
                        };
                    }
                    const value = try parser.parse_expression(args[1]);
                    const comparison =
                        try parser.parse_field_comparison(args[0], args[1], value);
                    return .{ .equal_field = comparison };
                }

                const actual_token = tree.firstToken(args[0]);
                const actual = try parser.parse_expression(args[0]);
                const record_type = try parser.result_record_type(actual, actual_token);

                var buffer: [2]std.zig.Ast.Node.Index = undefined;
                const elements =
                    try parser.parse_tuple(args[1], &buffer, "expected a tuple of records");
                if (elements.len == 0) {
                    return parser.fail(call.ast.lparen, "use assert_empty", .{});
                }
                const expected = try parser.arena.alloc(ast.Record, elements.len);
                for (elements, 0..) |element, index| {
                    var init_buffer: [2]std.zig.Ast.Node.Index = undefined;
                    const struct_init = tree.fullStructInit(&init_buffer, element) orelse
                        return parser.fail_node(element, "expected a record");
                    if (struct_init.ast.type_expr != .none) {
                        return parser.fail_node(element, "record type is implied, use .{{...}}");
                    }
                    expected[index] = try parser.parse_record(record_type, struct_init);
                }
                return .{ .equal = .{ .actual = actual.reference, .expected = expected } };
            },
            .assert_empty => {
                const node = try parser.assert_one_argument(call, "(actual)");
                return .{ .empty = try parser.parse_reference(node) };
            },
            .assert_id_ascending => {
                const node = try parser.assert_one_argument(call, "(ids)");
                return .{ .id_ascending = try parser.parse_reference(node) };
            },
            .assert_greater_than => {
                const args = try parser.assert_two_arguments(call, "(actual, value)");
                if (tree.nodeTag(args[1]) != .number_literal) {
                    return parser.fail_node(args[1], "expected an integer");
                }
                const integer = tree.tokenSlice(tree.nodeMainToken(args[1]));
                const comparison = try parser.parse_field_comparison(args[0], args[1], .{
                    .integer = integer,
                });
                return .{ .greater_than = comparison };
            },
            .assert_fail => {
                const inner_node = try parser.assert_one_argument(call, "(call)");
                return .{ .fail = .{
                    .action = try parser.parse_failing_action(inner_node),
                    .client_error = null,
                } };
            },
            .assert_fail_with => {
                const args = try parser.assert_two_arguments(call, "(call, client_error)");
                const value = try parser.parse_expression(args[1]);
                if (value != .enum_literal) {
                    return parser.fail_node(args[1], "expected a client error");
                }
                const client_error =
                    std.meta.stringToEnum(ast.ClientError, value.enum_literal) orelse
                    return parser.fail_node(args[1], "unknown client error");
                const action = try parser.parse_failing_action(args[0]);
                const action_matches = switch (client_error) {
                    .address_invalid,
                    .address_limit_exceeded,
                    .cluster_id_invalid,
                    => action == .create_client,
                    .too_much_data, .client_closed => action == .call,
                };
                if (!action_matches) {
                    return parser.fail_node(args[1], "invalid error for operation");
                }
                return .{ .fail = .{ .action = action, .client_error = client_error } };
            },
        }
    }

    fn parse_record(
        parser: *Parser,
        record_type: ast.Record.Type,
        struct_init: std.zig.Ast.full.StructInit,
    ) ParseError!ast.Record {
        const tree = &parser.tree;
        const fields = try parser.arena.alloc(ast.FieldValue, struct_init.ast.fields.len);
        var seen = std.bit_set.IntegerBitSet(64).initEmpty();
        for (struct_init.ast.fields, 0..) |field_init, index| {
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

            var buffer: [2]std.zig.Ast.Node.Index = undefined;
            if (tree.fullStructInit(&buffer, field_init)) |nested| {
                if (nested.ast.type_expr == .none) {
                    const nested_type = record_field_record_type(record_type, field_name) orelse
                        return parser.fail(name_token, "field '{s}' takes no record", .{
                            field_name,
                        });
                    fields[index] = .{
                        .field = .{
                            .name = field_name,
                            .type = ast.Field.Type.resolve(record_type, field_name),
                        },
                        .value = .{ .record = try parser.parse_record(nested_type, nested) },
                    };
                    continue;
                }
            }

            const value = try parser.parse_expression(field_init);
            if (ast.Record.is_flags(record_type) and (value != .boolean or !value.boolean)) {
                return parser.fail(name_token, "flag '{s}' must be true (omit to leave false)", .{
                    field_name,
                });
            }
            if (value == .enum_literal and
                !record_field_enum_has(record_type, field_name, value.enum_literal))
            {
                return parser.fail(name_token, "no member '.{s}' on {s}.{s}", .{
                    value.enum_literal,
                    @tagName(record_type),
                    field_name,
                });
            }
            fields[index] = .{
                .field = .{
                    .name = field_name,
                    .type = ast.Field.Type.resolve(record_type, field_name),
                },
                .value = value,
            };
        }
        const empty_allowed = switch (record_type) {
            .AccountFilter, .QueryFilter => true,
            else => ast.Record.is_flags(record_type),
        };
        if (fields.len == 0 and !empty_allowed) {
            return parser.fail(struct_init.ast.lbrace, "empty record", .{});
        }
        return .{ .type = record_type, .fields = fields };
    }

    fn result_record_type(
        parser: *Parser,
        actual: ast.Expression,
        token: std.zig.Ast.TokenIndex,
    ) !ast.Record.Type {
        if (actual == .reference) {
            const value = parser.bindings.get(actual.reference).?.data;
            if (value == .call) {
                if (operation_result(value.call.name)) |record_type| return record_type;
            }
        }
        return parser.fail(token, "expected an operation result", .{});
    }

    fn parse_reference(parser: *Parser, node: std.zig.Ast.Node.Index) ![]const u8 {
        const expression = try parser.parse_expression(node);
        if (expression != .reference) return parser.fail_node(node, "expected a binding");
        return expression.reference;
    }

    fn resolve_reference(
        parser: *Parser,
        node: std.zig.Ast.Node.Index,
    ) !struct { reference: []const u8, value: ast.Expression } {
        const tree = &parser.tree;
        if (tree.nodeTag(node) != .identifier) {
            return parser.fail_node(node, "expected a binding");
        }
        const reference = tree.tokenSlice(tree.nodeMainToken(node));
        const bound = parser.bindings.get(reference) orelse
            return parser.fail(tree.firstToken(node), "unknown binding '{s}'", .{reference});
        if (bound == .client) {
            return parser.fail(tree.firstToken(node), "'{s}' is a client", .{reference});
        }
        return .{ .reference = reference, .value = bound.data };
    }

    fn parse_index(parser: *Parser, node: std.zig.Ast.Node.Index) !ast.Expression {
        const base_node, const index_node = parser.tree.nodeData(node).node_and_node;
        const resolved = try parser.resolve_reference(base_node);
        const record_type = switch (resolved.value) {
            .call => |call| operation_result(call.name),
            else => null,
        } orelse return parser.fail_node(base_node, "expected an operation result");

        const index = try parser.parse_integer(u32, index_node);
        return .{ .index = .{
            .reference = resolved.reference,
            .index = index,
            .record_type = record_type,
        } };
    }

    fn parse_arithmetic(
        parser: *Parser,
        call: std.zig.Ast.full.Call,
    ) !ast.Expression.Arithmetic {
        const args = try parser.assert_two_arguments(call, "(reference, by)");
        const resolved = try parser.resolve_reference(args[0]);
        if (resolved.value != .field_access) {
            return parser.fail_node(args[0], "expected a binding to a field");
        }
        const field_type = resolved.value.field_access.field.type;
        if (field_type != .int or field_type.int > 64) {
            return parser.fail_node(args[0], "expected an integer field of at most 64 bits");
        }
        return .{
            .reference = resolved.reference,
            .by = try parser.parse_integer(u64, args[1]),
        };
    }

    // Only works on a single record taken from a slice, like `const foo = foos[0];`.
    fn parse_field_reference(parser: *Parser, node: std.zig.Ast.Node.Index) !struct {
        reference: ast.FieldReference,
        record_type: ast.Record.Type,
    } {
        const tree = &parser.tree;
        if (tree.nodeTag(node) != .field_access) {
            return parser.fail_node(node, "expected a field access");
        }
        const base_node, const field_token = tree.nodeData(node).node_and_token;
        const field_name = tree.tokenSlice(field_token);

        const resolved = try parser.resolve_reference(base_node);
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
        return .{
            .reference = .{
                .reference = resolved.reference,
                .field = .{
                    .name = field_name,
                    .type = ast.Field.Type.resolve(record_type, field_name),
                },
            },
            .record_type = record_type,
        };
    }

    fn parse_field_comparison(
        parser: *Parser,
        node: std.zig.Ast.Node.Index,
        value_node: std.zig.Ast.Node.Index,
        value: ast.Expression,
    ) !ast.Assertion.FieldComparison {
        const actual = try parser.parse_field_reference(node);
        if (value == .enum_literal and !record_field_enum_has(
            actual.record_type,
            actual.reference.field.name,
            value.enum_literal,
        )) {
            return parser.fail(
                parser.tree.firstToken(value_node),
                "no member '.{s}' on {s}.{s}",
                .{ value.enum_literal, @tagName(actual.record_type), actual.reference.field.name },
            );
        }
        return .{ .actual = actual.reference, .expected = .{ .expression = value } };
    }

    fn parse_field_equality(
        parser: *Parser,
        actual_node: std.zig.Ast.Node.Index,
        expected_node: std.zig.Ast.Node.Index,
    ) !ast.Assertion.FieldComparison {
        const actual = try parser.parse_field_reference(actual_node);
        const expected = try parser.parse_field_reference(expected_node);
        if (!field_types_equal(actual.reference.field.type, expected.reference.field.type)) {
            return parser.fail(
                parser.tree.firstToken(expected_node),
                "'{s}' and '{s}' have different types",
                .{ actual.reference.field.name, expected.reference.field.name },
            );
        }
        return .{
            .actual = actual.reference,
            .expected = .{ .field_reference = expected.reference },
        };
    }

    fn assert_arguments(
        parser: *Parser,
        call: std.zig.Ast.full.Call,
        count: usize,
        comptime expected: []const u8,
    ) !void {
        if (call.ast.params.len != count) {
            return parser.fail_arguments(call, expected);
        }
    }

    fn assert_zero_arguments(parser: *Parser, call: std.zig.Ast.full.Call) !void {
        return parser.assert_arguments(call, 0, "()");
    }

    fn assert_one_argument(
        parser: *Parser,
        call: std.zig.Ast.full.Call,
        comptime expected: []const u8,
    ) !std.zig.Ast.Node.Index {
        try parser.assert_arguments(call, 1, expected);
        return call.ast.params[0];
    }

    fn assert_two_arguments(
        parser: *Parser,
        call: std.zig.Ast.full.Call,
        comptime expected: []const u8,
    ) ![2]std.zig.Ast.Node.Index {
        try parser.assert_arguments(call, 2, expected);
        return call.ast.params[0..2].*;
    }

    fn parse_integer(parser: *Parser, comptime Int: type, node: std.zig.Ast.Node.Index) !Int {
        const tree = &parser.tree;
        if (tree.nodeTag(node) != .number_literal) {
            return parser.fail_node(node, "expected an integer");
        }
        const token = tree.nodeMainToken(node);
        return stdx.parse_int(Int, tree.tokenSlice(token), .{}) catch
            parser.fail(token, "invalid integer", .{});
    }
};

fn field_types_equal(actual: ast.Field.Type, expected: ast.Field.Type) bool {
    return switch (actual) {
        .int => |bits| expected == .int and expected.int == bits,
        .enum_name => |name| expected == .enum_name and
            std.mem.eql(u8, expected.enum_name, name),
        .boolean => expected == .boolean,
        .flags => expected == .flags,
    };
}

fn api_record_type(comptime T: type) ?ast.Record.Type {
    inline for (std.enums.values(ast.Record.Type)) |record_type| {
        if (ast.Record.Struct(record_type) == T) return record_type;
    }
    return null;
}

fn filter_type(comptime operation: ast.Call.Name) ?ast.Record.Type {
    const params = @typeInfo(@TypeOf(@field(api.Client, @tagName(operation)))).@"fn".params;
    if (params.len != 2) return null;
    const Param = params[1].type orelse return null;
    return api_record_type(Param);
}

fn operation_result(operation: ast.Call.Name) ?ast.Record.Type {
    switch (operation) {
        inline else => |comptime_operation| return comptime result: {
            const function = @typeInfo(
                @TypeOf(@field(api.Client, @tagName(comptime_operation))),
            ).@"fn";
            if (function.return_type.? == void) break :result null;
            break :result api_record_type(@typeInfo(function.return_type.?).pointer.child);
        },
    }
}

fn field_is_reserved(name: []const u8) bool {
    return std.mem.eql(u8, name, "padding") or std.mem.eql(u8, name, "reserved");
}

fn record_field_index(record_type: ast.Record.Type, name: []const u8) ?usize {
    if (field_is_reserved(name)) return null;
    switch (record_type) {
        inline else => |comptime_type| {
            const Field = std.meta.FieldEnum(ast.Record.Struct(comptime_type));
            const field = std.meta.stringToEnum(Field, name) orelse return null;
            return @intFromEnum(field);
        },
    }
}

fn record_field_record_type(
    record_type: ast.Record.Type,
    name: []const u8,
) ?ast.Record.Type {
    switch (record_type) {
        inline else => |comptime_type| {
            inline for (std.meta.fields(ast.Record.Struct(comptime_type))) |field| {
                if (@typeInfo(field.type) != .@"struct") continue;
                if (std.mem.eql(u8, field.name, name)) return comptime api_record_type(field.type);
            }
            return null;
        },
    }
}

fn record_field_enum_has(
    record_type: ast.Record.Type,
    field_name: []const u8,
    member: []const u8,
) bool {
    switch (record_type) {
        inline else => |comptime_type| {
            inline for (std.meta.fields(ast.Record.Struct(comptime_type))) |field| {
                if (@typeInfo(field.type) != .@"enum") continue;
                if (std.mem.eql(u8, field.name, field_name)) {
                    return std.meta.stringToEnum(field.type, member) != null;
                }
            }
            return false;
        },
    }
}
