const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../vsr.zig");
const constants = vsr.constants;
const IO = vsr.io.IO;
const Storage = vsr.storage.StorageType(IO);
const StateMachine = vsr.state_machine.StateMachineType(Storage);
const tb = vsr.tigerbeetle;

const Terminal = @import("terminal.zig").Terminal;

pub const Parser = struct {
    input: []const u8,
    offset: usize = 0,
    terminal: *const Terminal,

    pub const Error = error{
        IdentifierBad,
        OperationBad,
        ValueBad,
        KeyValuePairBad,
        KeyValuePairEqualMissing,
        SyntaxMatchNone,
        SliceOperationUnsupported,
    };

    pub const Operation = enum {
        none,
        help,
        create_accounts,
        create_transfers,
        lookup_accounts,
        lookup_transfers,
        get_account_transfers,
        get_account_balances,
        query_accounts,
        query_transfers,

        pub fn state_machine_op(operation: Operation) StateMachine.Operation {
            return switch (operation) {
                .none, .help => unreachable,
                .create_accounts => .create_accounts,
                .create_transfers => .create_transfers,
                .lookup_accounts => .lookup_accounts,
                .lookup_transfers => .lookup_transfers,
                .get_account_transfers => .get_account_transfers,
                .get_account_balances => .get_account_balances,
                .query_accounts => .query_accounts,
                .query_transfers => .query_transfers,
            };
        }
    };

    pub const LookupSyntaxTree = struct {
        id: u128,
    };

    pub const ObjectSyntaxTree = union(enum) {
        account: tb.Account,
        transfer: tb.Transfer,
        id: LookupSyntaxTree,
        account_filter: tb.AccountFilter,
        query_filter: tb.QueryFilter,
    };

    pub const Statement = struct {
        operation: Operation,
        arguments: *std.ArrayListUnmanaged(u8),
    };

    fn print_current_position(parser: *const Parser) !void {
        const target = target: {
            var position_cursor: usize = 0;
            var position_line: usize = 1;
            var lines = std.mem.splitScalar(u8, parser.input, '\n');
            while (lines.next()) |line| {
                if (position_cursor + line.len >= parser.offset) {
                    break :target .{
                        .line = line,
                        .position_line = position_line,
                        .position_column = parser.offset - position_cursor,
                    };
                } else {
                    position_line += 1;
                    position_cursor += line.len + 1; // +1 for trailing newline.
                }
            } else unreachable;
        };

        try parser.terminal.print_error("Fail near line {}, column {}:\n\n{s}\n", .{
            target.position_line,
            target.position_column,
            target.line,
        });
        var column = target.position_column;
        while (column > 0) {
            try parser.terminal.print_error(" ", .{});
            column -= 1;
        }
        try parser.terminal.print_error("^ Near here.\n\n", .{});
    }

    fn eat_whitespace(parser: *Parser) void {
        while (parser.offset < parser.input.len and
            std.ascii.isWhitespace(parser.input[parser.offset]))
        {
            parser.offset += 1;
        }
    }

    fn parse_identifier(parser: *Parser) []const u8 {
        parser.eat_whitespace();
        const after_whitespace = parser.offset;

        while (parser.offset < parser.input.len) {
            const char_is_valid = switch (parser.input[parser.offset]) {
                // Identifiers can contain any letter and `_`.
                'A'...'Z', 'a'...'z', '_' => true,
                // It also may contain numbers, but not start with a number.
                '0'...'9' => parser.offset > after_whitespace,
                else => false,
            };

            if (!char_is_valid) break;
            parser.offset += 1;
        }

        return parser.input[after_whitespace..parser.offset];
    }

    fn parse_syntax_char(parser: *Parser, syntax_char: u8) !void {
        parser.eat_whitespace();

        if (parser.offset < parser.input.len and
            parser.input[parser.offset] == syntax_char)
        {
            parser.offset += 1;
            return;
        }

        return Error.SyntaxMatchNone;
    }

    fn parse_value(parser: *Parser) []const u8 {
        parser.eat_whitespace();
        const after_whitespace = parser.offset;

        while (parser.offset < parser.input.len) {
            const c = parser.input[parser.offset];
            if (!(std.ascii.isAlphanumeric(c) or c == '_' or c == '|' or c == '-')) {
                // Allows flag fields to have whitespace before a '|'.
                var copy = Parser{
                    .input = parser.input,
                    .offset = parser.offset,
                    .terminal = parser.terminal,
                };
                copy.eat_whitespace();
                if (copy.offset < parser.input.len and parser.input[copy.offset] == '|') {
                    parser.offset = copy.offset;
                    continue;
                }

                // Allow flag fields to have whitespace after a '|'.
                if (copy.offset < parser.input.len and
                    parser.offset > 0 and
                    parser.input[parser.offset - 1] == '|')
                {
                    parser.offset = copy.offset;
                    continue;
                }

                break;
            }

            parser.offset += 1;
        }

        return parser.input[after_whitespace..parser.offset];
    }

    fn match_arg(
        out: *ObjectSyntaxTree,
        key_to_validate: []const u8,
        value_to_validate: []const u8,
    ) !void {
        inline for (@typeInfo(ObjectSyntaxTree).@"union".fields) |object_syntax_tree_field| {
            if (std.mem.eql(u8, @tagName(out.*), object_syntax_tree_field.name)) {
                const active_value = @field(out, object_syntax_tree_field.name);
                const ActiveValue = @TypeOf(active_value);

                inline for (@typeInfo(ActiveValue).@"struct".fields) |active_value_field| {
                    if (std.mem.eql(u8, active_value_field.name, key_to_validate)) {
                        // Handle everything but flags, and skip reserved.
                        if (comptime (!std.mem.eql(u8, active_value_field.name, "flags") and
                            !std.mem.eql(u8, active_value_field.name, "reserved")))
                        {
                            @field(
                                @field(out.*, object_syntax_tree_field.name),
                                active_value_field.name,
                            ) = try parse_int(
                                active_value_field.type,
                                value_to_validate,
                            );
                        }

                        // Handle flags, specific to Account and Transfer fields.
                        if (comptime std.mem.eql(u8, active_value_field.name, "flags") and
                            @hasField(ActiveValue, "flags"))
                        {
                            var flags_to_validate = std.mem.splitScalar(u8, value_to_validate, '|');
                            var validated_flags =
                                std.mem.zeroInit(active_value_field.type, .{});
                            while (flags_to_validate.next()) |flag_to_validate| {
                                const flag_to_validate_trimmed = std.mem.trim(
                                    u8,
                                    flag_to_validate,
                                    std.ascii.whitespace[0..],
                                );
                                inline for (@typeInfo(
                                    active_value_field.type,
                                ).@"struct".fields) |known_flag_field| {
                                    if (std.mem.eql(
                                        u8,
                                        known_flag_field.name,
                                        flag_to_validate_trimmed,
                                    )) {
                                        if (comptime !std.mem.eql(
                                            u8,
                                            known_flag_field.name,
                                            "padding",
                                        )) {
                                            @field(validated_flags, known_flag_field.name) = true;
                                        }
                                    }
                                }
                            }
                            @field(
                                @field(out.*, object_syntax_tree_field.name),
                                "flags",
                            ) = validated_flags;
                        }
                    }
                }
            }
        }
    }

    fn parse_int(comptime T: type, input: []const u8) !T {
        const info = @typeInfo(T);
        comptime assert(info == .int);

        // When base is zero the string prefix is examined to detect the true base:
        // "0b", "0o" or "0x", otherwise base=10 is assumed.
        const base_unknown = 0;

        assert(input.len > 0);
        const input_negative = input[0] == '-';

        if (info.int.signedness == .unsigned and input_negative) {
            // Negative input means `maxInt - input`.
            // Useful for representing sentinels such as `AMOUNT_MAX`, as `-0`.
            const max = std.math.maxInt(T);
            return max - try std.fmt.parseUnsigned(T, input[1..], base_unknown);
        }

        return try std.fmt.parseUnsigned(T, input, base_unknown);
    }

    fn parse_arguments(
        parser: *Parser,
        operation: Operation,
        arguments: *std.ArrayListUnmanaged(u8),
    ) !void {
        const default: ObjectSyntaxTree = switch (operation) {
            .help, .none => return,
            .create_accounts => .{ .account = std.mem.zeroInit(tb.Account, .{}) },
            .create_transfers => .{ .transfer = std.mem.zeroInit(tb.Transfer, .{}) },
            .lookup_accounts, .lookup_transfers => .{ .id = .{ .id = 0 } },
            inline .get_account_transfers,
            .get_account_balances,
            => |operation_comptime| .{ .account_filter = tb.AccountFilter{
                .account_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .code = 0,
                .timestamp_min = 0,
                .timestamp_max = 0,
                .limit = operation_comptime.state_machine_op().result_max(
                    constants.message_body_size_max,
                ),
                .flags = .{
                    .credits = true,
                    .debits = true,
                    .reversed = false,
                },
            } },
            inline .query_accounts,
            .query_transfers,
            => |operation_comptime| .{ .query_filter = tb.QueryFilter{
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .ledger = 0,
                .code = 0,
                .timestamp_min = 0,
                .timestamp_max = 0,
                .limit = operation_comptime.state_machine_op().result_max(
                    constants.message_body_size_max,
                ),
                .flags = .{
                    .reversed = false,
                },
            } },
        };
        var object = default;

        var object_has_fields = false;
        while (parser.offset < parser.input.len) {
            parser.eat_whitespace();
            // Always need to check i against length in case we've hit the end.
            if (parser.offset >= parser.input.len or parser.input[parser.offset] == ';') {
                break;
            }

            // Expect comma separating objects.
            if (parser.offset < parser.input.len and parser.input[parser.offset] == ',') {
                parser.offset += 1;
                inline for (@typeInfo(ObjectSyntaxTree).@"union".fields) |object_tree_field| {
                    if (std.mem.eql(u8, @tagName(object), object_tree_field.name)) {
                        const unwrapped_field = @field(object, object_tree_field.name);
                        arguments.appendSliceAssumeCapacity(std.mem.asBytes(&unwrapped_field));
                    }
                }

                const state_machine_op = operation.state_machine_op();
                if (!state_machine_op.is_batchable()) {
                    try parser.print_current_position();
                    try parser.terminal.print_error(
                        "{s} expects a single {s} but received multiple.\n",
                        .{ @tagName(operation), @tagName(object) },
                    );
                    return error.SliceOperationUnsupported;
                }

                // Reset object.
                object = default;
                object_has_fields = false;
            }

            // Grab key.
            const id_result = parser.parse_identifier();

            if (id_result.len == 0) {
                try parser.print_current_position();
                try parser.terminal.print_error(
                    "Expected key starting key-value pair. e.g. `id=1`\n",
                    .{},
                );
                return Error.IdentifierBad;
            }

            // Grab =.
            parser.parse_syntax_char('=') catch {
                try parser.print_current_position();
                try parser.terminal.print_error(
                    "Expected equal sign after key '{s}' in key-value" ++
                        " pair. e.g. `id=1`.\n",
                    .{id_result},
                );
                return Error.KeyValuePairEqualMissing;
            };

            // Grab value.
            const value_result = parser.parse_value();

            if (value_result.len == 0) {
                try parser.print_current_position();
                try parser.terminal.print_error(
                    "Expected value after equal sign in key-value pair. e.g. `id=1`.\n",
                    .{},
                );
                return Error.ValueBad;
            }

            // Match key to a field in the struct.
            match_arg(&object, id_result, value_result) catch {
                try parser.print_current_position();
                try parser.terminal.print_error(
                    "'{s}'='{s}' is not a valid pair for {s}.\n",
                    .{ id_result, value_result, @tagName(object) },
                );
                return Error.KeyValuePairBad;
            };

            object_has_fields = true;
        }

        // Add final object.
        if (object_has_fields) {
            inline for (@typeInfo(ObjectSyntaxTree).@"union".fields) |object_tree_field| {
                if (std.mem.eql(u8, @tagName(object), object_tree_field.name)) {
                    const unwrapped_field = @field(object, object_tree_field.name);
                    arguments.appendSliceAssumeCapacity(std.mem.asBytes(&unwrapped_field));
                }
            }
        }
    }

    // Statement grammar parsed here.
    // STATEMENT: OPERATION ARGUMENTS [;]
    // OPERATION: create_accounts | lookup_accounts | create_transfers | lookup_transfers
    //      ARGUMENTS: ARG [, ARG]
    //       ARG: KEY = VALUE
    //       KEY: string
    //     VALUE: string [| VALUE]
    //
    // For example:
    //   create_accounts id=1 code=2 ledger=3, id = 2 code= 2 ledger =3;
    //   create_accounts flags=linked | debits_must_not_exceed_credits ;
    pub fn parse_statement(
        input: []const u8,
        terminal: *const Terminal,
        arguments: *std.ArrayListUnmanaged(u8),
    ) (error{OutOfMemory} || std.fs.File.WriteError || Error)!Statement {
        var parser = Parser{ .input = input, .terminal = terminal };
        parser.eat_whitespace();
        const after_whitespace = parser.offset;
        const operation_identifier = parser.parse_identifier();

        const operation = operation: {
            if (std.meta.stringToEnum(Operation, operation_identifier)) |valid_operation| {
                break :operation valid_operation;
            }

            if (operation_identifier.len == 0) {
                break :operation .none;
            }

            // Set up the offset to after the whitespace so the
            // print_current_position function points at where we actually expected the
            // token.
            parser.offset = after_whitespace;
            try parser.print_current_position();
            try parser.terminal.print_error(
                "Operation must be " ++
                    comptime operations: {
                        var names: []const u8 = "";
                        for (std.enums.values(Operation), 0..) |operation, index| {
                            if (operation == .none) continue;
                            names = names ++
                                (if (names.len > 0) ", " else "") ++
                                (if (index == std.enums.values(Operation).len - 1) "or " else "") ++
                                @tagName(operation);
                        }
                        break :operations names;
                    } ++ ". Got: '{s}'.\n",
                .{operation_identifier},
            );
            return Error.OperationBad;
        };

        try parser.parse_arguments(operation, arguments);

        return Statement{
            .operation = operation,
            .arguments = arguments,
        };
    }
};

const null_terminal = Terminal{
    .mode_start = null,
    .stdin = undefined,
    .stderr = null,
    .stdout = null,
};

test "parser.zig: Parser single transfer successfully" {
    const vectors = [_]struct {
        string: []const u8 = "",
        result: tb.Transfer,
    }{
        .{
            .string = "create_transfers id=1",
            .result = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        .{
            .string = "create_transfers timestamp=1",
            .result = tb.Transfer{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 1,
            },
        },
        .{
            .string =
            \\create_transfers id=32 amount=65 ledger=12 code=9999 pending_id=7
            \\ credit_account_id=2121 debit_account_id=77 user_data_128=2
            \\ user_data_64=3 user_data_32=4 flags=linked
            ,
            .result = tb.Transfer{
                .id = 32,
                .debit_account_id = 77,
                .credit_account_id = 2121,
                .amount = 65,
                .pending_id = 7,
                .user_data_128 = 2,
                .user_data_64 = 3,
                .user_data_32 = 4,
                .timeout = 0,
                .ledger = 12,
                .code = 9999,
                .flags = .{ .linked = true },
                .timestamp = 0,
            },
        },
        .{
            .string =
            \\create_transfers flags=
            \\ post_pending_transfer |
            \\ balancing_credit |
            \\ balancing_debit |
            \\ void_pending_transfer |
            \\ pending |
            \\ linked
            ,
            .result = tb.Transfer{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .post_pending_transfer = true,
                    .balancing_credit = true,
                    .balancing_debit = true,
                    .void_pending_transfer = true,
                    .pending = true,
                    .linked = true,
                },
                .timestamp = 0,
            },
        },
        .{
            .string =
            \\create_transfers amount=-0
            ,
            .result = tb.Transfer{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = std.math.maxInt(u128),
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        .{
            .string =
            \\create_transfers amount=-1
            ,
            .result = tb.Transfer{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = std.math.maxInt(u128) - 1,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        .{
            .string =
            \\create_transfers amount=0xbee71e
            ,
            .result = tb.Transfer{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0xbee71e,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        .{
            .string =
            \\create_transfers amount=1_000_000
            ,
            .result = tb.Transfer{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 1_000_000,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        .{
            .string =
            \\create_transfers id=0xa1a2a3a4_b1b2_c1c2_d1d2_e1e2e3e4e5e6
            ,
            .result = tb.Transfer{
                .id = 0xa1a2a3a4_b1b2_c1c2_d1d2_e1e2e3e4e5e6,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const statement = try Parser.parse_statement(
            vector.string,
            &null_terminal,
            &arguments,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(
            u8,
            statement.arguments.items,
            std.mem.asBytes(&vector.result),
        );
    }
}

test "parser.zig: Parser multiple transfers successfully" {
    const vectors = [_]struct {
        string: []const u8 = "",
        result: [2]tb.Transfer,
    }{
        .{
            .string = "create_transfers id=1 debit_account_id=2, id=2 credit_account_id = 1;",
            .result = [2]tb.Transfer{
                tb.Transfer{
                    .id = 1,
                    .debit_account_id = 2,
                    .credit_account_id = 0,
                    .amount = 0,
                    .pending_id = 0,
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .timeout = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                    .timestamp = 0,
                },
                tb.Transfer{
                    .id = 2,
                    .debit_account_id = 0,
                    .credit_account_id = 1,
                    .amount = 0,
                    .pending_id = 0,
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .timeout = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                    .timestamp = 0,
                },
            },
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const statement = try Parser.parse_statement(
            vector.string,
            &null_terminal,
            &arguments,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(
            u8,
            statement.arguments.items,
            std.mem.sliceAsBytes(&vector.result),
        );
    }
}

test "parser.zig: Parser single account successfully" {
    const vectors = [_]struct {
        string: []const u8,
        result: tb.Account,
    }{
        .{
            .string = "create_accounts id=1",
            .result = tb.Account{
                .id = 1,
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .reserved = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        .{
            .string =
            \\create_accounts id=32 credits_posted=344 ledger=12 credits_pending=18
            \\ code=9999 flags=linked | debits_must_not_exceed_credits debits_posted=3390
            \\ debits_pending=3212 user_data_128=2 user_data_64=3 user_data_32=4
            ,
            .result = tb.Account{
                .id = 32,
                .debits_pending = 3212,
                .debits_posted = 3390,
                .credits_pending = 18,
                .credits_posted = 344,
                .user_data_128 = 2,
                .user_data_64 = 3,
                .user_data_32 = 4,
                .reserved = 0,
                .ledger = 12,
                .code = 9999,
                .flags = .{ .linked = true, .debits_must_not_exceed_credits = true },
                .timestamp = 0,
            },
        },
        .{
            .string =
            \\create_accounts flags=credits_must_not_exceed_debits|
            \\ linked|debits_must_not_exceed_credits id =1
            ,
            .result = tb.Account{
                .id = 1,
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .reserved = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .credits_must_not_exceed_debits = true,
                    .linked = true,
                    .debits_must_not_exceed_credits = true,
                },
                .timestamp = 0,
            },
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const statement = try Parser.parse_statement(vector.string, &null_terminal, &arguments);

        try std.testing.expectEqual(statement.operation, .create_accounts);
        try std.testing.expectEqualSlices(
            u8,
            statement.arguments.items,
            std.mem.asBytes(&vector.result),
        );
    }
}

test "parser.zig: Parser account filter successfully" {
    const vectors = [_]struct {
        string: []const u8,
        operation: Parser.Operation,
        result: tb.AccountFilter,
    }{
        .{
            .string = "get_account_transfers account_id=1",
            .operation = .get_account_transfers,
            .result = tb.AccountFilter{
                .account_id = 1,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .code = 0,
                .timestamp_min = 0,
                .timestamp_max = 0,
                .limit = StateMachine.Operation.get_account_transfers.result_max(
                    constants.message_body_size_max,
                ),
                .flags = .{
                    .credits = true,
                    .debits = true,
                    .reversed = false,
                },
            },
        },
        .{
            .string =
            \\get_account_balances account_id=1000
            \\user_data_128=128 user_data_64=64 user_data_32=32
            \\code=2
            \\flags=debits|reversed limit=10
            \\timestamp_min=1 timestamp_max=9999;
            \\
            ,
            .operation = .get_account_balances,
            .result = tb.AccountFilter{
                .account_id = 1000,
                .user_data_128 = 128,
                .user_data_64 = 64,
                .user_data_32 = 32,
                .code = 2,
                .timestamp_min = 1,
                .timestamp_max = 9999,
                .limit = 10,
                .flags = .{
                    .credits = false,
                    .debits = true,
                    .reversed = true,
                },
            },
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const statement = try Parser.parse_statement(vector.string, &null_terminal, &arguments);

        try std.testing.expectEqual(statement.operation, vector.operation);
        try std.testing.expectEqualSlices(
            u8,
            statement.arguments.items,
            std.mem.asBytes(&vector.result),
        );
    }
}

test "parser.zig: Parser query filter successfully" {
    const vectors = [_]struct {
        string: []const u8,
        operation: Parser.Operation,
        result: tb.QueryFilter,
    }{
        .{
            .string = "query_transfers user_data_128=1",
            .operation = .query_transfers,
            .result = tb.QueryFilter{
                .user_data_128 = 1,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .ledger = 0,
                .code = 0,
                .timestamp_min = 0,
                .timestamp_max = 0,
                .limit = StateMachine.Operation.query_transfers.result_max(
                    constants.message_body_size_max,
                ),
                .flags = .{
                    .reversed = false,
                },
            },
        },
        .{
            .string =
            \\query_accounts user_data_128=1000
            \\user_data_64=100 user_data_32=10
            \\ledger=1 code=2
            \\flags=reversed limit=10
            \\timestamp_min=1 timestamp_max=9999;
            \\
            ,
            .operation = .query_accounts,
            .result = tb.QueryFilter{
                .user_data_128 = 1000,
                .user_data_64 = 100,
                .user_data_32 = 10,
                .ledger = 1,
                .code = 2,
                .timestamp_min = 1,
                .timestamp_max = 9999,
                .limit = 10,
                .flags = .{
                    .reversed = true,
                },
            },
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const statement = try Parser.parse_statement(
            vector.string,
            &null_terminal,
            &arguments,
        );

        try std.testing.expectEqual(statement.operation, vector.operation);
        try std.testing.expectEqualSlices(
            u8,
            statement.arguments.items,
            std.mem.asBytes(&vector.result),
        );
    }
}

test "parser.zig: Parser multiple accounts successfully" {
    const vectors = [_]struct {
        string: []const u8,
        result: [2]tb.Account,
    }{
        .{
            .string = "create_accounts id=1, id=2",
            .result = [2]tb.Account{
                tb.Account{
                    .id = 1,
                    .debits_pending = 0,
                    .debits_posted = 0,
                    .credits_pending = 0,
                    .credits_posted = 0,
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .reserved = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                    .timestamp = 0,
                },
                tb.Account{
                    .id = 2,
                    .debits_pending = 0,
                    .debits_posted = 0,
                    .credits_pending = 0,
                    .credits_posted = 0,
                    .user_data_128 = 0,
                    .user_data_64 = 0,
                    .user_data_32 = 0,
                    .reserved = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                    .timestamp = 0,
                },
            },
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const statement = try Parser.parse_statement(
            vector.string,
            &null_terminal,
            &arguments,
        );

        try std.testing.expectEqual(statement.operation, .create_accounts);
        try std.testing.expectEqualSlices(
            u8,
            statement.arguments.items,
            std.mem.sliceAsBytes(&vector.result),
        );
    }
}

test "parser.zig: Parser odd but correct formatting" {
    const vectors = [_]struct {
        string: []const u8 = "",
        result: tb.Transfer,
    }{
        // Space between key-value pair and equality
        .{
            .string = "create_transfers id = 1",
            .result = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        // Space only before equals sign
        .{
            .string = "create_transfers id =1",
            .result = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        // Whitespace before command
        .{
            .string = "  \t  \n  create_transfers id=1",
            .result = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        // Trailing semicolon
        .{
            .string = "create_transfers id=1;",
            .result = tb.Transfer{
                .id = 1,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
        // Spaces everywhere
        .{
            .string =
            \\
            \\
            \\      create_transfers
            \\            id =    1
            \\       user_data_128 = 12
            \\ debit_account_id=1 credit_account_id        = 10
            \\    ;
            \\
            \\
            ,
            .result = tb.Transfer{
                .id = 1,
                .debit_account_id = 1,
                .credit_account_id = 10,
                .amount = 0,
                .pending_id = 0,
                .user_data_128 = 12,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .timestamp = 0,
            },
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const statement = try Parser.parse_statement(
            vector.string,
            &null_terminal,
            &arguments,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(
            u8,
            statement.arguments.items,
            std.mem.asBytes(&vector.result),
        );
    }
}

test "parser.zig: Handle parsing errors" {
    const vectors = [_]struct {
        string: []const u8 = "",
        result: anyerror,
    }{
        .{
            .string = "create_trans",
            .result = error.OperationBad,
        },
        .{
            .string =
            \\
            \\
            \\ create
            ,
            .result = error.OperationBad,
        },
        .{
            .string = "create_transfers 12",
            .result = error.IdentifierBad,
        },
        .{
            .string = "create_transfers =12",
            .result = error.IdentifierBad,
        },
        .{
            .string = "create_transfers x",
            .result = error.KeyValuePairEqualMissing,
        },
        .{
            .string = "create_transfers x=",
            .result = error.ValueBad,
        },
        .{
            .string = "create_transfers x=    ",
            .result = error.ValueBad,
        },
        .{
            .string = "create_transfers x=    ;",
            .result = error.ValueBad,
        },
        .{
            .string = "create_transfers x=[]",
            .result = error.ValueBad,
        },
        .{
            .string = "create_transfers id=abcd",
            .result = error.KeyValuePairBad,
        },
        .{
            .string = "create_transfers amount=0y1234",
            .result = error.KeyValuePairBad,
        },
        .{
            .string = "create_transfers amount=--0",
            .result = error.KeyValuePairBad,
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const result = Parser.parse_statement(
            vector.string,
            &null_terminal,
            &arguments,
        );
        try std.testing.expectError(vector.result, result);
    }
}

test "parser.zig: Parser fails for operations not supporting multiple objects" {
    const vectors = [_]struct {
        string: []const u8,
        result: anyerror,
    }{
        .{
            .string = "get_account_transfers account_id=1, account_id=2",
            .result = error.SliceOperationUnsupported,
        },
        .{
            .string = "get_account_balances account_id=1, account_id=2",
            .result = error.SliceOperationUnsupported,
        },
        .{
            .string = "query_accounts account_id=1, account_id=2",
            .result = error.SliceOperationUnsupported,
        },
        .{
            .string = "query_transfers account_id=1, account_id=2",
            .result = error.SliceOperationUnsupported,
        },
    };

    for (vectors) |vector| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
            allocator,
            constants.message_size_max,
        );
        errdefer arguments.deinit(allocator);

        const result = Parser.parse_statement(
            vector.string,
            &null_terminal,
            &arguments,
        );

        try std.testing.expectError(vector.result, result);
    }
}
