const stdx = @import("stdx");
const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("../vsr.zig");
const constants = vsr.constants;
const IO = vsr.io.IO;
const Storage = vsr.storage.StorageType(IO);
const StateMachine = vsr.state_machine.StateMachineType(Storage);
const tb = vsr.tigerbeetle;

pub const Parser = struct {
    input: []const u8,
    offset: u32 = 0,
    stderr: std.io.AnyWriter,

    pub const ArgumentsList = std.ArrayListAlignedUnmanaged(u8, constants.cache_line_size);
    pub const Error = error{ParseError};

    pub const Command = enum {
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
        query_two_phase_transfers,

        pub fn operation(command: Command) StateMachine.Operation {
            return switch (command) {
                .none, .help => unreachable,
                .create_accounts => .create_accounts,
                .create_transfers => .create_transfers,
                .lookup_accounts => .lookup_accounts,
                .lookup_transfers => .lookup_transfers,
                .get_account_transfers => .get_account_transfers,
                .get_account_balances => .get_account_balances,
                .query_accounts => .query_accounts,
                .query_transfers => .query_transfers,
                .query_two_phase_transfers => .query_two_phase_transfers,
            };
        }
    };

    pub const LookupSyntaxTree = extern struct {
        id: u128,
    };

    pub const Statement = struct {
        command: Command,
        arguments: *ArgumentsList,
    };

    fn print_error(parser: *const Parser, comptime format: []const u8, arguments: anytype) !void {
        comptime assert(format.len > 0);
        comptime assert(format[format.len - 1] == '\n' or std.mem.eql(u8, format, " "));

        return parser.stderr.print(format, arguments);
    }

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

        try parser.print_error("Fail near line {}, column {}:\n\n{s}\n", .{
            target.position_line,
            target.position_column,
            target.line,
        });
        var column = target.position_column;
        while (column > 0) : (column -= 1) {
            try parser.print_error(" ", .{});
        }
        try parser.print_error("^ Near here.\n\n", .{});
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

        while (parser.offset < parser.input.len) : (parser.offset += 1) {
            const char_is_valid = switch (parser.input[parser.offset]) {
                // Identifiers can contain any letter and `_`.
                'A'...'Z', 'a'...'z', '_' => true,
                // It also may contain numbers, but not start with a number.
                '0'...'9' => parser.offset > after_whitespace,
                else => false,
            };

            if (!char_is_valid) break;
        }

        return parser.input[after_whitespace..parser.offset];
    }

    fn parse_syntax_char(parser: *Parser, syntax_char: u8) bool {
        parser.eat_whitespace();

        if (parser.offset < parser.input.len and
            parser.input[parser.offset] == syntax_char)
        {
            parser.offset += 1;
            return true;
        }

        return false;
    }

    fn parse_value(parser: *Parser) []const u8 {
        parser.eat_whitespace();
        const after_whitespace = parser.offset;

        while (parser.offset < parser.input.len) {
            switch (parser.input[parser.offset]) {
                '0'...'9',
                'A'...'Z',
                'a'...'z',
                '-',
                '_',
                => {
                    parser.offset += 1;
                },
                else => {
                    var copy = parser.*;
                    // Flags may have whitespace on either side of a '|'.
                    copy.eat_whitespace();
                    if (copy.parse_syntax_char('|')) {
                        copy.eat_whitespace();

                        assert(parser.offset < copy.offset);
                        parser.offset = copy.offset;
                        continue;
                    }
                    break;
                },
            }
        }

        return parser.input[after_whitespace..parser.offset];
    }

    fn object_update(
        parser: *Parser,
        comptime Object: type,
        object: *Object,
        comptime field: std.meta.FieldEnum(Object),
        value_string: []const u8,
    ) !void {
        const Value = @FieldType(Object, @tagName(field));
        if (@hasField(Object, "flags") and field == .flags) {
            var flags_strings = std.mem.splitScalar(u8, value_string, '|');
            var validated_flags = std.mem.zeroInit(Value, .{});
            while (flags_strings.next()) |flag_string| {
                const flag_to_validate_trimmed =
                    std.mem.trim(u8, flag_string, std.ascii.whitespace[0..]);

                flags: inline for (@typeInfo(Value).@"struct".fields) |known_flag_field| {
                    if (comptime std.mem.eql(
                        u8,
                        known_flag_field.name,
                        "padding",
                    )) comptime continue :flags;

                    const flag_value = &@field(validated_flags, known_flag_field.name);

                    // Bit-flags can be either bool or u1 enums.
                    switch (@typeInfo(known_flag_field.type)) {
                        .bool => if (std.mem.eql(
                            u8,
                            known_flag_field.name,
                            flag_to_validate_trimmed,
                        )) {
                            if (flag_value.*) {
                                try parser.print_current_position();
                                try parser.print_error(
                                    "Duplicate flag set: \"{s}\".\n",
                                    .{known_flag_field.name},
                                );
                                return error.ParseError;
                            }
                            flag_value.* = true;
                            break :flags;
                        },
                        .@"enum" => |info| {
                            comptime assert(info.tag_type == u1);
                            inline for (info.fields) |variant| {
                                if (std.mem.eql(
                                    u8,
                                    known_flag_field.name ++ "_" ++ variant.name,
                                    flag_to_validate_trimmed,
                                )) {
                                    flag_value.* = @enumFromInt(variant.value);
                                    break :flags;
                                }
                            }
                        },
                        else => comptime unreachable,
                    }
                } else {
                    try parser.print_current_position();
                    try parser.print_error(
                        "Invalid flag: \"{s}\".\n",
                        .{flag_to_validate_trimmed},
                    );
                    return error.ParseError;
                }
            }
            object.flags = validated_flags;
            return;
        }

        if (@hasField(Object, "reserved") and field == .reserved) {
            try parser.print_current_position();
            try parser.print_error("Unexpected key 'reserved'.\n", .{});
            return error.ParseError;
        }

        @field(object, @tagName(field)) = switch (@typeInfo(Value)) {
            .int => parse_int(Value, value_string) catch {
                try parser.print_current_position();
                try parser.print_error(
                    "Invalid value \"{s}\"; expected {s} for key \"{s}\".\n",
                    .{ value_string, @typeName(Value), @tagName(field) },
                );
                return error.ParseError;
            },
            .@"enum" => std.meta.stringToEnum(Value, value_string) orelse {
                try parser.print_current_position();
                try parser.print_error(
                    "Invalid enum \"{s}\"; expected {s} for key \"{s}\".\n",
                    .{ value_string, @typeName(Value), @tagName(field) },
                );
                return error.ParseError;
            },
            else => unreachable,
        };
    }

    // Allows 0b/0o/0x prefixes for UUIDs.
    // Allows -N as a shorthand for INT_MAX - N.
    fn parse_int(comptime T: type, input: []const u8) !T {
        const info = @typeInfo(T);
        comptime assert(info == .int);

        assert(input.len > 0);
        const input_negative = input[0] == '-';

        if (info.int.signedness == .unsigned and input_negative) {
            // Negative input means `maxInt - input`.
            // Useful for representing sentinels such as `AMOUNT_MAX`, as `-0`.
            const max = std.math.maxInt(T);
            return max - try stdx.parse_int_with_base(T, input[1..]);
        }

        return try stdx.parse_int_with_base(T, input);
    }

    fn parse_arguments(
        parser: *Parser,
        comptime command: Command,
        arguments: *ArgumentsList,
    ) !void {
        const default = object_default(command.operation());
        var object = default;

        const ObjectField = std.meta.FieldEnum(@TypeOf(object));
        var object_fields = std.enums.EnumSet(ObjectField).initEmpty();

        while (parser.offset < parser.input.len) {
            const offset_start = parser.offset;
            if (parser.parse_syntax_char(';')) break;

            // Expect comma separating objects.
            if (parser.parse_syntax_char(',')) {
                arguments.appendSliceAssumeCapacity(std.mem.asBytes(&object));

                const operation = command.operation();
                if (!operation.is_batchable()) {
                    try parser.print_current_position();
                    try parser.print_error(
                        "{s} expects a single object, but received multiple.\n",
                        .{@tagName(command)},
                    );
                    return error.ParseError;
                }

                // Reset object.
                object = default;
                object_fields = .initEmpty();
            }

            const field_string = parser.parse_identifier();
            if (field_string.len == 0) {
                try parser.print_current_position();
                try parser.print_error("Expected key starting key-value pair. e.g. `id=1`\n", .{});
                return error.ParseError;
            }

            const field = std.meta.stringToEnum(ObjectField, field_string) orelse {
                try parser.print_current_position();
                try parser.print_error("Unknown key: \"{s}\".\n", .{field_string});
                return error.ParseError;
            };
            if (object_fields.contains(field)) {
                try parser.print_current_position();
                try parser.print_error(
                    "Duplicate field {s} for single object. Separate objects with \",\".\n",
                    .{@tagName(field)},
                );
                return error.ParseError;
            }

            if (!parser.parse_syntax_char('=')) {
                try parser.print_current_position();
                try parser.print_error(
                    "Expected equal sign after key \"{s}\" in key-value pair. e.g. `id=1`.\n",
                    .{field_string},
                );
                return error.ParseError;
            }

            const value_string = parser.parse_value();
            if (value_string.len == 0) {
                try parser.print_current_position();
                try parser.print_error(
                    "Expected value after equal sign in key-value pair. e.g. `id=1`.\n",
                    .{},
                );
                return error.ParseError;
            }

            // Match key to a field in the struct.
            switch (field) {
                inline else => |field_comptime| try parser.object_update(
                    @TypeOf(object),
                    &object,
                    field_comptime,
                    value_string,
                ),
            }
            assert(!object_fields.contains(field));
            object_fields.insert(field);

            // Assert that the parser state advances, to guard against unbounded loops.
            assert(offset_start < parser.offset);
        }

        // Add final object.
        if (object_fields.count() > 0) {
            arguments.appendSliceAssumeCapacity(std.mem.asBytes(&object));
        }

        parser.eat_whitespace();
        if (parser.offset < parser.input.len) {
            try parser.print_current_position();
            try parser.print_error("unexpected statement\n", .{});
            return error.ParseError;
        }
    }

    // Statement grammar parsed here.
    // STATEMENT: COMMAND ARGUMENTS [;]
    // COMMAND: create_accounts | lookup_accounts | create_transfers | lookup_transfers | ...
    //      ARGUMENTS: ARG [, ARG]
    //       ARG: KEY = VALUE
    //       KEY: string
    //     VALUE: string [| VALUE]
    //
    // For example:
    //   create_accounts id=1 code=2 ledger=3, id = 2 code= 2 ledger =3;
    //   create_accounts flags=linked | debits_must_not_exceed_credits ;
    //
    // TODO(zig): Replace the (implicit) anyerror with a concrete (std.io.Writer.Error || Error).
    pub fn parse_statement(
        input: []const u8,
        stderr: std.io.AnyWriter,
        arguments: *ArgumentsList,
    ) !Statement {
        var parser = Parser{ .input = input, .stderr = stderr };
        parser.eat_whitespace();
        const after_whitespace = parser.offset;
        const operation_identifier = parser.parse_identifier();

        const command = command: {
            if (std.meta.stringToEnum(Command, operation_identifier)) |valid_operation| {
                break :command valid_operation;
            }

            if (operation_identifier.len == 0) {
                break :command .none;
            }

            // Set up the offset to after the whitespace so the
            // print_current_position function points at where we actually expected the token.
            parser.offset = after_whitespace;
            try parser.print_current_position();
            try parser.print_error(
                "Operation must be " ++
                    comptime operations: {
                        var names: []const u8 = "";
                        for (std.enums.values(Command), 0..) |command, index| {
                            if (command == .none) continue;
                            names = names ++
                                (if (names.len > 0) ", " else "") ++
                                (if (index == std.enums.values(Command).len - 1) "or " else "") ++
                                @tagName(command);
                        }
                        break :operations names;
                    } ++ ".\n",
                .{},
            );
            try parser.print_error("Got: \"{s}\".\n", .{operation_identifier});
            return error.ParseError;
        };

        switch (command) {
            .none => {},
            .help => {},
            inline else => |operation_comptime| {
                try parser.parse_arguments(operation_comptime, arguments);
                assert(parser.offset == parser.input.len);
            },
        }

        return .{
            .command = command,
            .arguments = arguments,
        };
    }
};

fn ObjectType(comptime operation: StateMachine.Operation) type {
    return switch (operation) {
        .lookup_accounts => Parser.LookupSyntaxTree,
        .lookup_transfers => Parser.LookupSyntaxTree,
        else => operation.EventType(),
    };
}

fn object_default(comptime operation: StateMachine.Operation) ObjectType(operation) {
    return switch (operation) {
        .create_accounts => std.mem.zeroInit(tb.Account, .{}),
        .create_transfers => std.mem.zeroInit(tb.Transfer, .{}),
        .lookup_accounts,
        .lookup_transfers,
        => Parser.LookupSyntaxTree{ .id = 0 },
        .get_account_transfers,
        .get_account_balances,
        => tb.AccountFilter{
            .account_id = 0,
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .code = 0,
            .timestamp_min = 0,
            .timestamp_max = 0,
            .limit = operation.result_max(constants.message_body_size_max),
            .flags = .{
                .credits = true,
                .debits = true,
                .reversed = false,
            },
        },
        .query_accounts,
        .query_transfers,
        => tb.QueryFilter{
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .ledger = 0,
            .code = 0,
            .timestamp_min = 0,
            .timestamp_max = 0,
            .limit = operation.result_max(constants.message_body_size_max),
            .flags = .{
                .reversed = false,
            },
        },
        .query_two_phase_transfers => tb.TwoPhaseFilter{
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .ledger = 0,
            .code = 0,
            .pending_status = .none,
            .timestamp_min = 0,
            .timestamp_max = 0,
            .limit = StateMachine.Operation.query_two_phase_transfers.result_max(
                constants.message_body_size_max,
            ),
            .flags = .{
                .target = .pending,
                .reversed = false,
            },
        },
        else => unreachable,
    };
}

test "Parser: fuzz" {
    const test_count = 1024;
    const input_size_max = 256;

    const commands = std.enums.values(Parser.Command);
    const fields: []const []const u8 = comptime fields: {
        var fields: []const []const u8 = &.{};
        for (.{ tb.Account, tb.Transfer, tb.AccountFilter, tb.QueryFilter }) |Object| {
            for (std.meta.fieldNames(Object)) |field| {
                fields = fields ++ [_][]const u8{field};
            }
        }
        break :fields fields;
    };

    var stderr = std.ArrayListUnmanaged(u8){};
    defer stderr.deinit(std.testing.allocator);

    var input = try std.ArrayListUnmanaged(u8).initCapacity(std.testing.allocator, input_size_max);
    defer input.deinit(std.testing.allocator);

    var body =
        try Parser.ArgumentsList.initCapacity(std.testing.allocator, constants.message_size_max);
    defer body.deinit(std.testing.allocator);

    var prng = stdx.PRNG.from_seed_testing();
    var error_count: u32 = 0;
    for (0..test_count) |_| {
        defer stderr.clearRetainingCapacity();
        defer input.clearRetainingCapacity();
        defer body.clearRetainingCapacity();

        const input_size_target = prng.int_inclusive(u32, input_size_max / 3);
        const separator_probability = stdx.PRNG.ratio(1, 4);

        input.appendSliceAssumeCapacity(@tagName(commands[prng.index(commands)]));
        input.appendAssumeCapacity(' ');

        while (input.items.len < input_size_target) {
            input.appendSliceAssumeCapacity(fields[prng.index(fields)]);
            input.appendAssumeCapacity('=');

            const value_powers = [_]u8{ 0, 8, 16, 32, 64, 128 };
            const value_power = value_powers[prng.index(value_powers)];
            const value: u256 = if (prng.boolean())
                prng.int_inclusive(u256, (@as(u256, 1) << value_power) - 1)
            else
                (@as(u256, 1) << value_power) - @intFromBool(prng.boolean());

            _ = switch (prng.enum_uniform(enum { hex, dec, oct, bin })) {
                .dec => input.writer(std.testing.allocator).print("{d}", .{value}),
                .hex => input.writer(std.testing.allocator).print("0x{x}", .{value}),
                .oct => input.writer(std.testing.allocator).print("0o{o}", .{value}),
                .bin => input.writer(std.testing.allocator).print("0b{b}", .{value}),
            } catch unreachable;

            input.appendAssumeCapacity(' ');
            if (prng.chance(separator_probability)) input.appendAssumeCapacity(',');
        }

        if (prng.boolean()) {
            const alphabet = "\n\t _|-=;,01aA";
            input.items[prng.index(input.items)] = alphabet[prng.index(alphabet)];
        }

        const stderr_writer = stderr.writer(std.testing.allocator);
        _ = Parser.parse_statement(input.items, stderr_writer.any(), &body) catch {
            error_count += 1;
        };
    }
    // Verify that we are testing both failures and successes.
    assert(error_count > 0);
    assert(error_count < test_count);
}

test "Parser: snap" {
    const snap = stdx.Snap.snap_fn("src");

    const T = struct {
        body: Parser.ArgumentsList,
        body_formatted: std.ArrayListUnmanaged(u8),
        stderr: std.ArrayListUnmanaged(u8),

        fn check(t: *@This(), string: []const u8, want: stdx.Snap) !void {
            assert(t.body.items.len == 0);
            assert(t.body_formatted.items.len == 0);
            assert(t.stderr.items.len == 0);
            defer t.body.clearRetainingCapacity();
            defer t.body_formatted.clearRetainingCapacity();
            defer t.stderr.clearRetainingCapacity();

            try t.body.ensureTotalCapacity(std.testing.allocator, constants.message_size_max);

            const stderr_writer = t.stderr.writer(std.testing.allocator);
            const statement = Parser.parse_statement(string, stderr_writer.any(), &t.body) catch {
                try want.diff(t.stderr.items);
                return;
            };

            switch (statement.command) {
                .none => {},
                .help => {},
                inline else => |command| try t.print_objects(
                    command.operation(),
                    @alignCast(std.mem.bytesAsSlice(
                        ObjectType(command.operation()),
                        t.body.items,
                    )),
                ),
            }
            try want.diff(t.body_formatted.items);
        }

        fn print_objects(
            t: *@This(),
            comptime operation: StateMachine.Operation,
            objects: []const ObjectType(operation),
        ) !void {
            const body_formatted_writer = t.body_formatted.writer(std.testing.allocator);
            try body_formatted_writer.print("{s}", .{@tagName(operation)});
            if (objects.len > 1) try t.body_formatted.append(std.testing.allocator, '\n');

            const Object = ObjectType(operation);
            for (objects, 0..) |*object, i| {
                if (i > 0) try t.body_formatted.append(std.testing.allocator, '\n');
                inline for (std.meta.fields(Object)) |field| {
                    const value = @field(object, field.name);

                    if (stdx.zeroed(std.mem.asBytes(&value))) {
                        // Omit zeroed fields for readability.
                    } else {
                        if (comptime std.mem.eql(u8, field.name, "flags")) {
                            try body_formatted_writer.print(" flags=", .{});
                            var separate = false;
                            inline for (std.meta.fields(field.type)) |flag| {
                                const flag_value = @field(value, flag.name);
                                if (comptime std.mem.eql(u8, flag.name, "padding")) {
                                    assert(flag_value == 0);
                                } else {
                                    const Flag = @TypeOf(flag_value);
                                    switch (@typeInfo(Flag)) {
                                        .bool => if (flag_value) {
                                            if (separate) try body_formatted_writer.print("|", .{});
                                            separate = true;
                                            try body_formatted_writer.print("{s}", .{flag.name});
                                        },
                                        .@"enum" => |info| {
                                            comptime assert(info.tag_type == u1);
                                            if (separate) try body_formatted_writer.print("|", .{});
                                            separate = true;
                                            try body_formatted_writer.print(
                                                "{s}_{s}",
                                                .{ flag.name, @tagName(flag_value) },
                                            );
                                        },
                                        else => unreachable,
                                    }
                                }
                            }
                        } else {
                            switch (@typeInfo(field.type)) {
                                .int => try body_formatted_writer.print(
                                    " {s}={}",
                                    .{ field.name, value },
                                ),
                                .array => |info| {
                                    comptime assert(info.child == u8);
                                    try body_formatted_writer.print(
                                        " {s}={s}",
                                        .{ field.name, value },
                                    );
                                },
                                .@"enum" => try body_formatted_writer.print(
                                    " {s}={s}",
                                    .{ field.name, @tagName(value) },
                                ),
                                else => comptime unreachable,
                            }
                        }
                    }
                }
            }
        }
    };

    var t = T{ .body = .{}, .body_formatted = .{}, .stderr = .{} };
    defer t.stderr.deinit(std.testing.allocator);
    defer t.body_formatted.deinit(std.testing.allocator);
    defer t.body.deinit(std.testing.allocator);

    // create_transfers
    try t.check("create_transfers id=1", snap(@src(),
        \\create_transfers id=1
    ));
    try t.check("create_transfers timestamp=1", snap(@src(),
        \\create_transfers timestamp=1
    ));
    try t.check(
        \\create_transfers id=32 amount=65 ledger=12 code=9999 pending_id=7
        \\ credit_account_id=2121 debit_account_id=77 user_data_128=2
        \\ user_data_64=3 user_data_32=4 flags=linked
    , snap(@src(),
        \\create_transfers id=32 debit_account_id=77 credit_account_id=2121 amount=65 pending_id=7 user_data_128=2 user_data_64=3 user_data_32=4 ledger=12 code=9999 flags=linked
    ));
    try t.check(
        \\create_transfers flags=
        \\ pending | post_pending_transfer | void_pending_transfer |
        \\ balancing_credit | balancing_debit | linked
    , snap(@src(),
        \\create_transfers flags=linked|pending|post_pending_transfer|void_pending_transfer|balancing_debit|balancing_credit
    ));
    try t.check("create_transfers amount=-0", snap(@src(),
        \\create_transfers amount=340282366920938463463374607431768211455
    ));
    try t.check("create_transfers amount=-1", snap(@src(),
        \\create_transfers amount=340282366920938463463374607431768211454
    ));
    try t.check("create_transfers amount=0xbee71e", snap(@src(),
        \\create_transfers amount=12511006
    ));
    try t.check("create_transfers amount=1_000_000", snap(@src(),
        \\create_transfers amount=1000000
    ));
    try t.check("create_transfers id=0xa1a2a3a4_b1b2_c1c2_d1d2_e1e2e3e4e5e6", snap(@src(),
        \\create_transfers id=214850178493633095719753766415838275046
    ));
    try t.check(
        "create_transfers id=1 debit_account_id=2, id=2 credit_account_id = 1;",
        snap(@src(),
            \\create_transfers
            \\ id=1 debit_account_id=2
            \\ id=2 credit_account_id=1
        ),
    );

    // create_accounts
    try t.check("create_accounts id=1", snap(@src(),
        \\create_accounts id=1
    ));
    try t.check("create_accounts id=1", snap(@src(),
        \\create_accounts id=1
    ));
    try t.check(
        \\create_accounts id=32 credits_posted=344 ledger=12 credits_pending=18
        \\ code=9999 flags=linked | debits_must_not_exceed_credits debits_posted=3390
        \\ debits_pending=3212 user_data_128=2 user_data_64=3 user_data_32=4
    , snap(@src(),
        \\create_accounts id=32 debits_pending=3212 debits_posted=3390 credits_pending=18 credits_posted=344 user_data_128=2 user_data_64=3 user_data_32=4 ledger=12 code=9999 flags=linked|debits_must_not_exceed_credits
    ));
    try t.check(
        \\create_accounts flags=credits_must_not_exceed_debits|
        \\ linked|debits_must_not_exceed_credits id =1
    , snap(@src(),
        \\create_accounts id=1 flags=linked|debits_must_not_exceed_credits|credits_must_not_exceed_debits
    ));

    // get_account_transfers/get_account_balances
    try t.check("get_account_transfers account_id=1", snap(@src(),
        \\get_account_transfers account_id=1 limit=29 flags=debits|credits
    ));
    try t.check(
        \\get_account_balances account_id=1000
        \\user_data_128=128 user_data_64=64 user_data_32=32
        \\code=2
        \\flags=debits|reversed limit=10
        \\timestamp_min=1 timestamp_max=9999;
    , snap(@src(),
        \\get_account_balances account_id=1000 user_data_128=128 user_data_64=64 user_data_32=32 code=2 timestamp_min=1 timestamp_max=9999 limit=10 flags=debits|reversed
    ));

    // query_accounts/query_transfers
    try t.check("query_transfers user_data_128=1", snap(@src(),
        \\query_transfers user_data_128=1 limit=29
    ));
    try t.check(
        \\query_accounts user_data_128=1000
        \\user_data_64=100 user_data_32=10
        \\ledger=1 code=2
        \\flags=reversed limit=10
        \\timestamp_min=1 timestamp_max=9999;
    , snap(@src(),
        \\query_accounts user_data_128=1000 user_data_64=100 user_data_32=10 ledger=1 code=2 timestamp_min=1 timestamp_max=9999 limit=10 flags=reversed
    ));

    // query_two_phase_transfers
    try t.check("query_two_phase_transfers user_data_128=1", snap(@src(),
        \\query_two_phase_transfers user_data_128=1 limit=14
    ));
    try t.check(
        \\query_two_phase_transfers
        \\user_data_128=128 user_data_64=64 user_data_32=32
        \\ledger=1 code=2
        \\pending_status=expired
        \\flags=target_outcome|reversed limit=10
        \\timestamp_min=1 timestamp_max=9999;
        \\
    , snap(@src(),
        \\query_two_phase_transfers user_data_128=128 user_data_64=64 user_data_32=32 ledger=1 code=2 pending_status=expired timestamp_min=1 timestamp_max=9999 limit=10 flags=target_outcome|reversed
    ));

    // Unusual formatting.
    try t.check("create_transfers id = 1", snap(@src(),
        \\create_transfers id=1
    ));
    try t.check("create_transfers id =1", snap(@src(),
        \\create_transfers id=1
    ));
    try t.check("  \t  \n  create_transfers id=1", snap(@src(),
        \\create_transfers id=1
    ));
    try t.check("create_transfers id=1;", snap(@src(),
        \\create_transfers id=1
    ));
    try t.check("create_transfers id=1 , id=2", snap(@src(),
        \\create_transfers
        \\ id=1
        \\ id=2
    ));
    try t.check("create_transfers id=1 ,id=2", snap(@src(),
        \\create_transfers
        \\ id=1
        \\ id=2
    ));
    try t.check(
        \\
        \\
        \\      create_transfers
        \\            id =    1
        \\       user_data_128 = 12
        \\ debit_account_id=1 credit_account_id        = 10
        \\    ;
        \\
    , snap(@src(),
        \\create_transfers id=1 debit_account_id=1 credit_account_id=10 user_data_128=12
    ));

    // Errors
    try t.check("create_trans", snap(@src(),
        \\Fail near line 1, column 0:
        \\
        \\create_trans
        \\^ Near here.
        \\
        \\Operation must be help, create_accounts, create_transfers, lookup_accounts, lookup_transfers, get_account_transfers, get_account_balances, query_accounts, query_transfers, or query_two_phase_transfers.
        \\Got: "create_trans".
        \\
    ));
    try t.check(
        \\
        \\
        \\ create
    , snap(@src(),
        \\Fail near line 3, column 1:
        \\
        \\ create
        \\ ^ Near here.
        \\
        \\Operation must be help, create_accounts, create_transfers, lookup_accounts, lookup_transfers, get_account_transfers, get_account_balances, query_accounts, query_transfers, or query_two_phase_transfers.
        \\Got: "create".
        \\
    ));
    try t.check("create_transfers 12", snap(@src(),
        \\Fail near line 1, column 17:
        \\
        \\create_transfers 12
        \\                 ^ Near here.
        \\
        \\Expected key starting key-value pair. e.g. `id=1`
        \\
    ));
    try t.check("create_transfers =12", snap(@src(),
        \\Fail near line 1, column 17:
        \\
        \\create_transfers =12
        \\                 ^ Near here.
        \\
        \\Expected key starting key-value pair. e.g. `id=1`
        \\
    ));
    try t.check("create_transfers id", snap(@src(),
        \\Fail near line 1, column 19:
        \\
        \\create_transfers id
        \\                   ^ Near here.
        \\
        \\Expected equal sign after key "id" in key-value pair. e.g. `id=1`.
        \\
    ));
    try t.check("create_transfers id=", snap(@src(),
        \\Fail near line 1, column 20:
        \\
        \\create_transfers id=
        \\                    ^ Near here.
        \\
        \\Expected value after equal sign in key-value pair. e.g. `id=1`.
        \\
    ));
    try t.check("create_transfers id=    ", snap(@src(),
        \\Fail near line 1, column 24:
        \\
        \\create_transfers id=    
        \\                        ^ Near here.
        \\
        \\Expected value after equal sign in key-value pair. e.g. `id=1`.
        \\
    ));
    try t.check("create_transfers id=    ;", snap(@src(),
        \\Fail near line 1, column 24:
        \\
        \\create_transfers id=    ;
        \\                        ^ Near here.
        \\
        \\Expected value after equal sign in key-value pair. e.g. `id=1`.
        \\
    ));
    try t.check("create_transfers id=[]", snap(@src(),
        \\Fail near line 1, column 20:
        \\
        \\create_transfers id=[]
        \\                    ^ Near here.
        \\
        \\Expected value after equal sign in key-value pair. e.g. `id=1`.
        \\
    ));
    try t.check("create_transfers id=abcd", snap(@src(),
        \\Fail near line 1, column 24:
        \\
        \\create_transfers id=abcd
        \\                        ^ Near here.
        \\
        \\Invalid value "abcd"; expected u128 for key "id".
        \\
    ));
    try t.check("create_transfers amount=0y1234", snap(@src(),
        \\Fail near line 1, column 30:
        \\
        \\create_transfers amount=0y1234
        \\                              ^ Near here.
        \\
        \\Invalid value "0y1234"; expected u128 for key "amount".
        \\
    ));
    try t.check("create_transfers amount=--0", snap(@src(),
        \\Fail near line 1, column 27:
        \\
        \\create_transfers amount=--0
        \\                           ^ Near here.
        \\
        \\Invalid value "--0"; expected u128 for key "amount".
        \\
    ));
    try t.check("create_transfers id=1 id=2", snap(@src(),
        \\Fail near line 1, column 24:
        \\
        \\create_transfers id=1 id=2
        \\                        ^ Near here.
        \\
        \\Duplicate field id for single object. Separate objects with ",".
        \\
    ));
    try t.check("create_transfers id=1; id=2", snap(@src(),
        \\Fail near line 1, column 23:
        \\
        \\create_transfers id=1; id=2
        \\                       ^ Near here.
        \\
        \\unexpected statement
        \\
    ));
    try t.check("create_transfers idd=1", snap(@src(),
        \\Fail near line 1, column 20:
        \\
        \\create_transfers idd=1
        \\                    ^ Near here.
        \\
        \\Unknown key: "idd".
        \\
    ));
    try t.check("query_accounts user_data_128=1|,", snap(@src(),
        \\Fail near line 1, column 31:
        \\
        \\query_accounts user_data_128=1|,
        \\                               ^ Near here.
        \\
        \\Invalid value "1|"; expected u128 for key "user_data_128".
        \\
    ));
    try t.check("query_accounts user_data_128=1||", snap(@src(),
        \\Fail near line 1, column 32:
        \\
        \\query_accounts user_data_128=1||
        \\                                ^ Near here.
        \\
        \\Invalid value "1||"; expected u128 for key "user_data_128".
        \\
    ));

    // Operations not supporting multiple objects:
    try t.check("get_account_transfers account_id=1, account_id=2", snap(@src(),
        \\Fail near line 1, column 35:
        \\
        \\get_account_transfers account_id=1, account_id=2
        \\                                   ^ Near here.
        \\
        \\get_account_transfers expects a single object, but received multiple.
        \\
    ));
    try t.check("get_account_balances account_id=1, account_id=2", snap(@src(),
        \\Fail near line 1, column 34:
        \\
        \\get_account_balances account_id=1, account_id=2
        \\                                  ^ Near here.
        \\
        \\get_account_balances expects a single object, but received multiple.
        \\
    ));
    try t.check("query_accounts code=1, code=2", snap(@src(),
        \\Fail near line 1, column 22:
        \\
        \\query_accounts code=1, code=2
        \\                      ^ Near here.
        \\
        \\query_accounts expects a single object, but received multiple.
        \\
    ));
    try t.check("query_transfers code=1, code=2", snap(@src(),
        \\Fail near line 1, column 23:
        \\
        \\query_transfers code=1, code=2
        \\                       ^ Near here.
        \\
        \\query_transfers expects a single object, but received multiple.
        \\
    ));
    try t.check("query_two_phase_transfers code=1, code=2", snap(@src(),
        \\Fail near line 1, column 33:
        \\
        \\query_two_phase_transfers code=1, code=2
        \\                                 ^ Near here.
        \\
        \\query_two_phase_transfers expects a single object, but received multiple.
        \\
    ));

    try t.check("create_transfers flags=foo", snap(@src(),
        \\Fail near line 1, column 26:
        \\
        \\create_transfers flags=foo
        \\                          ^ Near here.
        \\
        \\Invalid flag: "foo".
        \\
    ));
    try t.check("create_transfers flags=linked|foo", snap(@src(),
        \\Fail near line 1, column 33:
        \\
        \\create_transfers flags=linked|foo
        \\                                 ^ Near here.
        \\
        \\Invalid flag: "foo".
        \\
    ));
    try t.check("create_transfers flags=linked|linked", snap(@src(),
        \\Fail near line 1, column 36:
        \\
        \\create_transfers flags=linked|linked
        \\                                    ^ Near here.
        \\
        \\Duplicate flag set: "linked".
        \\
    ));
}
