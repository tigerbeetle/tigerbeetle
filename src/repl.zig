const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const posix = std.posix;
const windows = std.os.windows;

const vsr = @import("vsr.zig");
const stdx = vsr.stdx;
const constants = vsr.constants;
const IO = vsr.io.IO;
const Storage = vsr.storage.Storage(IO);
const StateMachine = vsr.state_machine.StateMachineType(
    Storage,
    constants.state_machine_config,
);
const MessagePool = vsr.message_pool.MessagePool;

const tb = vsr.tigerbeetle;

// Printer is separate from logging since messages from the REPL
// aren't logs but actual messages for the user. The fields are made
// optional so that printing on failure can be disabled in tests that
// test for failure.
pub const Printer = struct {
    stdout: ?std.fs.File.Writer,
    stderr: ?std.fs.File.Writer,

    fn print(printer: Printer, comptime format: []const u8, arguments: anytype) !void {
        if (printer.stdout) |stdout| {
            try stdout.print(format, arguments);
        }
    }

    fn print_error(printer: Printer, comptime format: []const u8, arguments: anytype) !void {
        if (printer.stderr) |stderr| {
            try stderr.print(format, arguments);
        }
    }
};

pub const Parser = struct {
    input: []const u8,
    offset: usize = 0,
    printer: Printer,

    pub const Error = error{
        BadKeyValuePair,
        BadValue,
        BadOperation,
        BadIdentifier,
        MissingEqualBetweenKeyValuePair,
        NoSyntaxMatch,
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
        arguments: []const u8,
    };

    fn print_current_position(parser: *const Parser) !void {
        const target = target: {
            var position_cursor: usize = 0;
            var position_line: usize = 1;
            var lines = std.mem.split(u8, parser.input, "\n");
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

        try parser.printer.print_error("Fail near line {}, column {}:\n\n{s}\n", .{
            target.position_line,
            target.position_column,
            target.line,
        });
        var column = target.position_column;
        while (column > 0) {
            try parser.printer.print_error(" ", .{});
            column -= 1;
        }
        try parser.printer.print_error("^ Near here.\n\n", .{});
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

        return Error.NoSyntaxMatch;
    }

    fn parse_value(parser: *Parser) []const u8 {
        parser.eat_whitespace();
        const after_whitespace = parser.offset;

        while (parser.offset < parser.input.len) {
            const c = parser.input[parser.offset];
            if (!(std.ascii.isAlphanumeric(c) or c == '_' or c == '|')) {
                // Allows flag fields to have whitespace before a '|'.
                var copy = Parser{
                    .input = parser.input,
                    .offset = parser.offset,
                    .printer = parser.printer,
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
        inline for (@typeInfo(ObjectSyntaxTree).Union.fields) |object_syntax_tree_field| {
            if (std.mem.eql(u8, @tagName(out.*), object_syntax_tree_field.name)) {
                const active_value = @field(out, object_syntax_tree_field.name);
                const ActiveValue = @TypeOf(active_value);

                inline for (@typeInfo(ActiveValue).Struct.fields) |active_value_field| {
                    if (std.mem.eql(u8, active_value_field.name, key_to_validate)) {
                        // Handle everything but flags, skip reserved and timestamp.
                        if (comptime (!std.mem.eql(u8, active_value_field.name, "flags") and
                            !std.mem.eql(u8, active_value_field.name, "reserved") and
                            !std.mem.eql(u8, active_value_field.name, "timestamp")))
                        {
                            @field(
                                @field(out.*, object_syntax_tree_field.name),
                                active_value_field.name,
                            ) = try std.fmt.parseInt(
                                active_value_field.type,
                                value_to_validate,
                                10,
                            );
                        }

                        // Handle flags, specific to Account and Transfer fields.
                        if (comptime std.mem.eql(u8, active_value_field.name, "flags") and
                            @hasField(ActiveValue, "flags"))
                        {
                            var flags_to_validate = std.mem.split(u8, value_to_validate, "|");
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
                                ).Struct.fields) |known_flag_field| {
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

    fn parse_arguments(
        parser: *Parser,
        operation: Operation,
        arguments: *std.ArrayList(u8),
    ) !void {
        const default: ObjectSyntaxTree = switch (operation) {
            .help, .none => return,
            .create_accounts => .{ .account = std.mem.zeroInit(tb.Account, .{}) },
            .create_transfers => .{ .transfer = std.mem.zeroInit(tb.Transfer, .{}) },
            .lookup_accounts, .lookup_transfers => .{ .id = .{ .id = 0 } },
            .get_account_transfers, .get_account_balances => .{ .account_filter = tb.AccountFilter{
                .account_id = 0,
                .timestamp_min = 0,
                .timestamp_max = 0,
                .limit = switch (operation) {
                    .get_account_transfers => StateMachine.constants
                        .batch_max.get_account_transfers,
                    .get_account_balances => StateMachine.constants
                        .batch_max.get_account_balances,
                    else => unreachable,
                },
                .flags = .{
                    .credits = true,
                    .debits = true,
                    .reversed = false,
                },
            } },
            .query_accounts, .query_transfers => .{ .query_filter = tb.QueryFilter{
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .ledger = 0,
                .code = 0,
                .timestamp_min = 0,
                .timestamp_max = 0,
                .limit = switch (operation) {
                    .query_accounts => StateMachine.constants
                        .batch_max.query_accounts,
                    .query_transfers => StateMachine.constants
                        .batch_max.query_transfers,
                    else => unreachable,
                },
                .flags = .{
                    .reversed = false,
                },
            } },
        };
        var object = default;

        var object_has_fields = false;
        while (parser.offset < parser.input.len) {
            parser.eat_whitespace();
            // Always need to check `i` against length in case we've hit the end.
            if (parser.offset >= parser.input.len or parser.input[parser.offset] == ';') {
                break;
            }

            // Expect comma separating objects.
            // TODO: Not all operations allow multiple objects, e.g. get_account_transfers.
            if (parser.offset < parser.input.len and parser.input[parser.offset] == ',') {
                parser.offset += 1;
                inline for (@typeInfo(ObjectSyntaxTree).Union.fields) |object_tree_field| {
                    if (std.mem.eql(u8, @tagName(object), object_tree_field.name)) {
                        const unwrapped_field = @field(object, object_tree_field.name);
                        try arguments.appendSlice(std.mem.asBytes(&unwrapped_field));
                    }
                }

                // Reset object.
                object = default;
                object_has_fields = false;
            }

            // Grab key.
            const id_result = parser.parse_identifier();

            if (id_result.len == 0) {
                try parser.print_current_position();
                try parser.printer.print_error(
                    "Expected key starting key-value pair. e.g. `id=1`\n",
                    .{},
                );
                return Error.BadIdentifier;
            }

            // Grab =.
            parser.parse_syntax_char('=') catch {
                try parser.print_current_position();
                try parser.printer.print_error(
                    "Expected equal sign after key '{s}' in key-value" ++
                        " pair. e.g. `id=1`.\n",
                    .{id_result},
                );
                return Error.MissingEqualBetweenKeyValuePair;
            };

            // Grab value.
            const value_result = parser.parse_value();

            if (value_result.len == 0) {
                try parser.print_current_position();
                try parser.printer.print_error(
                    "Expected value after equal sign in key-value pair. e.g. `id=1`.\n",
                    .{},
                );
                return Error.BadValue;
            }

            // Match key to a field in the struct.
            match_arg(&object, id_result, value_result) catch {
                try parser.print_current_position();
                try parser.printer.print_error(
                    "'{s}'='{s}' is not a valid pair for {s}.\n",
                    .{ id_result, value_result, @tagName(object) },
                );
                return Error.BadKeyValuePair;
            };

            object_has_fields = true;
        }

        // Add final object.
        if (object_has_fields) {
            inline for (@typeInfo(ObjectSyntaxTree).Union.fields) |object_tree_field| {
                if (std.mem.eql(u8, @tagName(object), object_tree_field.name)) {
                    const unwrapped_field = @field(object, object_tree_field.name);
                    try arguments.appendSlice(std.mem.asBytes(&unwrapped_field));
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
        arena: *std.heap.ArenaAllocator,
        input: []const u8,
        printer: Printer,
    ) (error{OutOfMemory} || std.fs.File.WriteError || Error)!Statement {
        var parser = Parser{ .input = input, .printer = printer };
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
            try parser.printer.print_error(
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
            return Error.BadOperation;
        };

        var arguments = std.ArrayList(u8).init(arena.allocator());
        try parser.parse_arguments(operation, &arguments);

        return Statement{
            .operation = operation,
            .arguments = arguments.items,
        };
    }
};

pub fn ReplType(comptime MessageBus: type) type {
    const Client = vsr.Client(StateMachine, MessageBus);

    return struct {
        event_loop_done: bool,
        request_done: bool,

        interactive: bool,
        debug_logs: bool,

        starting_terminal_mode: *const anyopaque,
        client: *Client,
        printer: Printer,
        history: std.ArrayList([]const u8),

        const Repl = @This();

        fn fail(repl: *const Repl, comptime format: []const u8, arguments: anytype) !void {
            if (!repl.interactive) {
                try repl.printer.print_error(format, arguments);
                std.process.exit(1);
            }

            try repl.printer.print(format, arguments);
        }

        fn debug(repl: *const Repl, comptime format: []const u8, arguments: anytype) !void {
            if (repl.debug_logs) {
                try repl.printer.print("[Debug] " ++ format, arguments);
            }
        }

        fn do_statement(
            repl: *Repl,
            statement: Parser.Statement,
        ) !void {
            try repl.debug("Running command: {}.\n", .{statement.operation});
            switch (statement.operation) {
                .none => {
                    // No input was parsed.
                    try repl.debug("No command was parsed, continuing.\n", .{});
                },
                .help => {
                    try repl.display_help();
                },

                .create_accounts,
                .create_transfers,
                .lookup_accounts,
                .lookup_transfers,
                .get_account_transfers,
                .get_account_balances,
                .query_accounts,
                .query_transfers,
                => |operation| {
                    const state_machine_operation =
                        std.meta.stringToEnum(StateMachine.Operation, @tagName(operation));
                    assert(state_machine_operation != null);

                    try repl.send(state_machine_operation.?, statement.arguments);
                },
            }
        }

        const prompt = "> ";
        const single_repl_input_max = 10 * 4 * 1024;

        fn read_until_newline_or_eof_alloc(
            repl: *Repl,
            allocator: std.mem.Allocator,
            reader: std.io.AnyReader,
        ) !?[]u8 {
            try repl.prompt_mode_set();
            defer repl.prompt_mode_unset() catch {};

            var terminal = try repl.get_terminal(allocator, reader);

            var buffer = std.ArrayList(u8).init(allocator);
            var buffer_index: usize = 0;
            var history_index = repl.history.items.len;
            // Cache the buffer the user is typing into before navigating through history.
            var buffer_outside_history = std.ArrayList(u8).init(allocator);

            while (buffer.items.len < single_repl_input_max) {
                const user_input = try UserInput.read(reader) orelse return null;
                switch (user_input) {
                    .newline => {
                        try repl.printer.print("\n", .{});
                        return try buffer.toOwnedSlice();
                    },
                    .printable => |character| {
                        const is_append = buffer_index == buffer.items.len;
                        if (is_append) {
                            try repl.printer.print("{c}{s}", .{
                                character,
                                // Some terminals may not automatically move/scroll us down to the
                                // next row after appending a character at the last column. This can
                                // cause us to incorrectly report the cursor's position, so we force
                                // the terminal to do so by moving the cursor forward and backward
                                // one space.
                                if (terminal.cursor_position.column == terminal.size.columns)
                                    "\x20\x08"
                                else
                                    "",
                            });
                            terminal.cursor_position = terminal.cursor_position_after_delta(1);
                        } else {
                            // If we're inserting mid-buffer, we need to redraw everything that
                            // comes after as well.
                            const buffer_redraw_len: isize = @intCast(
                                buffer.items.len - buffer_index,
                            );
                            const position_buffer_end = terminal.cursor_position_after_delta(
                                buffer_redraw_len,
                            );
                            const position_after_new_character = position_buffer_end.after_delta(
                                -(buffer_redraw_len - 1),
                                terminal.size,
                            );

                            try repl.printer.print("{c}{s}\x1b[{};{}H", .{
                                character,
                                buffer.items[buffer_index..],
                                position_after_new_character.row,
                                position_after_new_character.column,
                            });
                            terminal.cursor_position = position_after_new_character;
                        }

                        try buffer.insert(buffer_index, character);
                        buffer_index += 1;
                    },
                    .backspace => if (buffer_index > 0) {
                        terminal.cursor_position = terminal.cursor_position_after_delta(-1);
                        try repl.printer.print("\x08{s}\x20\x1b[{};{}H", .{
                            // If we're deleting mid-buffer, we need to redraw everything that
                            // comes after as well.
                            if (buffer_index < buffer.items.len)
                                buffer.items[buffer_index..]
                            else
                                "",
                            terminal.cursor_position.row,
                            terminal.cursor_position.column,
                        });
                        buffer_index -= 1;
                        _ = buffer.orderedRemove(buffer_index);
                    },
                    .left => if (buffer_index > 0) {
                        try repl.printer.print("\x08", .{});
                        terminal.cursor_position = terminal.cursor_position_after_delta(-1);
                        buffer_index -= 1;
                    },
                    .right => if (buffer_index < buffer.items.len) {
                        terminal.cursor_position = terminal.cursor_position_after_delta(1);
                        try repl.printer.print("\x1b[{};{}H", .{
                            terminal.cursor_position.row,
                            terminal.cursor_position.column,
                        });
                        buffer_index += 1;
                    },
                    .up => if (history_index > 0) {
                        const history_index_next = history_index - 1;
                        const buffer_next = repl.history.items[history_index_next];

                        const position_buffer_start = terminal.cursor_position_after_delta(
                            -@as(isize, @intCast(buffer_index)),
                        );
                        const position_buffer_end = position_buffer_start.after_delta(
                            @as(isize, @intCast(buffer_next.len)),
                            terminal.size,
                        );

                        try repl.printer.print("\x1b[{};1H\x1b[J{s}{s}\x20\x08", .{
                            position_buffer_start.row,
                            prompt,
                            buffer_next,
                        });
                        terminal.cursor_position = position_buffer_end;

                        if (history_index == repl.history.items.len) {
                            buffer_outside_history.clearRetainingCapacity();
                            try buffer_outside_history.appendSlice(buffer.items);
                        }
                        history_index = history_index_next;

                        buffer.clearRetainingCapacity();
                        try buffer.appendSlice(buffer_next);
                        buffer_index = buffer.items.len;
                    },
                    .down => if (history_index < repl.history.items.len) {
                        const history_index_next = history_index + 1;
                        const buffer_next = if (history_index_next == repl.history.items.len)
                            buffer_outside_history.items
                        else
                            repl.history.items[history_index_next];

                        const position_buffer_start = terminal.cursor_position_after_delta(
                            -@as(isize, @intCast(buffer_index)),
                        );
                        const position_buffer_end = position_buffer_start.after_delta(
                            @as(isize, @intCast(buffer_next.len)),
                            terminal.size,
                        );

                        try repl.printer.print("\x1b[{};1H\x1b[J{s}{s}\x20\x08", .{
                            position_buffer_start.row,
                            prompt,
                            buffer_next,
                        });
                        terminal.cursor_position = position_buffer_end;

                        history_index = history_index_next;

                        buffer.clearRetainingCapacity();
                        try buffer.appendSlice(buffer_next);
                        buffer_index = buffer.items.len;
                    },
                    .unhandled => {},
                }
            }
            return error.StreamTooLong;
        }

        fn do_repl(
            repl: *Repl,
            arena: *std.heap.ArenaAllocator,
        ) !void {
            try repl.printer.print(prompt, .{});

            const stdin = std.io.getStdIn();
            var stdin_buffered_reader = std.io.bufferedReader(stdin.reader());
            const stdin_stream = stdin_buffered_reader.reader().any();

            const input = repl.read_until_newline_or_eof_alloc(
                arena.allocator(),
                stdin_stream,
            ) catch |err| {
                repl.event_loop_done = true;
                return err;
            } orelse {
                // EOF.
                repl.event_loop_done = true;
                try repl.fail("\nExiting.\n", .{});
                return;
            };

            if (input.len > 0) {
                if (repl.history.items.len == 0 or
                    !std.mem.eql(u8, repl.history.getLast(), input))
                {
                    const history_item_next = try repl.history.allocator.alloc(u8, input.len);
                    @memcpy(history_item_next, input);
                    repl.history.append(history_item_next) catch |err| {
                        repl.event_loop_done = true;
                        return err;
                    };
                }
            }

            const statement = Parser.parse_statement(
                arena,
                input,
                repl.printer,
            ) catch |err| {
                switch (err) {
                    // These are parsing errors, so the REPL should
                    // not continue to execute this statement but can
                    // still accept new statements.
                    Parser.Error.BadIdentifier,
                    Parser.Error.BadOperation,
                    Parser.Error.BadValue,
                    Parser.Error.BadKeyValuePair,
                    Parser.Error.MissingEqualBetweenKeyValuePair,
                    Parser.Error.NoSyntaxMatch,
                    // TODO(zig): This will be more convenient to express
                    // once https://github.com/ziglang/zig/issues/2473 is
                    // in.
                    => return,

                    // An unexpected error for which we do
                    // want the stacktrace.
                    error.AccessDenied,
                    error.BrokenPipe,
                    error.ConnectionResetByPeer,
                    error.DeviceBusy,
                    error.DiskQuota,
                    error.FileTooBig,
                    error.InputOutput,
                    error.InvalidArgument,
                    error.LockViolation,
                    error.NoSpaceLeft,
                    error.NotOpenForWriting,
                    error.OperationAborted,
                    error.OutOfMemory,
                    error.SystemResources,
                    error.Unexpected,
                    error.WouldBlock,
                    => return err,
                }
            };
            try repl.do_statement(statement);
        }

        fn display_help(repl: *Repl) !void {
            try repl.printer.print("TigerBeetle CLI Client {}\n" ++
                \\  Hit enter after a semicolon to run a command.
                \\
                \\Examples:
                \\  create_accounts id=1 code=10 ledger=700 flags=linked|history,
                \\                  id=2 code=10 ledger=700;
                \\  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
                \\  lookup_accounts id=1;
                \\  lookup_accounts id=1, id=2;
                \\  get_account_transfers account_id=1 flags=debits|credits;
                \\  get_account_balances account_id=1 flags=debits|credits;
                \\
                \\
            , .{constants.semver});
        }

        pub fn run(
            arena: *std.heap.ArenaAllocator,
            addresses: []const std.net.Address,
            cluster_id: u128,
            statements: []const u8,
            verbose: bool,
        ) !void {
            const allocator = arena.allocator();
            const stdout = std.io.getStdOut();

            var repl = Repl{
                .starting_terminal_mode = undefined,
                .client = undefined,
                .debug_logs = verbose,
                .request_done = true,
                .event_loop_done = false,
                .interactive = statements.len == 0,
                .printer = .{
                    .stderr = std.io.getStdErr().writer(),
                    .stdout = stdout.writer(),
                },
                .history = std.ArrayList([]const u8).init(allocator),
            };

            if (repl.interactive) {
                if (!stdout.getOrEnableAnsiEscapeSupport()) {
                    std.debug.print("ANSI escape sequences not supported.\n", .{});
                    std.process.exit(1);
                }
                if (builtin.os.tag == .windows) {
                    const handle_stdin = try windows.GetStdHandle(windows.STD_INPUT_HANDLE);
                    var mode_stdin: u32 = 0;
                    if (windows.kernel32.GetConsoleMode(handle_stdin, &mode_stdin) == 0) {
                        return windows.unexpectedError(windows.kernel32.GetLastError());
                    }
                    const handle_stdout = try windows.GetStdHandle(windows.STD_OUTPUT_HANDLE);
                    var mode_stdout: u32 = 0;
                    if (windows.kernel32.GetConsoleMode(handle_stdout, &mode_stdout) == 0) {
                        return windows.unexpectedError(windows.kernel32.GetLastError());
                    }
                    repl.starting_terminal_mode = @ptrCast(&WindowsConsoleMode{
                        .stdin = mode_stdin,
                        .stdout = mode_stdout,
                    });
                } else {
                    const termios = try posix.tcgetattr(std.io.getStdIn().handle);
                    repl.starting_terminal_mode = @ptrCast(&termios);
                }
            }

            try repl.debug("Connecting to '{any}'.\n", .{addresses});

            const client_id = std.crypto.random.int(u128);

            var io = try IO.init(32, 0);

            var message_pool = try MessagePool.init(allocator, .client);

            var client = try Client.init(
                allocator,
                .{
                    .id = client_id,
                    .cluster = cluster_id,
                    .replica_count = @intCast(addresses.len),
                    .message_pool = &message_pool,
                    .message_bus_options = .{
                        .configuration = addresses,
                        .io = &io,
                    },
                },
            );
            repl.client = &client;

            client.register(register_callback, @intCast(@intFromPtr(&repl)));
            while (!repl.event_loop_done) {
                repl.client.tick();
                try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }
            repl.event_loop_done = false;

            if (statements.len > 0) {
                var statements_iterator = std.mem.split(u8, statements, ";");
                while (statements_iterator.next()) |statement_string| {
                    // Release allocation after every execution.
                    var execution_arena = std.heap.ArenaAllocator.init(allocator);
                    defer execution_arena.deinit();
                    const statement = Parser.parse_statement(
                        &execution_arena,
                        statement_string,
                        repl.printer,
                    ) catch |err| {
                        switch (err) {
                            // These are parsing errors and since this
                            // is not an interactive command, we should
                            // exit immediately. Parsing error info
                            // has already been emitted to stderr.
                            Parser.Error.BadIdentifier,
                            Parser.Error.BadOperation,
                            Parser.Error.BadValue,
                            Parser.Error.BadKeyValuePair,
                            Parser.Error.MissingEqualBetweenKeyValuePair,
                            Parser.Error.NoSyntaxMatch,
                            // TODO: This will be more convenient to express
                            // once https://github.com/ziglang/zig/issues/2473 is
                            // in.
                            => std.process.exit(1),

                            // An unexpected error for which we do
                            // want the stacktrace.
                            error.AccessDenied,
                            error.BrokenPipe,
                            error.ConnectionResetByPeer,
                            error.DeviceBusy,
                            error.DiskQuota,
                            error.FileTooBig,
                            error.InputOutput,
                            error.InvalidArgument,
                            error.LockViolation,
                            error.NoSpaceLeft,
                            error.NotOpenForWriting,
                            error.OperationAborted,
                            error.OutOfMemory,
                            error.SystemResources,
                            error.Unexpected,
                            error.WouldBlock,
                            => return err,
                        }
                    };
                    try repl.do_statement(statement);
                }
            } else {
                try repl.display_help();
            }

            while (!repl.event_loop_done) {
                if (repl.request_done and repl.interactive) {
                    // Release allocation after every execution.
                    var execution_arena = std.heap.ArenaAllocator.init(allocator);
                    defer execution_arena.deinit();
                    try repl.do_repl(&execution_arena);
                }
                repl.client.tick();
                try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }
        }

        fn register_callback(
            user_data: u128,
            result: *const vsr.RegisterResult,
        ) void {
            _ = result;

            const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(!repl.event_loop_done);

            repl.event_loop_done = true;
        }

        fn send(
            repl: *Repl,
            operation: StateMachine.Operation,
            arguments: []const u8,
        ) !void {
            const operation_type = switch (operation) {
                .create_accounts, .create_transfers => "create",
                .get_account_transfers, .get_account_balances => "get",
                .lookup_accounts, .lookup_transfers => "lookup",
                .pulse => unreachable,
                .query_accounts, .query_transfers => "query",
            };
            const object_type = switch (operation) {
                .create_accounts, .lookup_accounts, .query_accounts => "accounts",
                .create_transfers, .lookup_transfers, .query_transfers => "transfers",
                .get_account_transfers => "account transfers",
                .get_account_balances => "account balances",
                .pulse => unreachable,
            };

            if (arguments.len == 0) {
                try repl.fail(
                    "No {s} to {s}.\n",
                    .{ object_type, operation_type },
                );
                return;
            }

            repl.request_done = false;

            try repl.debug("Sending command: {}.\n", .{operation});
            repl.client.request(
                client_request_callback,
                @intCast(@intFromPtr(repl)),
                operation,
                arguments,
            );
        }

        fn display_object(repl: *Repl, object: anytype) !void {
            assert(@TypeOf(object.*) == tb.Account or
                @TypeOf(object.*) == tb.Transfer or
                @TypeOf(object.*) == tb.AccountBalance);

            try repl.printer.print("{{\n", .{});
            inline for (@typeInfo(@TypeOf(object.*)).Struct.fields, 0..) |object_field, i| {
                if (comptime std.mem.eql(u8, object_field.name, "reserved")) {
                    continue;
                    // No need to print out reserved.
                }

                if (i > 0) {
                    try repl.printer.print(",\n", .{});
                }

                if (comptime std.mem.eql(u8, object_field.name, "flags")) {
                    try repl.printer.print("  \"" ++ object_field.name ++ "\": [", .{});
                    var needs_comma = false;

                    inline for (@typeInfo(object_field.type).Struct.fields) |flag_field| {
                        if (comptime !std.mem.eql(u8, flag_field.name, "padding")) {
                            if (@field(@field(object, "flags"), flag_field.name)) {
                                if (needs_comma) {
                                    try repl.printer.print(",", .{});
                                    needs_comma = false;
                                }

                                try repl.printer.print("\"{s}\"", .{flag_field.name});
                                needs_comma = true;
                            }
                        }
                    }

                    try repl.printer.print("]", .{});
                } else {
                    try repl.printer.print(
                        "  \"{s}\": \"{}\"",
                        .{ object_field.name, @field(object, object_field.name) },
                    );
                }
            }
            try repl.printer.print("\n}}\n", .{});
        }

        fn client_request_callback_error(
            user_data: u128,
            operation: StateMachine.Operation,
            result: []const u8,
        ) !void {
            const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(repl.request_done == false);
            try repl.debug("Operation completed: {}.\n", .{operation});

            defer {
                repl.request_done = true;

                if (!repl.interactive) {
                    repl.event_loop_done = true;
                }
            }

            switch (operation) {
                .create_accounts => {
                    const create_account_results = std.mem.bytesAsSlice(
                        tb.CreateAccountsResult,
                        result,
                    );

                    if (create_account_results.len > 0) {
                        for (create_account_results) |*reason| {
                            try repl.printer.print(
                                "Failed to create account ({}): {any}.\n",
                                .{ reason.index, reason.result },
                            );
                        }
                    }
                },
                .lookup_accounts, .query_accounts => {
                    const account_results = std.mem.bytesAsSlice(
                        tb.Account,
                        result,
                    );

                    if (account_results.len == 0) {
                        try repl.fail("No accounts were found.\n", .{});
                    } else {
                        for (account_results) |*account| {
                            try repl.display_object(account);
                        }
                    }
                },
                .create_transfers => {
                    const create_transfer_results = std.mem.bytesAsSlice(
                        tb.CreateTransfersResult,
                        result,
                    );

                    if (create_transfer_results.len > 0) {
                        for (create_transfer_results) |*reason| {
                            try repl.printer.print(
                                "Failed to create transfer ({}): {any}.\n",
                                .{ reason.index, reason.result },
                            );
                        }
                    }
                },
                .lookup_transfers, .get_account_transfers, .query_transfers => {
                    const transfer_results = std.mem.bytesAsSlice(
                        tb.Transfer,
                        result,
                    );

                    if (transfer_results.len == 0) {
                        try repl.fail("No transfers were found.\n", .{});
                    } else {
                        for (transfer_results) |*transfer| {
                            try repl.display_object(transfer);
                        }
                    }
                },
                .get_account_balances => {
                    const get_account_balances_results = std.mem.bytesAsSlice(
                        tb.AccountBalance,
                        result,
                    );

                    if (get_account_balances_results.len == 0) {
                        try repl.fail("No balances were found.\n", .{});
                    } else {
                        for (get_account_balances_results) |*balance| {
                            try repl.display_object(balance);
                        }
                    }
                },
                .pulse => unreachable,
            }
        }

        fn client_request_callback(
            user_data: u128,
            operation: StateMachine.Operation,
            result: []u8,
        ) void {
            client_request_callback_error(
                user_data,
                operation,
                result,
            ) catch |err| {
                const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
                repl.fail("Error in callback: {any}", .{err}) catch return;
            };
        }

        fn get_cursor_position(
            repl: *Repl,
            allocator: std.mem.Allocator,
            reader: std.io.AnyReader,
        ) !Terminal.CursorPosition {
            // Obtaining the cursor's position relies on sending a request payload to stdout. The
            // response is read from stdin, but it may have been altered by user input, so we keep
            // retrying until successful.
            var buffer = std.ArrayList(u8).init(allocator);
            while (true) {
                // The response is of the form `<ESC>[{row};{col}R`.
                try repl.printer.print("\x1b[6n", .{});
                buffer.clearRetainingCapacity();
                try reader.streamUntilDelimiter(buffer.writer(), '[', null);

                buffer.clearRetainingCapacity();
                try reader.streamUntilDelimiter(buffer.writer(), ';', null);
                const row = std.fmt.parseInt(usize, buffer.items, 10) catch continue;

                buffer.clearRetainingCapacity();
                try reader.streamUntilDelimiter(buffer.writer(), 'R', null);
                const column = std.fmt.parseInt(usize, buffer.items, 10) catch continue;

                return Terminal.CursorPosition{
                    .row = row,
                    .column = column,
                };
            }
        }

        fn get_terminal(
            repl: *Repl,
            allocator: std.mem.Allocator,
            reader: std.io.AnyReader,
        ) !Terminal {
            // We move the cursor to a location that is unlikely to exist (2^16th row and column).
            // Terminals usually handle this by placing the cursor at their end position, which we
            // can use to obtain its resolution/size.
            const cursor_start = try repl.get_cursor_position(allocator, reader);
            try repl.printer.print("\x1b[{};{}H", .{ std.math.maxInt(u16), std.math.maxInt(u16) });
            const cursor_end = try repl.get_cursor_position(allocator, reader);
            try repl.printer.print("\x1b[{};{}H", .{ cursor_start.row, cursor_start.column });
            return Terminal{
                .cursor_position = cursor_start,
                .size = Terminal.Size{
                    .rows = cursor_end.row,
                    .columns = cursor_end.column,
                },
            };
        }

        fn prompt_mode_set(repl: *Repl) anyerror!void {
            if (builtin.os.tag == .windows) {
                const console_mode: *const WindowsConsoleMode = @alignCast(@ptrCast(
                    repl.starting_terminal_mode,
                ));

                const handle_stdin = try windows.GetStdHandle(windows.STD_INPUT_HANDLE);
                var mode_stdin: u32 = console_mode.*.stdin;
                mode_stdin &= ~@intFromEnum(WindowsConsoleMode.Input.enable_line_input);
                mode_stdin &= ~@intFromEnum(WindowsConsoleMode.Input.enable_echo_input);
                mode_stdin |= @intFromEnum(WindowsConsoleMode.Input.enable_virtual_terminal_input);
                if (windows.kernel32.SetConsoleMode(handle_stdin, mode_stdin) == 0) {
                    return windows.unexpectedError(windows.kernel32.GetLastError());
                }

                const handle_stdout = try windows.GetStdHandle(windows.STD_OUTPUT_HANDLE);
                var mode_stdout: u32 = console_mode.*.stdout;
                mode_stdout |= @intFromEnum(WindowsConsoleMode.Output.enable_processed_output);
                mode_stdout |= @intFromEnum(WindowsConsoleMode.Output.enable_wrap_at_eol_output);
                mode_stdout |= @intFromEnum(
                    WindowsConsoleMode.Output.enable_virtual_terminal_processing,
                );
                mode_stdout &= ~@intFromEnum(WindowsConsoleMode.Output.disable_newline_auto_return);
                if (windows.kernel32.SetConsoleMode(handle_stdout, mode_stdout) == 0) {
                    return windows.unexpectedError(windows.kernel32.GetLastError());
                }
            } else {
                const termios_start: *const posix.termios = @alignCast(@ptrCast(
                    repl.starting_terminal_mode,
                ));

                var termios_new = termios_start.*;
                termios_new.lflag.ECHO = false;
                termios_new.lflag.ICANON = false;
                termios_new.cc[@intFromEnum(posix.V.MIN)] = 1;
                termios_new.cc[@intFromEnum(posix.V.TIME)] = 0;
                try posix.tcsetattr(std.io.getStdIn().handle, .NOW, termios_new);
            }
        }

        fn prompt_mode_unset(repl: *Repl) !void {
            if (builtin.os.tag == .windows) {
                const console_mode: *const WindowsConsoleMode = @alignCast(@ptrCast(
                    repl.starting_terminal_mode,
                ));
                const handle_stdin = try windows.GetStdHandle(windows.STD_INPUT_HANDLE);
                if (windows.kernel32.SetConsoleMode(handle_stdin, console_mode.stdin) == 0) {
                    return windows.unexpectedError(windows.kernel32.GetLastError());
                }
                const handle_stdout = try windows.GetStdHandle(windows.STD_OUTPUT_HANDLE);
                if (windows.kernel32.SetConsoleMode(handle_stdout, console_mode.stdout) == 0) {
                    return windows.unexpectedError(windows.kernel32.GetLastError());
                }
            } else {
                const termios: *const posix.termios = @alignCast(@ptrCast(
                    repl.starting_terminal_mode,
                ));
                try posix.tcsetattr(std.io.getStdIn().handle, .NOW, termios.*);
            }
        }
    };
}

const WindowsConsoleMode = struct {
    stdin: u32,
    stdout: u32,

    const Input = enum(u32) {
        enable_line_input = 0x0002,
        enable_echo_input = 0x0004,
        enable_virtual_terminal_input = 0x0200,
    };

    const Output = enum(u32) {
        enable_processed_output = 0x0001,
        enable_wrap_at_eol_output = 0x0002,
        enable_virtual_terminal_processing = 0x0004,
        disable_newline_auto_return = 0x0008,
    };
};

const Terminal = struct {
    size: Size,
    cursor_position: CursorPosition,

    const Size = struct { rows: usize, columns: usize };

    const CursorPosition = struct {
        row: usize,
        column: usize,

        fn after_delta(self: CursorPosition, delta: isize, size: Size) CursorPosition {
            if (delta == 0) return self;

            // Rows and columns in terminals are one-based indexed. For simple manipulation across
            // the grid, we map it to a zero-based index array of cells. When experiencing overflows
            // on either end, we always saturate them such that they remain within the first or last
            // row of the terminal (assuming a fixed size).
            const cell_total: isize = @intCast(size.rows * size.columns);
            const cell_index_current: isize = @intCast(
                (self.row - 1) * size.columns + (self.column - 1),
            );
            assert(cell_index_current >= 0 and cell_index_current < cell_total);

            const cell_index_last: isize = @intCast(cell_total - 1);
            const column_total: isize = @intCast(size.columns);

            var cell_index_after_delta: isize = cell_index_current + delta;
            if (cell_index_after_delta > cell_index_last) {
                const cell_index_row_n_col_1 = cell_total - column_total;
                const cell_extra = cell_index_after_delta - cell_index_last;
                cell_index_after_delta =
                    cell_index_row_n_col_1 + @rem((cell_extra - 1), column_total);
            } else if (cell_index_after_delta < 0) {
                const cell_index_row_1_col_n = column_total - 1;
                const cell_extra: isize = @intCast(@abs(cell_index_after_delta));
                cell_index_after_delta =
                    cell_index_row_1_col_n - @rem((cell_extra - 1), column_total);
            }

            const row_after_delta = @divTrunc(cell_index_after_delta, column_total) + 1;
            const column_after_delta = @rem(cell_index_after_delta, column_total) + 1;

            assert(row_after_delta >= 1 and row_after_delta <= size.rows);
            assert(column_after_delta >= 1 and column_after_delta <= size.columns);

            return .{ .row = @intCast(row_after_delta), .column = @intCast(column_after_delta) };
        }
    };

    fn cursor_position_after_delta(self: Terminal, delta: isize) CursorPosition {
        return self.cursor_position.after_delta(delta, self.size);
    }
};

const UserInput = union(enum) {
    printable: u8,
    newline,
    backspace,
    up,
    down,
    left,
    right,
    unhandled,

    pub fn read(reader: std.io.AnyReader) !?UserInput {
        const byte = try reader.readByte();
        switch (byte) {
            std.ascii.control_code.eot => return null,
            std.ascii.control_code.cr, std.ascii.control_code.lf => return .newline,
            std.ascii.control_code.bs, std.ascii.control_code.del => return .backspace,
            std.ascii.control_code.esc => {
                const second_byte = try reader.readByte();
                switch (second_byte) {
                    '[' => {
                        const third_byte = try reader.readByte();
                        switch (third_byte) {
                            'A' => return .up,
                            'B' => return .down,
                            'C' => return .right,
                            'D' => return .left,
                            else => return .unhandled,
                        }
                    },
                    else => return .unhandled,
                }
            },
            else => {
                if (std.ascii.isPrint(byte)) {
                    return .{ .printable = byte };
                }
                return .unhandled;
            },
        }
    }
};

const null_printer = Printer{
    .stderr = null,
    .stdout = null,
};

test "repl.zig: Terminal cursor position change is valid" {
    const tests = [_]struct {
        size: Terminal.Size,
        source: Terminal.CursorPosition,
        delta: isize,
        destination: Terminal.CursorPosition,
    }{
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 1, .column = 1 },
            .delta = -32,
            .destination = .{ .row = 1, .column = 9 },
        },
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 1, .column = 1 },
            .delta = -1,
            .destination = .{ .row = 1, .column = 10 },
        },
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 1, .column = 2 },
            .delta = -1,
            .destination = .{ .row = 1, .column = 1 },
        },
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 3, .column = 8 },
            .delta = -26,
            .destination = .{ .row = 1, .column = 2 },
        },
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 2, .column = 9 },
            .delta = 0,
            .destination = .{ .row = 2, .column = 9 },
        },
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 2, .column = 9 },
            .delta = 61,
            .destination = .{ .row = 8, .column = 10 },
        },
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 10, .column = 9 },
            .delta = 1,
            .destination = .{ .row = 10, .column = 10 },
        },
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 10, .column = 10 },
            .delta = 1,
            .destination = .{ .row = 10, .column = 1 },
        },
        .{
            .size = .{ .rows = 10, .columns = 10 },
            .source = .{ .row = 10, .column = 10 },
            .delta = 21,
            .destination = .{ .row = 10, .column = 1 },
        },
    };

    for (tests) |t| {
        const terminal = Terminal{
            .size = t.size,
            .cursor_position = t.source,
        };
        try std.testing.expectEqual(terminal.cursor_position_after_delta(t.delta), t.destination);
    }
}

test "repl.zig: Parser single transfer successfully" {
    const tests = [_]struct {
        in: []const u8 = "",
        want: tb.Transfer,
    }{
        .{
            .in = "create_transfers id=1",
            .want = tb.Transfer{
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
            .in =
            \\create_transfers id=32 amount=65 ledger=12 code=9999 pending_id=7
            \\ credit_account_id=2121 debit_account_id=77 user_data_128=2
            \\ user_data_64=3 user_data_32=4 flags=linked
            ,
            .want = tb.Transfer{
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
            .in =
            \\create_transfers flags=
            \\ post_pending_transfer |
            \\ balancing_credit |
            \\ balancing_debit |
            \\ void_pending_transfer |
            \\ pending |
            \\ linked
            ,
            .want = tb.Transfer{
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
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Parser multiple transfers successfully" {
    const tests = [_]struct {
        in: []const u8 = "",
        want: [2]tb.Transfer,
    }{
        .{
            .in = "create_transfers id=1 debit_account_id=2, id=2 credit_account_id = 1;",
            .want = [2]tb.Transfer{
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

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.sliceAsBytes(&t.want));
    }
}

test "repl.zig: Parser single account successfully" {
    const tests = [_]struct {
        in: []const u8,
        want: tb.Account,
    }{
        .{
            .in = "create_accounts id=1",
            .want = tb.Account{
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
            },
        },
        .{
            .in =
            \\create_accounts id=32 credits_posted=344 ledger=12 credits_pending=18
            \\ code=9999 flags=linked | debits_must_not_exceed_credits debits_posted=3390
            \\ debits_pending=3212 user_data_128=2 user_data_64=3 user_data_32=4
            ,
            .want = tb.Account{
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
            },
        },
        .{
            .in =
            \\create_accounts flags=credits_must_not_exceed_debits|
            \\ linked|debits_must_not_exceed_credits id =1
            ,
            .want = tb.Account{
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
            },
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_accounts);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Parser account filter successfully" {
    const tests = [_]struct {
        in: []const u8,
        operation: Parser.Operation,
        want: tb.AccountFilter,
    }{
        .{
            .in = "get_account_transfers account_id=1",
            .operation = .get_account_transfers,
            .want = tb.AccountFilter{
                .account_id = 1,
                .timestamp_min = 0,
                .timestamp_max = 0,
                .limit = StateMachine.constants.batch_max.get_account_transfers,
                .flags = .{
                    .credits = true,
                    .debits = true,
                    .reversed = false,
                },
            },
        },
        .{
            .in =
            \\get_account_balances account_id=1000
            \\flags=debits|reversed limit=10
            \\timestamp_min=1 timestamp_max=9999;
            \\
            ,
            .operation = .get_account_balances,
            .want = tb.AccountFilter{
                .account_id = 1000,
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

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, t.operation);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Parser query filter successfully" {
    const tests = [_]struct {
        in: []const u8,
        operation: Parser.Operation,
        want: tb.QueryFilter,
    }{
        .{
            .in = "query_transfers user_data_128=1",
            .operation = .query_transfers,
            .want = tb.QueryFilter{
                .user_data_128 = 1,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .ledger = 0,
                .code = 0,
                .timestamp_min = 0,
                .timestamp_max = 0,
                .limit = StateMachine.constants.batch_max.query_transfers,
                .flags = .{
                    .reversed = false,
                },
            },
        },
        .{
            .in =
            \\query_accounts user_data_128=1000
            \\user_data_64=100 user_data_32=10
            \\ledger=1 code=2
            \\flags=reversed limit=10
            \\timestamp_min=1 timestamp_max=9999;
            \\
            ,
            .operation = .query_accounts,
            .want = tb.QueryFilter{
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

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, t.operation);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Parser multiple accounts successfully" {
    const tests = [_]struct {
        in: []const u8,
        want: [2]tb.Account,
    }{
        .{
            .in = "create_accounts id=1, id=2",
            .want = [2]tb.Account{
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
                },
            },
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_accounts);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.sliceAsBytes(&t.want));
    }
}

test "repl.zig: Parser odd but correct formatting" {
    const tests = [_]struct {
        in: []const u8 = "",
        want: tb.Transfer,
    }{
        // Space between key-value pair and equality
        .{
            .in = "create_transfers id = 1",
            .want = tb.Transfer{
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
            .in = "create_transfers id =1",
            .want = tb.Transfer{
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
            .in = "  \t  \n  create_transfers id=1",
            .want = tb.Transfer{
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
            .in = "create_transfers id=1;",
            .want = tb.Transfer{
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
            .in =
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
            .want = tb.Transfer{
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

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Handle parsing errors" {
    const tests = [_]struct {
        in: []const u8 = "",
        err: anyerror,
    }{
        .{
            .in = "create_trans",
            .err = error.BadOperation,
        },
        .{
            .in =
            \\
            \\
            \\ create
            ,
            .err = error.BadOperation,
        },
        .{
            .in = "create_transfers 12",
            .err = error.BadIdentifier,
        },
        .{
            .in = "create_transfers =12",
            .err = error.BadIdentifier,
        },
        .{
            .in = "create_transfers x",
            .err = error.MissingEqualBetweenKeyValuePair,
        },
        .{
            .in = "create_transfers x=",
            .err = error.BadValue,
        },
        .{
            .in = "create_transfers x=    ",
            .err = error.BadValue,
        },
        .{
            .in = "create_transfers x=    ;",
            .err = error.BadValue,
        },
        .{
            .in = "create_transfers x=[]",
            .err = error.BadValue,
        },
        .{
            .in = "create_transfers id=abcd",
            .err = error.BadKeyValuePair,
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        const result = Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );
        try std.testing.expectError(t.err, result);
    }
}
