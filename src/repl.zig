const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("vsr.zig");
const stdx = vsr.stdx;
const constants = vsr.constants;
const IO = vsr.io.IO;
const Storage = vsr.storage.StorageType(IO);
const StateMachine = vsr.state_machine.StateMachineType(
    Storage,
    constants.state_machine_config,
);
const MessagePool = vsr.message_pool.MessagePool;
const RingBufferType = @import("ring_buffer.zig").RingBufferType;

const tb = vsr.tigerbeetle;

const Terminal = @import("terminal.zig").Terminal;
const Completion = @import("completion.zig").Completion;

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
        inline for (@typeInfo(ObjectSyntaxTree).Union.fields) |object_syntax_tree_field| {
            if (std.mem.eql(u8, @tagName(out.*), object_syntax_tree_field.name)) {
                const active_value = @field(out, object_syntax_tree_field.name);
                const ActiveValue = @TypeOf(active_value);

                inline for (@typeInfo(ActiveValue).Struct.fields) |active_value_field| {
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

    fn parse_int(comptime T: type, input: []const u8) !T {
        const info = @typeInfo(T);
        comptime assert(info == .Int);

        // When base is zero the string prefix is examined to detect the true base:
        // "0b", "0o" or "0x", otherwise base=10 is assumed.
        const base_unknown = 0;

        assert(input.len > 0);
        const input_negative = input[0] == '-';

        if (info.Int.signedness == .unsigned and input_negative) {
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
            .get_account_transfers, .get_account_balances => .{ .account_filter = tb.AccountFilter{
                .account_id = 0,
                .user_data_128 = 0,
                .user_data_64 = 0,
                .user_data_32 = 0,
                .code = 0,
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
            // Always need to check i against length in case we've hit the end.
            if (parser.offset >= parser.input.len or parser.input[parser.offset] == ';') {
                break;
            }

            // Expect comma separating objects.
            if (parser.offset < parser.input.len and parser.input[parser.offset] == ',') {
                parser.offset += 1;
                inline for (@typeInfo(ObjectSyntaxTree).Union.fields) |object_tree_field| {
                    if (std.mem.eql(u8, @tagName(object), object_tree_field.name)) {
                        const unwrapped_field = @field(object, object_tree_field.name);
                        arguments.appendSliceAssumeCapacity(std.mem.asBytes(&unwrapped_field));
                    }
                }
                const state_machine_op = std.meta.stringToEnum(
                    StateMachine.Operation,
                    @tagName(operation),
                ).?;

                if (!StateMachine.event_is_slice(state_machine_op)) {
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
            inline for (@typeInfo(ObjectSyntaxTree).Union.fields) |object_tree_field| {
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
            .arguments = arguments.items,
        };
    }
};

const repl_history_entries = 256;
const repl_history_entry_bytes_with_nul = 512;
const repl_history_entry_bytes_without_nul = 511;

const ReplBufferBoundedArray = stdx.BoundedArrayType(u8, repl_history_entry_bytes_without_nul);

pub fn ReplType(comptime MessageBus: type, comptime Time: type) type {
    const Client = vsr.ClientType(StateMachine, MessageBus, Time);

    // Requires 512 * 256 == 128KiB of stack space.
    const HistoryBuffer = RingBufferType(
        [repl_history_entry_bytes_with_nul]u8,
        .{ .array = repl_history_entries },
    );

    return struct {
        event_loop_done: bool,
        request_done: bool,

        interactive: bool,
        debug_logs: bool,

        client: Client,
        terminal: Terminal,
        completion: Completion,
        history: HistoryBuffer,

        /// Fixed-capacity buffer for reading input strings.
        buffer: ReplBufferBoundedArray,
        /// Saved input string while navigating through history.
        buffer_outside_history: ReplBufferBoundedArray,

        arguments: std.ArrayListUnmanaged(u8),
        message_pool: *MessagePool,
        io: *IO,

        const Repl = @This();

        fn fail(repl: *const Repl, comptime format: []const u8, arguments: anytype) !void {
            if (!repl.interactive) {
                try repl.terminal.print_error(format, arguments);
                std.process.exit(1);
            }

            try repl.terminal.print(format, arguments);
        }

        fn debug(repl: *const Repl, comptime format: []const u8, arguments: anytype) !void {
            if (repl.debug_logs) {
                try repl.terminal.print("[Debug] " ++ format, arguments);
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

        fn read_until_newline_or_eof(
            repl: *Repl,
        ) !?[]const u8 {
            repl.buffer.clear();
            repl.buffer_outside_history.clear();

            try repl.terminal.prompt_mode_set();
            defer repl.terminal.prompt_mode_unset() catch {};

            var terminal_screen = try repl.terminal.get_screen();

            var buffer_index: usize = 0;
            var history_index = repl.history.count;

            var current_completion: []const u8 = "";

            while (true) {
                const user_input = try repl.terminal.read_user_input() orelse return null;

                // Clear the completion menu when a match is selected or another key is pressed.
                if (user_input != .tab and repl.completion.count() > 0) {
                    repl.completion.clear();
                    try repl.terminal.print("\x1b[{};1H\x1b[J\x1b[{};{}H", .{
                        terminal_screen.cursor_row + 1,
                        terminal_screen.cursor_row,
                        terminal_screen.cursor_column,
                    });
                }

                switch (user_input) {
                    .ctrlc => {
                        // Erase everything below the current cursor's position in case Ctrl-C was
                        // pressed somewhere inside the buffer.
                        try repl.terminal.print("^C\x1b[J\n", .{});
                        return &.{};
                    },
                    .newline => {
                        try repl.terminal.print("\n", .{});
                        return repl.buffer.const_slice();
                    },
                    .printable => |character| {
                        if (repl.buffer.count() == repl_history_entry_bytes_without_nul) {
                            continue;
                        }

                        const is_append = buffer_index == repl.buffer.count();
                        if (is_append) {
                            terminal_screen.update_cursor_position(1);
                            try repl.terminal.print("{c}", .{character});
                            // Some terminals may not automatically move/scroll us down to the next
                            // row after appending a character at the last column. This can cause us
                            // to incorrectly report the cursor's position, so we force the terminal
                            // to do so by moving the cursor forward and backward one space.
                            if (terminal_screen.cursor_column == terminal_screen.columns) {
                                try repl.terminal.print("\x20\x1b[{};{}H", .{
                                    terminal_screen.cursor_row,
                                    terminal_screen.cursor_column,
                                });
                            }
                        } else {
                            // If we're inserting mid-buffer, we need to redraw everything that
                            // comes after as well. We'll track as if the cursor moved to the end of
                            // the buffer after redrawing, and move it back to one position after
                            // the newly inserted character.
                            const buffer_redraw_len: isize = @intCast(
                                repl.buffer.count() - buffer_index,
                            );
                            // It's crucial to update in two steps because the terminal may have
                            // scrolled down as part of the text redraw.
                            terminal_screen.update_cursor_position(buffer_redraw_len);
                            terminal_screen.update_cursor_position(1 - buffer_redraw_len);
                            try repl.terminal.print("{c}{s}\x1b[{};{}H", .{
                                character,
                                repl.buffer.const_slice()[buffer_index..],
                                terminal_screen.cursor_row,
                                terminal_screen.cursor_column,
                            });
                        }

                        repl.buffer.insert_assume_capacity(buffer_index, character);
                        buffer_index += 1;
                    },
                    .backspace => if (buffer_index > 0) {
                        terminal_screen.update_cursor_position(-1);
                        try repl.terminal.print("\x1b[{};{}H{s}\x20\x1b[{};{}H", .{
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                            // If we're deleting mid-buffer, we need to redraw everything that
                            // comes after as well.
                            if (buffer_index < repl.buffer.count())
                                repl.buffer.const_slice()[buffer_index..]
                            else
                                "",
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });
                        buffer_index -= 1;
                        _ = repl.buffer.ordered_remove(buffer_index);
                    },
                    .tab => {
                        try repl.completion.split_and_complete(repl.buffer.slice(), buffer_index);

                        current_completion = try repl.completion.get_next_completion();

                        // Skip the current completion entry if adding it would overflow the buffer.
                        if ((repl.completion.prefix.count() + repl.completion.suffix.count() +
                            current_completion.len) >= repl_history_entry_bytes_without_nul)
                        {
                            continue;
                        }

                        // Move cursor to the start of the current row, clear the screen from start
                        // to end. Then print the leading buffer, currently selected completion and
                        // trailing buffer(if any).
                        terminal_screen.update_cursor_position(-@as(isize, @intCast(buffer_index)));

                        const buffer_row = terminal_screen.cursor_row;

                        terminal_screen.update_cursor_position(
                            @as(
                                isize,
                                @intCast(repl.completion.prefix.count() + current_completion.len),
                            ),
                        );

                        // \x1b[n;mH - Moves the cursor to row n, column m.
                        // \x1b[J - Clear the screen from current cursor position to end.
                        try repl.terminal.print("\x1b[{};1H\x1b[J{s}{s}{s}{s}\x20\x08", .{
                            buffer_row,
                            prompt,
                            repl.completion.prefix.const_slice(),
                            current_completion,
                            repl.completion.suffix.const_slice(),
                        });

                        // When cursor reaches the last row of terminal, scroll content upward
                        // to create space for displaying completion menu.
                        // \x1b[nS - Scroll whole page up by n lines; defualt 1 line.
                        // \x1b[nA - Moves cursor up n cells; default 1 cell up.
                        if (terminal_screen.cursor_row == terminal_screen.rows) {
                            try repl.terminal.print("\x1b[S\x1b[A", .{});
                        }

                        // Position cursor for completion menu. If the current buffer overflows to
                        // the next line after printing the prefix, selected completion and suffix,
                        // the completion menu should effectively be placed `row_delta` lines below
                        // the current buffer row.
                        const buffer_width =
                            repl.completion.prefix.count() +
                            repl.completion.suffix.count() +
                            current_completion.len + 1;
                        const row_delta = @divFloor(buffer_width, terminal_screen.columns) + 1;
                        try repl.terminal.print("\x1b[{};1H", .{buffer_row + row_delta});

                        // Display completion menu in the next line and highlight the currently
                        // selected completion.
                        var menu_width: usize = 0;
                        var match_itr = repl.completion.matches.iterator();

                        while (match_itr.next()) |match| {
                            const match_len = std.mem.indexOf(u8, match[0..], &[_]u8{'\x00'}).?;
                            menu_width += match_len + 2;

                            // \x1b[7m - Highlights the text by inverting the color. Inverts
                            //           the text and background color.
                            // \x1b[0m - Reset the style property of text. In this case, resets
                            //           the effect of \x1b[7m - color inversion.
                            if (std.mem.eql(u8, match[0..match_len], current_completion)) {
                                try repl.terminal.print("\x20\x1b[7m{s}\x1b[0m\x20", .{
                                    match[0..match_len],
                                });
                            } else {
                                try repl.terminal.print("\x20{s}\x20", .{match[0..match_len]});
                            }
                        }

                        // If the completion menu overflows past the last row of terminal screen,
                        // move the cursor up by number of rows that would be overflown.
                        const menu_rows = @divFloor(menu_width, terminal_screen.columns) + 1;
                        const required_rows = terminal_screen.cursor_row + menu_rows;
                        const overflow_rows = @as(
                            isize,
                            @intCast(required_rows),
                        ) - @as(
                            isize,
                            @intCast(terminal_screen.rows),
                        );
                        if (overflow_rows > 0) {
                            terminal_screen.update_cursor_position(
                                -@as(isize, @intCast(terminal_screen.columns)) * overflow_rows,
                            );
                        }

                        try repl.terminal.print("\x1b[{};{}H", .{
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });

                        // Re-fill the buffer with prefix, current match and suffix. Set the
                        // buffer index where the current selected match ends.
                        repl.buffer.clear();
                        repl.buffer.append_slice_assume_capacity(repl.completion.prefix.slice());
                        repl.buffer.append_slice_assume_capacity(current_completion);
                        repl.buffer.append_slice_assume_capacity(repl.completion.suffix.slice());
                        buffer_index = repl.buffer.count() - repl.completion.suffix.count();
                    },
                    .left => if (buffer_index > 0) {
                        terminal_screen.update_cursor_position(-1);
                        try repl.terminal.print("\x1b[{};{}H", .{
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });
                        buffer_index -= 1;
                    },
                    .right => if (buffer_index < repl.buffer.count()) {
                        terminal_screen.update_cursor_position(1);
                        try repl.terminal.print("\x1b[{};{}H", .{
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });
                        buffer_index += 1;
                    },
                    .up => if (history_index > 0) {
                        const history_index_next = history_index - 1;
                        const buffer_next_full = repl.history.get_ptr(history_index_next).?;
                        const buffer_next = std.mem.sliceTo(buffer_next_full, '\x00');
                        assert(buffer_next.len < repl_history_entry_bytes_with_nul);

                        // Move to the beginning of the current buffer.
                        terminal_screen.update_cursor_position(
                            -@as(isize, @intCast(buffer_index)),
                        );
                        const row_current_buffer_start = terminal_screen.cursor_row;

                        // Move to the end of the new buffer.
                        terminal_screen.update_cursor_position(
                            @as(isize, @intCast(buffer_next.len)),
                        );

                        try repl.terminal.print("\x1b[{};1H\x1b[J{s}{s}\x20\x1b[{};{}H", .{
                            row_current_buffer_start,
                            prompt,
                            buffer_next,
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });

                        if (history_index == repl.history.count) {
                            repl.buffer_outside_history.clear();
                            repl.buffer_outside_history.append_slice_assume_capacity(
                                repl.buffer.const_slice(),
                            );
                        }
                        history_index = history_index_next;

                        repl.buffer.clear();
                        repl.buffer.append_slice_assume_capacity(buffer_next);
                        buffer_index = repl.buffer.count();
                    },
                    .down => if (history_index < repl.history.count) {
                        const history_index_next = history_index + 1;

                        const buffer_next = if (history_index_next == repl.history.count)
                            repl.buffer_outside_history.const_slice()
                        else brk: {
                            const buffer_next_full = repl.history.get_ptr(history_index_next).?;
                            const buffer_next = std.mem.sliceTo(buffer_next_full, '\x00');
                            assert(buffer_next.len < repl_history_entry_bytes_with_nul);
                            break :brk buffer_next;
                        };

                        history_index = history_index_next;

                        // Move to the beginning of the current buffer.
                        terminal_screen.update_cursor_position(
                            -@as(isize, @intCast(buffer_index)),
                        );
                        const row_current_buffer_start = terminal_screen.cursor_row;

                        // Move to the end of the new buffer.
                        terminal_screen.update_cursor_position(
                            @as(isize, @intCast(buffer_next.len)),
                        );

                        try repl.terminal.print("\x1b[{};1H\x1b[J{s}{s}\x20\x1b[{};{}H", .{
                            row_current_buffer_start,
                            prompt,
                            buffer_next,
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });

                        repl.buffer.clear();
                        repl.buffer.append_slice_assume_capacity(buffer_next);
                        buffer_index = repl.buffer.count();
                    },
                    .unhandled => {},
                }
            }
            unreachable;
        }

        fn do_repl(
            repl: *Repl,
            arguments: *std.ArrayListUnmanaged(u8),
        ) !void {
            try repl.terminal.print(prompt, .{});
            const input = repl.read_until_newline_or_eof() catch |err| {
                repl.event_loop_done = true;
                return err;
            } orelse {
                // EOF.
                repl.event_loop_done = true;
                try repl.fail("\nExiting.\n", .{});
                return;
            };

            if (input.len > 0) {
                if (repl.history.empty() or
                    !std.mem.eql(u8, repl.history.tail_ptr_const().?[0..input.len], input))
                {
                    // NB: Avoiding big stack allocations below.

                    assert(input.len < repl_history_entry_bytes_with_nul);

                    if (repl.history.full()) {
                        repl.history.advance_head();
                    }
                    const history_tail: *[repl_history_entry_bytes_with_nul]u8 =
                        repl.history.next_tail_ptr().?;
                    @memset(history_tail, '\x00');
                    stdx.copy_left(.inexact, u8, history_tail, input);
                    repl.history.advance_tail();
                }
            }

            const statement = Parser.parse_statement(
                input,
                &repl.terminal,
                arguments,
            ) catch |err| {
                switch (err) {
                    // These are parsing errors, so the REPL should
                    // not continue to execute this statement but can
                    // still accept new statements.
                    Parser.Error.IdentifierBad,
                    Parser.Error.OperationBad,
                    Parser.Error.ValueBad,
                    Parser.Error.KeyValuePairBad,
                    Parser.Error.KeyValuePairEqualMissing,
                    Parser.Error.SyntaxMatchNone,
                    Parser.Error.SliceOperationUnsupported,
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
            try repl.terminal.print("TigerBeetle CLI Client {}\n" ++
                \\  Hit enter after a semicolon to run a command.
                \\  Ctrl+D to exit.
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

        pub fn init(
            allocator: std.mem.Allocator,
            time: Time,
            options: struct {
                addresses: []const std.net.Address,
                cluster_id: u128,
                verbose: bool,
            },
        ) !Repl {
            var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
                allocator,
                constants.message_size_max,
            );
            errdefer arguments.deinit(allocator);

            var message_pool = try allocator.create(MessagePool);
            errdefer allocator.destroy(message_pool);

            message_pool.* = try MessagePool.init(allocator, .client);
            errdefer message_pool.deinit(allocator);

            var io = try allocator.create(IO);
            errdefer allocator.destroy(io);

            io.* = try IO.init(32, 0);
            errdefer io.deinit();

            const client_id = stdx.unique_u128();
            const client = try Client.init(
                allocator,
                .{
                    .id = client_id,
                    .cluster = options.cluster_id,
                    .replica_count = @intCast(options.addresses.len),
                    .time = time,
                    .message_pool = message_pool,
                    .message_bus_options = .{
                        .configuration = options.addresses,
                        .io = io,
                    },
                },
            );
            errdefer client.deinit(allocator);

            return .{
                .client = client,
                .debug_logs = options.verbose,
                .request_done = true,
                .event_loop_done = false,
                .interactive = false,
                .terminal = undefined, // Init on run.
                .completion = undefined, // Init on run.
                .history = HistoryBuffer.init(), // No corresponding deinit.
                .buffer = .{},
                .buffer_outside_history = .{},
                .arguments = arguments,
                .message_pool = message_pool,
                .io = io,
            };
        }

        pub fn deinit(repl: *Repl, allocator: std.mem.Allocator) void {
            repl.client.deinit(allocator);
            repl.io.deinit();
            repl.message_pool.deinit(allocator);
            allocator.destroy(repl.message_pool);
            repl.arguments.deinit(allocator);
        }

        pub fn run(repl: *Repl, statements: []const u8) !void {
            repl.interactive = statements.len == 0;
            try Terminal.init(&repl.terminal, repl.interactive); // No corresponding deinit.

            try Completion.init(&repl.completion);

            repl.client.register(register_callback, @intCast(@intFromPtr(repl)));
            while (!repl.event_loop_done) {
                repl.client.tick();
                try repl.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }
            repl.event_loop_done = false;

            if (repl.interactive) {
                try repl.display_help();
            }

            var statements_iterator = if (statements.len > 0)
                std.mem.split(u8, statements, ";")
            else
                null;

            while (!repl.event_loop_done) {
                if (repl.request_done) {
                    repl.arguments.clearRetainingCapacity();
                    if (repl.interactive) {
                        try repl.do_repl(&repl.arguments);
                    } else blk: {
                        const statement_string = statements_iterator.?.next() orelse break :blk;

                        const statement = Parser.parse_statement(
                            statement_string,
                            &repl.terminal,
                            &repl.arguments,
                        ) catch |err| {
                            switch (err) {
                                // These are parsing errors and since this
                                // is not an interactive command, we should
                                // exit immediately. Parsing error info
                                // has already been emitted to stderr.
                                Parser.Error.IdentifierBad,
                                Parser.Error.OperationBad,
                                Parser.Error.ValueBad,
                                Parser.Error.KeyValuePairBad,
                                Parser.Error.KeyValuePairEqualMissing,
                                Parser.Error.SyntaxMatchNone,
                                Parser.Error.SliceOperationUnsupported,
                                // TODO: This will be more convenient to express
                                // once https://github.com/ziglang/zig/issues/2473 is
                                // in.
                                => std.posix.exit(1),

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
                }

                repl.client.tick();
                try repl.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
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

            try repl.terminal.print("{{\n", .{});
            inline for (@typeInfo(@TypeOf(object.*)).Struct.fields, 0..) |object_field, i| {
                if (comptime std.mem.eql(u8, object_field.name, "reserved")) {
                    continue;
                    // No need to print out reserved.
                }

                if (i > 0) {
                    try repl.terminal.print(",\n", .{});
                }

                if (comptime std.mem.eql(u8, object_field.name, "flags")) {
                    try repl.terminal.print("  \"" ++ object_field.name ++ "\": [", .{});
                    var needs_comma = false;

                    inline for (@typeInfo(object_field.type).Struct.fields) |flag_field| {
                        if (comptime !std.mem.eql(u8, flag_field.name, "padding")) {
                            if (@field(@field(object, "flags"), flag_field.name)) {
                                if (needs_comma) {
                                    try repl.terminal.print(",", .{});
                                    needs_comma = false;
                                }

                                try repl.terminal.print("\"{s}\"", .{flag_field.name});
                                needs_comma = true;
                            }
                        }
                    }

                    try repl.terminal.print("]", .{});
                } else {
                    try repl.terminal.print(
                        "  \"{s}\": \"{}\"",
                        .{ object_field.name, @field(object, object_field.name) },
                    );
                }
            }
            try repl.terminal.print("\n}}\n", .{});
        }

        fn client_request_callback_error(
            user_data: u128,
            operation: StateMachine.Operation,
            timestamp: u64,
            result: []const u8,
        ) !void {
            const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(repl.request_done == false);
            try repl.debug("Operation completed: {} timestamp={}.\n", .{ operation, timestamp });

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
                            try repl.terminal.print(
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
                            try repl.terminal.print(
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
            timestamp: u64,
            result: []u8,
        ) void {
            client_request_callback_error(
                user_data,
                operation,
                timestamp,
                result,
            ) catch |err| {
                const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
                repl.fail("Error in callback: {any}", .{err}) catch return;
            };
        }
    };
}

const null_terminal = Terminal{
    .mode_start = null,
    .stdin = undefined,
    .stderr = null,
    .stdout = null,
    .buffer_in = undefined,
};

test "repl.zig: Parser single transfer successfully" {
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
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&vector.result));
    }
}

test "repl.zig: Parser multiple transfers successfully" {
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
            statement.arguments,
            std.mem.sliceAsBytes(&vector.result),
        );
    }
}

test "repl.zig: Parser single account successfully" {
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
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&vector.result));
    }
}

test "repl.zig: Parser account filter successfully" {
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
                .limit = StateMachine.constants.batch_max.get_account_transfers,
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
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&vector.result));
    }
}

test "repl.zig: Parser query filter successfully" {
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
                .limit = StateMachine.constants.batch_max.query_transfers,
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
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&vector.result));
    }
}

test "repl.zig: Parser multiple accounts successfully" {
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
            statement.arguments,
            std.mem.sliceAsBytes(&vector.result),
        );
    }
}

test "repl.zig: Parser odd but correct formatting" {
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
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&vector.result));
    }
}

test "repl.zig: Handle parsing errors" {
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

test "repl.zig: Parser fails for operations not supporting multiple objects" {
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
