const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("vsr.zig");
const stdx = vsr.stdx;
const constants = vsr.constants;
const IO = vsr.io.IO;
const Time = vsr.time.Time;
const StaticAllocator = @import("static_allocator.zig");
const Storage = vsr.storage.StorageType(IO);
const StateMachine = vsr.state_machine.StateMachineType(
    Storage,
    constants.state_machine_config,
);
const MessagePool = vsr.message_pool.MessagePool;
const RingBufferType = stdx.RingBufferType;

const tb = vsr.tigerbeetle;

const Terminal = @import("repl/terminal.zig").Terminal;
const Completion = @import("repl/completion.zig").Completion;
const Parser = @import("repl/parser.zig").Parser;
const LineEditor = @import("repl/line_editor.zig").LineEditor;

const repl_history_entries = 256;
const repl_history_entry_bytes_with_nul = 512;
const repl_history_entry_bytes_without_nul = 511;

const ReplBufferBoundedArray = stdx.BoundedArrayType(u8, repl_history_entry_bytes_without_nul);

pub fn ReplType(comptime MessageBus: type) type {
    const Client = vsr.ClientType(StateMachine, MessageBus);

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

        line_editor: LineEditor,
        /// Saved input string while navigating through history.
        buffer_outside_history: ReplBufferBoundedArray,

        arguments: std.ArrayListUnmanaged(u8),
        message_pool: *MessagePool,
        io: *IO,

        static_allocator: StaticAllocator,

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
                    try repl.send(
                        state_machine_operation.?,
                        statement.arguments,
                    );
                },
            }
        }

        const prompt = "> ";

        fn redraw_line(repl: *const Repl, prompt_str: []const u8) !void {
            const buf = repl.line_editor.get_buffer();
            const prompt_len = prompt_str.len;

            // move to start of line (CR)
            try repl.terminal.print("\r", .{});

            try repl.terminal.print("{s}", .{prompt_str});

            // clear to end of line
            try repl.terminal.print("\x1b[K", .{});

            try repl.terminal.print("{s}", .{buf});

            // compute cursor column in bytes: prompt bytes + byte offset of cursor.
            const cursor_byte_offset = repl.line_editor.get_cursor_byte_offset();
            const cursor_col_1based = prompt_len + cursor_byte_offset + 1;

            // move cursor to calculated column (G is 1-based)
            try repl.terminal.print("\x1b[{}G", .{cursor_col_1based});
        }

        fn read_until_newline_or_eof(
            repl: *Repl,
        ) !?[]const u8 {
            repl.line_editor.clear();
            repl.buffer_outside_history.clear();

            try repl.terminal.prompt_mode_set();
            defer repl.terminal.prompt_mode_unset() catch {};

            var terminal_screen = try repl.terminal.get_screen();

            var history_index = repl.history.count;

            while (true) {
                const user_input = try repl.terminal.read_user_input() orelse return null;

                switch (user_input) {
                    .ctrld => {
                        if (repl.line_editor.get_buffer().len == 0) {
                            return null;
                        }
                        repl.line_editor.delete();
                        try redraw_line(repl, prompt);
                    },
                    .ctrlc => {
                        // Move to end of line, print "^C" and abort the command.
                        repl.line_editor.move_end();
                        try redraw_line(repl, prompt);
                        try repl.terminal.print("^C\n", .{});
                        repl.line_editor.clear();
                        return &.{};
                    },
                    .newline => {
                        repl.line_editor.move_end();
                        try redraw_line(repl, prompt);
                        try repl.terminal.print("\n", .{});
                        return repl.line_editor.get_buffer();
                    },
                    .printable => |character| {
                        if (repl.line_editor.get_buffer().len >= repl_history_entry_bytes_without_nul) {
                            continue;
                        }
                        repl.line_editor.insert_char(character);
                        try redraw_line(repl, prompt);
                    },
                    .backspace => {
                        repl.line_editor.backspace();
                        try redraw_line(repl, prompt);
                    },
                    .delete => {
                        repl.line_editor.delete();
                        try redraw_line(repl, prompt);
                    },
                    // Tab completion skipped for this implementation
                    .tab => {},
                    .left, .ctrlb => {
                        repl.line_editor.move_left();
                        try redraw_line(repl, prompt);
                    },
                    .right, .ctrlf => {
                        repl.line_editor.move_right();
                        try redraw_line(repl, prompt);
                    },
                    .up, .ctrlp => if (history_index > 0) {
                        const history_index_next = history_index - 1;
                        const buffer_next_full = repl.history.get_ptr(history_index_next).?;
                        const buffer_next = std.mem.sliceTo(buffer_next_full, '\x00');

                        if (history_index == repl.history.count) {
                            repl.buffer_outside_history.clear();
                            repl.buffer_outside_history.push_slice(repl.line_editor.get_buffer());
                        }
                        try repl.line_editor.set_content(buffer_next);
                        history_index = history_index_next;
                        try redraw_line(repl, prompt);
                    },
                    .down, .ctrln => if (history_index < repl.history.count) {
                        const history_index_next = history_index + 1;

                        const buffer_next = if (history_index_next == repl.history.count)
                            repl.buffer_outside_history.const_slice()
                        else brk: {
                            const buffer_next_full = repl.history.get_ptr(history_index_next).?;
                            const buffer_next = std.mem.sliceTo(buffer_next_full, '\x00');
                            break :brk buffer_next;
                        };

                        history_index = history_index_next;

                        try repl.line_editor.set_content(buffer_next);
                        try redraw_line(repl, prompt);
                    },
                    .altf, .ctrlright => {
                        repl.line_editor.move_forward_by_word();
                        try redraw_line(repl, prompt);
                    },
                    .altb, .ctrlleft => {
                        repl.line_editor.move_backward_by_word();
                        try redraw_line(repl, prompt);
                    },
                    .ctrla, .home => {
                        repl.line_editor.move_home();
                        try redraw_line(repl, prompt);
                    },
                    .ctrle, .end => {
                        repl.line_editor.move_end();
                        try redraw_line(repl, prompt);
                    },
                    .ctrlk => {
                        // Clear screen from cursor.
                        try repl.terminal.print("\x1b[J", .{});
                    },
                    .ctrll => {
                        // Move to 0,0 and clear the screen from cursor, print the prompt, then ask
                        // the terminal for the new position.
                        try repl.terminal.print("\x1b[0;0H\x1b[J", .{});
                        try repl.terminal.print(prompt, .{});
                        terminal_screen = try repl.terminal.get_screen();

                        // Print whatever is in the buffer and move the cursor back to cursor.
                        terminal_screen.update_cursor_position(
                            @as(isize, @intCast(repl.line_editor.cursor)),
                        );

                        try repl.terminal.print("{s}\x1b[{};{}H", .{
                            repl.line_editor.get_buffer(),
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });
                    },
                    .unhandled => {},
                }
            }
            unreachable;
        }

        fn move_forward_by_word(buffer: []const u8, buffer_index: usize) usize {
            var cur_pos = buffer_index;
            while (cur_pos < buffer.len and std.ascii.isWhitespace(buffer[cur_pos])) {
                cur_pos += 1;
            }
            while (cur_pos < buffer.len and !std.ascii.isWhitespace(buffer[cur_pos])) {
                cur_pos += 1;
            }
            return cur_pos;
        }

        fn move_backward_by_word(buffer: []const u8, buffer_index: usize) usize {
            var cur_pos = buffer_index;
            while (cur_pos > 0 and std.ascii.isWhitespace(buffer[cur_pos - 1])) {
                cur_pos -= 1;
            }
            while (cur_pos > 0 and !std.ascii.isWhitespace(buffer[cur_pos - 1])) {
                cur_pos -= 1;
            }
            return cur_pos;
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
                const add_to_history = brk: {
                    const last_entry = repl.history.tail_ptr_const() orelse break :brk true;
                    const last_entry_str = std.mem.sliceTo(last_entry, '\x00');
                    break :brk !std.mem.eql(u8, last_entry_str, input);
                };

                if (add_to_history) {
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
                    error.NoDevice,
                    error.ProcessNotFound,
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
                \\  create_accounts id=1 code=10 ledger=700 flags=linked|history, id=2 code=10 ledger=700;
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
            parent_allocator: std.mem.Allocator,
            io: *IO,
            time: Time,
            options: struct {
                addresses: []const std.net.Address,
                cluster_id: u128,
                verbose: bool,
            },
        ) !Repl {
            var static_allocator = StaticAllocator.init(parent_allocator);
            const allocator = static_allocator.allocator();

            var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
                allocator,
                constants.message_body_size_max,
            );
            errdefer arguments.deinit(allocator);

            var message_pool = try allocator.create(MessagePool);
            errdefer allocator.destroy(message_pool);

            message_pool.* = try MessagePool.init(allocator, .client);
            errdefer message_pool.deinit(allocator);

            const client_id = stdx.unique_u128();
            const client = try Client.init(
                allocator,
                time,
                message_pool,
                .{
                    .id = client_id,
                    .cluster = options.cluster_id,
                    .replica_count = @intCast(options.addresses.len),
                    .message_bus_options = .{
                        .configuration = options.addresses,
                        .io = io,
                    },
                },
            );
            errdefer client.deinit(allocator);

            var line_editor = LineEditor.init();
            errdefer line_editor.deinit();

            // Disable all dynamic allocation from this point onwards.
            static_allocator.transition_from_init_to_static();

            return .{
                .client = client,
                .debug_logs = options.verbose,
                .request_done = true,
                .event_loop_done = false,
                .interactive = false,
                .terminal = undefined, // Init on run.
                .completion = undefined, // Init on run.
                .history = HistoryBuffer.init(), // No corresponding deinit.
                .line_editor = line_editor,
                .buffer_outside_history = .{},
                .arguments = arguments,
                .message_pool = message_pool,
                .io = io,
                .static_allocator = static_allocator,
            };
        }

        pub fn deinit(repl: *Repl, allocator: std.mem.Allocator) void {
            repl.static_allocator.transition_from_static_to_deinit();

            repl.line_editor.deinit();
            repl.client.deinit(allocator);
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
                std.mem.splitScalar(u8, statements, ';')
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
                                error.NoDevice,
                                error.ProcessNotFound,
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
            arguments: *std.ArrayListUnmanaged(u8),
        ) !void {
            const operation_type = switch (operation) {
                .create_accounts, .create_transfers => "create",
                .get_account_transfers, .get_account_balances => "get",
                .lookup_accounts, .lookup_transfers => "lookup",
                .query_accounts, .query_transfers => "query",
                else => unreachable,
            };
            const object_type = switch (operation) {
                .create_accounts, .lookup_accounts, .query_accounts => "accounts",
                .create_transfers, .lookup_transfers, .query_transfers => "transfers",
                .get_account_transfers => "account transfers",
                .get_account_balances => "account balances",
                else => unreachable,
            };

            if (arguments.items.len == 0) {
                try repl.fail(
                    "No {s} to {s}.\n",
                    .{ object_type, operation_type },
                );
                return;
            }

            repl.request_done = false;
            try repl.debug("Sending command: {}.\n", .{operation});

            const payload_size: u32 = @intCast(arguments.items.len);
            const buffer: []u8 = buffer: {
                arguments.expandToCapacity();
                assert(arguments.items.len == constants.message_body_size_max);
                break :buffer arguments.items;
            };
            var body_encoder = vsr.multi_batch.MultiBatchEncoder.init(buffer, .{
                .element_size = StateMachine.event_size_bytes(operation),
            });
            body_encoder.add(payload_size);
            const bytes_written = body_encoder.finish();
            assert(bytes_written > 0);

            repl.client.request(
                client_request_callback,
                @intCast(@intFromPtr(repl)),
                operation,
                buffer[0..bytes_written],
            );
        }

        fn display_object(repl: *Repl, object: anytype) !void {
            assert(@TypeOf(object.*) == tb.Account or
                @TypeOf(object.*) == tb.Transfer or
                @TypeOf(object.*) == tb.AccountBalance);

            try repl.terminal.print("{{\n", .{});
            inline for (@typeInfo(@TypeOf(object.*)).@"struct".fields, 0..) |object_field, i| {
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

                    inline for (@typeInfo(object_field.type).@"struct".fields) |flag_field| {
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

        fn client_request_completed(
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
                    const create_account_results = stdx.bytes_as_slice(
                        .exact,
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
                    const account_results = stdx.bytes_as_slice(
                        .exact,
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
                    const create_transfer_results = stdx.bytes_as_slice(
                        .exact,
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
                .lookup_transfers,
                .get_account_transfers,
                .query_transfers,
                => {
                    const transfer_results = stdx.bytes_as_slice(
                        .exact,
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
                    const get_account_balances_results = stdx.bytes_as_slice(
                        .exact,
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
                else => unreachable,
            }
        }

        fn client_request_callback(
            user_data: u128,
            operation_vsr: vsr.Operation,
            timestamp: u64,
            result: []u8,
        ) void {
            const operation = operation_vsr.cast(StateMachine);
            const reply_decoder = vsr.multi_batch.MultiBatchDecoder.init(result, .{
                .element_size = StateMachine.result_size_bytes(operation),
            }) catch unreachable;
            assert(reply_decoder.batch_count() == 1);
            client_request_completed(
                user_data,
                operation,
                timestamp,
                reply_decoder.peek(),
            ) catch |err| {
                const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
                repl.fail("Error in callback: {any}", .{err}) catch return;
            };
        }
    };
}
