const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const build_options = @import("vsr_options");

const vsr = @import("vsr.zig");
const stdx = vsr.stdx;
const constants = vsr.constants;
const IO = vsr.io.IO;
const Storage = vsr.storage.Storage;
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
    };

    pub const LookupSyntaxTree = struct {
        id: u128,
    };

    pub const ObjectSyntaxTree = union(enum) {
        account: tb.Account,
        transfer: tb.Transfer,
        id: LookupSyntaxTree,
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
                var active_value = @field(out, object_syntax_tree_field.name);
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
            .lookup_accounts => .{ .id = .{ .id = 0 } },
            .lookup_transfers => .{ .id = .{ .id = 0 } },
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
                \\Operation must be help, create_accounts, lookup_accounts,
                \\create_transfers, or lookup_transfers. Got: '{s}'.
                \\
            ,
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

        client: *Client,
        printer: Printer,

        const Repl = @This();

        fn fail(repl: *const Repl, comptime format: []const u8, arguments: anytype) !void {
            if (!repl.interactive) {
                try repl.printer.print_error(format, arguments);
                std.os.exit(1);
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
                => |operation| {
                    const state_machine_operation =
                        std.meta.stringToEnum(StateMachine.Operation, @tagName(operation));
                    assert(state_machine_operation != null);

                    try repl.send(state_machine_operation.?, statement.arguments);
                },
            }
        }

        const single_repl_input_max = 10 * 4 * 1024;
        fn do_repl(
            repl: *Repl,
            arena: *std.heap.ArenaAllocator,
        ) !void {
            try repl.printer.print("> ", .{});

            const stdin = std.io.getStdIn();
            var stdin_buffered_reader = std.io.bufferedReader(stdin.reader());
            var stdin_stream = stdin_buffered_reader.reader();

            const input = stdin_stream.readUntilDelimiterOrEofAlloc(
                arena.allocator(),
                ';',
                single_repl_input_max,
            ) catch |err| {
                repl.event_loop_done = true;
                return err;
            } orelse {
                // EOF.
                repl.event_loop_done = true;
                try repl.fail("\nExiting.\n", .{});
                return;
            };

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
                    // TODO: This will be more convenient to express
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
            const version = build_options.version;
            try repl.printer.print("TigerBeetle CLI Client " ++ version ++ "\n" ++
                \\  Hit enter after a semicolon to run a command.
                \\
                \\Examples:
                \\  create_accounts id=1 code=10 ledger=700,
                \\                  id=2 code=10 ledger=700;
                \\  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
                \\  lookup_accounts id=1;
                \\  lookup_accounts id=1, id=2;
                \\
                \\
            , .{});
        }

        pub fn run(
            arena: *std.heap.ArenaAllocator,
            addresses: []std.net.Address,
            cluster_id: u128,
            statements: []const u8,
            verbose: bool,
        ) !void {
            const allocator = arena.allocator();

            var repl = Repl{
                .client = undefined,
                .debug_logs = verbose,
                .request_done = true,
                .event_loop_done = false,
                .interactive = statements.len == 0,
                .printer = .{
                    .stderr = std.io.getStdErr().writer(),
                    .stdout = std.io.getStdOut().writer(),
                },
            };

            try repl.debug("Connecting to '{any}'.\n", .{addresses});

            const client_id = std.crypto.random.int(u128);

            var io = try IO.init(32, 0);

            var message_pool = try MessagePool.init(allocator, .client);

            var client = try Client.init(
                allocator,
                client_id,
                cluster_id,
                @as(u8, @intCast(addresses.len)),
                &message_pool,
                .{
                    .configuration = addresses,
                    .io = &io,
                },
            );
            repl.client = &client;

            if (statements.len > 0) {
                var statements_iterator = std.mem.split(u8, statements, ";");
                while (statements_iterator.next()) |statement_string| {
                    // Release allocation after every execution.
                    var execution_arena = std.heap.ArenaAllocator.init(allocator);
                    defer execution_arena.deinit();
                    var statement = Parser.parse_statement(
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
                            // TODO: This wil be more convenient to express
                            // once https://github.com/ziglang/zig/issues/2473 is
                            // in.
                            => std.os.exit(1),

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

        fn send(
            repl: *Repl,
            operation: StateMachine.Operation,
            arguments: []const u8,
        ) !void {
            const operation_type =
                if (operation == .create_accounts or
                operation == .create_transfers)
                "create"
            else
                "lookup";

            const object_type =
                if (operation == .create_accounts or
                operation == .lookup_accounts)
                "accounts"
            else
                "transfers";

            if (arguments.len == 0) {
                try repl.fail(
                    "No {s} to {s}.\n",
                    .{ object_type, operation_type },
                );
                return;
            }

            const batch = repl.client.batch_get(operation, switch (operation) {
                inline else => |op| @divExact(arguments.len, @sizeOf(StateMachine.Event(op))),
            }) catch unreachable;

            stdx.copy_disjoint(
                .exact,
                u8,
                batch.slice(),
                arguments,
            );

            repl.request_done = false;
            try repl.debug("Sending command: {}.\n", .{operation});
            repl.client.batch_submit(
                @as(u128, @intCast(@intFromPtr(repl))),
                client_request_callback,
                batch,
            );
        }

        fn display_object(repl: *Repl, object: anytype) !void {
            assert(@TypeOf(object.*) == tb.Account or @TypeOf(object.*) == tb.Transfer);

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
            const repl = @as(*Repl, @ptrFromInt(@as(usize, @intCast(user_data))));
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
                .lookup_accounts => {
                    const lookup_account_results = std.mem.bytesAsSlice(
                        tb.Account,
                        result,
                    );

                    if (lookup_account_results.len == 0) {
                        try repl.fail(
                            "Failed to lookup account: {any}\n",
                            .{LookupAccountResult.account_not_found},
                        );
                    } else {
                        for (lookup_account_results) |*account| {
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
                .lookup_transfers => {
                    const lookup_transfer_results = std.mem.bytesAsSlice(
                        tb.Transfer,
                        result,
                    );

                    if (lookup_transfer_results.len == 0) {
                        try repl.fail(
                            "Failed to lookup transfer: {any}\n",
                            .{LookupTransferResult.transfer_not_found},
                        );
                    } else {
                        for (lookup_transfer_results) |*transfer| {
                            try repl.display_object(transfer);
                        }
                    }
                },
                .get_account_transfers => unreachable,
            }
        }

        fn client_request_callback(
            user_data: u128,
            operation: StateMachine.Operation,
            result: []const u8,
        ) void {
            client_request_callback_error(
                user_data,
                operation,
                result,
            ) catch |err| {
                const repl = @as(*Repl, @ptrFromInt(@as(u64, @intCast(user_data))));
                repl.fail("Error in callback: {any}", .{err}) catch return;
            };
        }
    };
}

pub const LookupAccountResult = enum(u32) {
    ok = 0,
    account_not_found = 1,
    comptime {
        for (std.enums.values(LookupAccountResult), 0..) |result, index| {
            assert(@intFromEnum(result) == index);
        }
    }
};

pub const LookupTransferResult = enum(u32) {
    ok = 0,
    transfer_not_found = 1,
    comptime {
        for (std.enums.values(LookupTransferResult), 0..) |result, index| {
            assert(@intFromEnum(result) == index);
        }
    }
};

const null_printer = Printer{
    .stderr = null,
    .stdout = null,
};

test "repl.zig: Parser single transfer successfully" {
    var tests = [_]struct {
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

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Parser multiple transfers successfully" {
    var tests = [_]struct {
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

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.sliceAsBytes(&t.want));
    }
}

test "repl.zig: Parser single account successfully" {
    var tests = [_]struct {
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

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_accounts);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Parser multiple accounts successfully" {
    var tests = [_]struct {
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

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_accounts);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.sliceAsBytes(&t.want));
    }
}

test "repl.zig: Parser odd but correct formatting" {
    var tests = [_]struct {
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

        var statement = try Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );

        try std.testing.expectEqual(statement.operation, .create_transfers);
        try std.testing.expectEqualSlices(u8, statement.arguments, std.mem.asBytes(&t.want));
    }
}

test "repl.zig: Handle parsing errors" {
    var tests = [_]struct {
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

        var result = Parser.parse_statement(
            &arena,
            t.in,
            null_printer,
        );
        try std.testing.expectError(t.err, result);
    }
}
