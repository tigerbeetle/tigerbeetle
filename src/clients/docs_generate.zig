const builtin = @import("builtin");
const std = @import("std");

const Docs = @import("./docs_types.zig").Docs;
const go = @import("./go/docs.zig").GoDocs;
const node = @import("./node/docs.zig").NodeDocs;
const java = @import("./java/docs.zig").JavaDocs;
const samples = @import("./docs_samples.zig").samples;

pub const cmd_sep = if (builtin.os.tag == .windows) ";" else "&&";

const languages = [_]Docs{ go, node, java };

const MarkdownWriter = struct {
    buf: *std.ArrayList(u8),
    writer: std.ArrayList(u8).Writer,

    fn init(buf: *std.ArrayList(u8)) MarkdownWriter {
        return MarkdownWriter{
            .buf = buf,
            .writer = buf.writer(),
        };
    }

    fn header(
        mw: *MarkdownWriter,
        comptime n: i8,
        content: []const u8,
    ) void {
        mw.print(("#" ** n) ++ " {s}\n\n", .{content});
    }

    fn paragraph(mw: *MarkdownWriter, content: []const u8) void {
        // Don't print empty lines.
        if (content.len == 0) {
            return;
        }
        mw.print("{s}\n\n", .{content});
    }

    fn code(
        mw: *MarkdownWriter,
        language: []const u8,
        content: []const u8,
    ) void {
        // Don't print empty lines.
        if (content.len == 0) {
            return;
        }
        mw.print("```{s}\n{s}\n```\n\n", .{ language, content });
    }

    fn commands(mw: *MarkdownWriter, content: []const u8) void {
        mw.print("```console\n", .{});
        var splits = std.mem.split(u8, content, "\n");
        while (splits.next()) |chunk| {
            mw.print("$ {s}\n", .{chunk});
        }

        mw.print("```\n\n", .{});
    }

    fn print(
        mw: *MarkdownWriter,
        comptime fmt: []const u8,
        args: anytype,
    ) void {
        mw.writer.print(fmt, args) catch unreachable;
    }

    fn reset(mw: *MarkdownWriter) void {
        mw.buf.clearRetainingCapacity();
    }

    fn diffOnDisk(mw: *MarkdownWriter, filename: []const u8) !bool {
        const file = std.fs.cwd().createFile(
            filename,
            .{ .read = true, .truncate = false },
        ) catch |e| {
            std.debug.print("Could not open file for reading: {s}.\n", .{filename});
            return e;
        };
        const fSize = (try file.stat()).size;
        if (fSize != mw.buf.items.len) {
            return true;
        }

        var buf = std.mem.zeroes([4096]u8);
        var cursor: usize = 0;
        while (cursor < fSize) {
            var maxCanRead = if (fSize - cursor > 4096) 4096 else fSize - cursor;
            // Phil: Sometimes this infinite loops and returns `0` but
            // I don't know why. Allowing it to just overwrite solves the problem.
            var n = try file.read(buf[0..maxCanRead]);
            if (n == 0 and maxCanRead != n) {
                return true;
            }

            if (std.mem.eql(u8, buf[0..], mw.buf.items[cursor..n])) {
                return false;
            }
        }

        return true;
    }

    // save() only actually writes the buffer to disk if it has
    // changed compared to what's on disk, so that file modify time stays
    // reasonable.
    fn save(mw: *MarkdownWriter, filename: []const u8) !void {
        var diff = try mw.diffOnDisk(filename);
        if (!diff) {
            return;
        }

        const file = try std.fs.cwd().openFile(
            filename,
            .{ .write = true },
        );
        defer file.close();

        // First truncate(0) the file.
        try file.setEndPos(0);

        // Then write what we need.
        try file.writeAll(mw.buf.items);
    }
};

pub fn exec(
    arena: *std.heap.ArenaAllocator,
    cmd: []const []const u8,
) !std.ChildProcess.ExecResult {
    var res = try std.ChildProcess.exec(.{
        .allocator = arena.allocator(),
        .argv = cmd,
    });
    switch (res.term) {
        .Exited => |code| {
            if (code != 0) {
                return error.ExecCommandFailed;
            }
        },

        else => return error.UnknownCase,
    }

    return res;
}

pub fn run_with_env(
    arena: *std.heap.ArenaAllocator,
    cmd: []const []const u8,
    env: []const []const u8,
) !void {
    std.debug.print("Running:", .{});
    var i: u32 = 0;
    while (i < env.len) : (i += 2) {
        std.debug.print(" {s}={s}", .{ env[i], env[i + 1] });
    }
    for (cmd) |c| {
        std.debug.print(" {s}", .{c});
    }
    std.debug.print("\n", .{});

    var cp = try std.ChildProcess.init(cmd, arena.allocator());

    var env_map = try std.process.getEnvMap(arena.allocator());
    i = 0;
    while (i < env.len) : (i += 2) {
        try env_map.put(env[i], env[i + 1]);
    }
    cp.env_map = &env_map;

    var res = try cp.spawnAndWait();
    switch (res) {
        .Exited => |code| {
            if (code != 0) {
                return error.RunCommandFailed;
            }
        },

        else => return error.UnknownCase,
    }
}

// Needs to have -e for `sh` otherwise the command might not fail.
pub fn shell_wrap(arena: *std.heap.ArenaAllocator, cmd: []const u8) ![]const []const u8 {
    var wrapped = std.ArrayList([]const u8).init(arena.allocator());
    if (builtin.os.tag == .windows) {
        try wrapped.append("powershell");
    } else {
        try wrapped.append("sh");
        try wrapped.append("-e");
    }
    try wrapped.append("-c");
    try wrapped.append(cmd);
    return wrapped.items;
}

pub fn run_shell_with_env(
    arena: *std.heap.ArenaAllocator,
    cmd: []const u8,
    env: []const []const u8,
) !void {
    try run_with_env(
        arena,
        try shell_wrap(arena, cmd),
        env,
    );
}

pub fn run(
    arena: *std.heap.ArenaAllocator,
    cmd: []const []const u8,
) !void {
    try run_with_env(arena, cmd, &.{});
}

pub fn run_shell(
    arena: *std.heap.ArenaAllocator,
    cmd: []const u8,
) !void {
    try run_shell_with_env(arena, cmd, &.{});
}

pub const TmpDir = struct {
    dir: std.testing.TmpDir,
    path: []const u8,

    pub fn init(arena: *std.heap.ArenaAllocator) !TmpDir {
        var tmp_dir = std.testing.tmpDir(.{});
        return TmpDir{
            .dir = tmp_dir,
            .path = try tmp_dir.dir.realpathAlloc(arena.allocator(), "."),
        };
    }

    pub fn deinit(self: *TmpDir) void {
        self.dir.cleanup();
    }
};

pub fn git_root(arena: *std.heap.ArenaAllocator) ![]const u8 {
    var prefix: []const u8 = "";
    var tries: i32 = 0;
    while (tries < 100) {
        var dir = std.fs.cwd().openDir(
            try std.fmt.allocPrint(
                arena.allocator(),
                "{s}.git",
                .{prefix},
            ),
            .{},
        ) catch {
            prefix = try std.fmt.allocPrint(
                arena.allocator(),
                "../{s}",
                .{prefix},
            );
            tries += 1;
            continue;
        };

        // When looking up realpathAlloc, it can't be an empty string.
        if (prefix.len == 0) {
            prefix = ".";
        }

        const path = try std.fs.cwd().realpathAlloc(arena.allocator(), prefix);
        dir.close();
        return path;
    }

    std.debug.print("Failed to find .git root of TigerBeetle repo.\n", .{});
    return error.CouldNotFindGitRoot;
}

const Generator = struct {
    arena: *std.heap.ArenaAllocator,
    language: Docs,
    test_file_name: []const u8,

    fn init(
        arena: *std.heap.ArenaAllocator,
        language: Docs,
    ) !Generator {
        var test_file_name = language.test_file_name;
        if (test_file_name.len == 0) {
            test_file_name = "main";
        }

        return Generator{
            .arena = arena,
            .language = language,
            .test_file_name = test_file_name,
        };
    }

    fn write_shell_newlines_into_single_line(into: *std.ArrayList(u8), from: []const u8) !void {
        if (from.len == 0) {
            return;
        }

        var lines = std.mem.split(u8, from, "\n");
        while (lines.next()) |line| {
            try into.writer().print("{s} {s} ", .{ line, cmd_sep });
        }
    }

    fn ensure_path(self: Generator, path: []const u8) !void {
        self.printf(
            "[Debug] Ensuring path: {s}",
            .{path},
        );
        try std.fs.cwd().makePath(path);
    }

    fn build_file_within_project(self: Generator, tmp_dir: TmpDir, file: []const u8, run_setup_tests: bool) !void {
        var tmp_file_name = self.sprintf(
            "{s}/{s}{s}.{s}",
            .{
                tmp_dir.path,
                self.language.test_source_path,
                self.test_file_name,
                self.language.extension,
            },
        );
        try self.ensure_path(std.fs.path.dirname(tmp_file_name).?);
        var tmp_file = try std.fs.cwd().createFile(tmp_file_name, .{
            .truncate = true,
        });
        defer tmp_file.close();
        _ = try tmp_file.write(file);

        // Some languages (Java) have an additional project file
        // (pom.xml) they need to have available.
        if (self.language.project_file.len > 0) {
            const project_file = try std.fs.cwd().createFile(
                self.sprintf(
                    "{s}/{s}",
                    .{ tmp_dir.path, self.language.project_file_name },
                ),
                .{ .truncate = true },
            );
            defer project_file.close();

            _ = try project_file.write(self.language.project_file);
        }

        var cmd = std.ArrayList(u8).init(self.arena.allocator());
        defer cmd.deinit();

        var env = std.ArrayList([]const u8).init(self.arena.allocator());
        defer env.deinit();
        try env.appendSlice(&[_][]const u8{
            "TB_ROOT", try git_root(self.arena),
        });
        try env.appendSlice(&[_][]const u8{
            "SAMPLE_ROOT", tmp_dir.path,
        });

        // First run general setup within already cloned repo
        try write_shell_newlines_into_single_line(
            &cmd,
            if (builtin.os.tag == .windows)
                self.language.developer_setup_pwsh_commands
            else
                self.language.developer_setup_sh_commands,
        );
        try write_shell_newlines_into_single_line(
            &cmd,
            self.language.current_commit_pre_install_commands,
        );

        try cmd.appendSlice("echo ok");
        try run_shell_with_env(
            self.arena,
            cmd.items,
            env.items,
        );

        // Then run project, within tmp dir
        try std.os.chdir(tmp_dir.path);

        cmd.clearRetainingCapacity();
        try write_shell_newlines_into_single_line(
            &cmd,
            self.language.install_commands,
        );
        try write_shell_newlines_into_single_line(
            &cmd,
            self.language.current_commit_post_install_commands,
        );

        try write_shell_newlines_into_single_line(
            &cmd,
            self.language.build_commands,
        );

        // The above commands all end with ` {cmd_sep} `
        try cmd.appendSlice("echo ok");

        if (run_setup_tests) {
            try env.appendSlice(&[_][]const u8{ "TEST", "true" });
        }

        try run_shell_with_env(
            self.arena,
            cmd.items,
            env.items,
        );
    }

    fn print(self: Generator, msg: []const u8) void {
        std.debug.print("[{s}] {s}\n", .{
            self.language.markdown_name,
            msg,
        });
    }

    fn printf(self: Generator, comptime msg: []const u8, obj: anytype) void {
        self.print(self.sprintf(msg, obj));
    }

    fn sprintf(self: Generator, comptime msg: []const u8, obj: anytype) []const u8 {
        return std.fmt.allocPrint(
            self.arena.allocator(),
            msg,
            obj,
        ) catch unreachable;
    }

    fn validate_minimal(self: Generator) !void {
        // Test the sample file
        self.print("Building minimal sample file");
        var tmp_dir = try TmpDir.init(self.arena);
        defer tmp_dir.deinit();
        try self.build_file_within_project(tmp_dir, self.language.install_sample_file, true);
    }

    fn validate_aggregate(self: Generator) !void {
        // Test major parts of sample code
        var sample = try self.make_aggregate_sample();
        self.print("Building aggregate sample file");
        var tmp_dir = try TmpDir.init(self.arena);
        defer tmp_dir.deinit();
        try self.build_file_within_project(tmp_dir, sample, false);
    }

    const tests = [_]struct {
        name: []const u8,
        validate: fn (Generator) anyerror!void,
    }{
        .{
            .name = "minimal",
            .validate = validate_minimal,
        },
        .{
            .name = "aggregate",
            .validate = validate_aggregate,
        },
    };

    // This will not include every snippet but it includes as much as //
    // reasonable. Both so we can type-check as much as possible and also so
    // we can produce a building sample file for READMEs.
    fn make_aggregate_sample(self: Generator) ![]const u8 {
        var parts = [_][]const u8{
            self.language.test_main_prefix,
            self.language.client_object_example,
            self.language.create_accounts_example,
            self.language.account_flags_example,
            self.language.create_accounts_errors_example,
            self.language.lookup_accounts_example,
            self.language.create_transfers_example,
            self.language.create_transfers_errors_example,
            self.language.transfer_flags_link_example,
            self.language.transfer_flags_post_example,
            self.language.transfer_flags_void_example,
            self.language.lookup_transfers_example,
            self.language.linked_events_example,
            self.language.test_main_suffix,
        };
        var aggregate = std.ArrayList(u8).init(self.arena.allocator());
        for (parts) |part| {
            try aggregate.writer().print("{s}\n", .{part});
        }
        return aggregate.items;
    }

    fn generate_language_setup_steps(self: Generator, mw: *MarkdownWriter, directory_info: []const u8, include_project_file: bool) void {
        var language = self.language;

        const windowsSupported: []const u8 = if (language.developer_setup_pwsh_commands.len > 0)
            " and Windows"
        else
            "";
        mw.print(
            \\Linux >= 5.6 is the only production environment we
            \\support. But for ease of development we also support macOS{s}.
            \\
        , .{windowsSupported});
        mw.paragraph(language.prerequisites);

        mw.header(2, "Setup");

        mw.paragraph(directory_info);

        if (language.project_file.len > 0 and include_project_file) {
            mw.print(
                "Then create `{s}` and copy this into it:\n\n",
                .{language.project_file_name},
            );
            mw.code(language.markdown_name, language.project_file);
        }

        mw.paragraph("Then, install the TigerBeetle client:");
        mw.commands(language.install_commands);
    }

    fn generate_main_readme(self: Generator, mw: *MarkdownWriter) !void {
        var language = self.language;

        mw.print(
            \\---
            \\title: {s}
            \\---
            \\
            \\This file is generated by
            \\[/src/clients/docs_generate.zig](/src/clients/docs_generate.zig).
            \\
        , .{language.proper_name});

        mw.header(1, language.name);
        mw.paragraph(language.description);

        mw.header(3, "Prerequisites");
        self.generate_language_setup_steps(
            mw,
            \\First, create a directory for your project and `cd` into the directory.
        ,
            true,
        );

        mw.print("Now, create `{s}{s}.{s}` and copy this into it:\n\n", .{
            self.language.test_source_path,
            self.test_file_name,
            language.extension,
        });
        mw.code(language.markdown_name, language.install_sample_file);
        mw.paragraph("Finally, build and run:");
        mw.commands(language.run_commands);

        mw.paragraph(
            \\Now that all prerequisites and dependencies are correctly set
            \\up, let's dig into using TigerBeetle.
            ,
        );

        mw.paragraph(language.install_documentation);

        mw.header(2, "Sample projects");
        mw.paragraph(
            \\This document is primarily a reference guide to
            \\the client. Below are various sample projects demonstrating
            \\features of TigerBeetle.
            ,
        );
        // Absolute paths here are necessary for resolving within the docs site.
        for (samples) |sample| {
            mw.print("* [{s}](/src/clients/{s}/samples/{s}/): {s}\n", .{
                sample.proper_name,
                language.directory,
                sample.directory,
                sample.short_description,
            });
        }

        if (language.examples.len != 0) {
            mw.paragraph(language.examples);
        }

        mw.header(2, "Creating a Client");
        mw.paragraph(
            \\A client is created with a cluster ID and replica
            \\addresses for all replicas in the cluster. The cluster
            \\ID and replica addresses are both chosen by the system that
            \\starts the TigerBeetle cluster.
            \\
            \\Clients are thread-safe. But for better
            \\performance, a single instance should be shared between
            \\multiple concurrent tasks.
            \\
            \\Multiple clients are useful when connecting to more than
            \\one TigerBeetle cluster.
            \\
            \\In this example the cluster ID is `0` and there are
            \\three replicas running on ports `3001`, `3002`, and
            \\`3003`.
        );
        mw.code(language.markdown_name, language.client_object_example);
        mw.paragraph(language.client_object_documentation);

        mw.paragraph(
            \\The following are valid addresses:
            \\* `3000` (interpreted as `127.0.0.1:3000`)
            \\* `127.0.0.1:3000` (interpreted as `127.0.0.1:3000`)
            \\* `127.0.0.1` (interpreted as `127.0.0.1:3001`, `3001` is the default port)
        );

        mw.header(2, "Creating Accounts");
        mw.paragraph(
            \\See details for account fields in the [Accounts
            \\reference](https://docs.tigerbeetle.com/reference/accounts).
        );
        mw.code(language.markdown_name, language.create_accounts_example);
        mw.paragraph(language.create_accounts_documentation);

        mw.header(3, "Account Flags");
        mw.paragraph(
            \\The account flags value is a bitfield. See details for
            \\these flags in the [Accounts
            \\reference](https://docs.tigerbeetle.com/reference/accounts#flags).
        );
        mw.paragraph(language.account_flags_documentation);

        mw.paragraph(
            \\For example, to link two accounts where the first account
            \\additionally has the `debits_must_not_exceed_credits` constraint:
        );
        mw.code(language.markdown_name, language.account_flags_example);

        mw.header(3, "Response and Errors");
        mw.paragraph(
            \\The response is an empty array if all accounts were
            \\created successfully. If the response is non-empty, each
            \\object in the response array contains error information
            \\for an account that failed. The error object contains an
            \\error code and the index of the account in the request
            \\batch.
            \\
            \\See all error conditions in the [create_accounts
            \\reference](https://docs.tigerbeetle.com/reference/operations/create_accounts).
        );

        mw.code(language.markdown_name, language.create_accounts_errors_example);

        mw.paragraph(language.create_accounts_errors_documentation);

        mw.header(2, "Account Lookup");
        mw.paragraph(
            \\Account lookup is batched, like account creation. Pass
            \\in all IDs to fetch. The account for each matched ID is returned.
            \\
            \\If no account matches an ID, no object is returned for
            \\that account. So the order of accounts in the response is
            \\not necessarily the same as the order of IDs in the
            \\request. You can refer to the ID field in the response to
            \\distinguish accounts.
        );
        mw.code(language.markdown_name, language.lookup_accounts_example);

        mw.header(2, "Create Transfers");
        mw.paragraph(
            \\This creates a journal entry between two accounts.
            \\
            \\See details for transfer fields in the [Transfers
            \\reference](https://docs.tigerbeetle.com/reference/transfers).
        );
        mw.code(language.markdown_name, language.create_transfers_example);

        mw.header(3, "Response and Errors");
        mw.paragraph(
            \\The response is an empty array if all transfers were created
            \\successfully. If the response is non-empty, each object in the
            \\response array contains error information for an transfer that
            \\failed. The error object contains an error code and the index of the
            \\transfer in the request batch.
            \\
            \\See all error conditions in the [create_transfers
            \\reference](https://docs.tigerbeetle.com/reference/operations/create_transfers).
        );
        mw.code(language.markdown_name, language.create_transfers_errors_example);

        mw.paragraph(language.create_transfers_errors_documentation);

        mw.header(2, "Batching");
        mw.paragraph(
            \\TigerBeetle performance is maximized when you batch
            \\API requests. The client does not do this automatically for
            \\you. So, for example, you *can* insert 1 million transfers
            \\one at a time like so:
        );
        mw.code(language.markdown_name, language.no_batch_example);
        mw.paragraph(
            \\But the insert rate will be a *fraction* of
            \\potential. Instead, **always batch what you can**.
            \\
            \\The maximum batch size is set in the TigerBeetle server. The default
            \\is 8191.
        );
        mw.code(language.markdown_name, language.batch_example);

        mw.header(3, "Queues and Workers");
        mw.paragraph(
            \\If you are making requests to TigerBeetle from workers
            \\pulling jobs from a queue, you can batch requests to
            \\TigerBeetle by having the worker act on multiple jobs from
            \\the queue at once rather than one at a time. i.e. pulling
            \\multiple jobs from the queue rather than just one.
        );

        mw.header(2, "Transfer Flags");
        mw.paragraph(
            \\The transfer `flags` value is a bitfield. See details for these flags in
            \\the [Transfers
            \\reference](https://docs.tigerbeetle.com/reference/transfers#flags).
        );
        mw.paragraph(language.transfer_flags_documentation);
        mw.paragraph("For example, to link `transfer0` and `transfer1`:");
        mw.code(language.markdown_name, language.transfer_flags_link_example);

        mw.header(3, "Two-Phase Transfers");
        mw.paragraph(
            \\Two-phase transfers are supported natively by toggling the appropriate
            \\flag. TigerBeetle will then adjust the `credits_pending` and
            \\`debits_pending` fields of the appropriate accounts. A corresponding
            \\post pending transfer then needs to be sent to post or void the
            \\transfer.
        );
        mw.header(4, "Post a Pending Transfer");
        mw.paragraph(
            \\With `flags` set to `post_pending_transfer`,
            \\TigerBeetle will post the transfer. TigerBeetle will atomically roll
            \\back the changes to `debits_pending` and `credits_pending` of the
            \\appropriate accounts and apply them to the `debits_posted` and
            \\`credits_posted` balances.
        );
        mw.code(language.markdown_name, language.transfer_flags_post_example);

        mw.header(4, "Void a Pending Transfer");
        mw.paragraph(
            \\In contrast, with `flags` set to `void_pending_transfer`,
            \\TigerBeetle will void the transfer. TigerBeetle will roll
            \\back the changes to `debits_pending` and `credits_pending` of the
            \\appropriate accounts and **not** apply them to the `debits_posted` and
            \\`credits_posted` balances.
        );
        mw.code(language.markdown_name, language.transfer_flags_void_example);

        mw.header(2, "Transfer Lookup");
        mw.paragraph(
            \\NOTE: While transfer lookup exists, it is not a flexible query API. We
            \\are developing query APIs and there will be new methods for querying
            \\transfers in the future.
            \\
            \\Transfer lookup is batched, like transfer creation. Pass in all `id`s to
            \\fetch, and matched transfers are returned.
            \\
            \\If no transfer matches an `id`, no object is returned for that
            \\transfer. So the order of transfers in the response is not necessarily
            \\the same as the order of `id`s in the request. You can refer to the
            \\`id` field in the response to distinguish transfers.
        );
        mw.code(language.markdown_name, language.lookup_transfers_example);

        mw.header(2, "Linked Events");
        mw.paragraph(
            \\When the `linked` flag is specified for an account when creating accounts or
            \\a transfer when creating transfers, it links that event with the next event in the
            \\batch, to create a chain of events, of arbitrary length, which all
            \\succeed or fail together. The tail of a chain is denoted by the first
            \\event without this flag. The last event in a batch may therefore never
            \\have the `linked` flag set as this would leave a chain
            \\open-ended. Multiple chains or individual events may coexist within a
            \\batch to succeed or fail independently.
            \\
            \\Events within a chain are executed within order, or are rolled back on
            \\error, so that the effect of each event in the chain is visible to the
            \\next, and so that the chain is either visible or invisible as a unit
            \\to subsequent events after the chain. The event that was the first to
            \\break the chain will have a unique error result. Other events in the
            \\chain will have their error result set to `linked_event_failed`.
        );
        mw.code(language.markdown_name, language.linked_events_example);

        mw.header(2, "Development Setup");
        // Shell setup
        mw.header(3, "On Linux and macOS");
        mw.paragraph("In a POSIX shell run:");
        mw.commands(
            self.sprintf("{s}{s}", .{
                \\git clone https://github.com/tigerbeetledb/tigerbeetle
                \\cd tigerbeetle
                \\git submodule update --init --recursive
                \\./scripts/install_zig.sh
                \\
                ,
                language.developer_setup_sh_commands,
            }),
        );

        // Windows setup
        mw.header(3, "On Windows");
        if (language.developer_setup_pwsh_commands.len > 0) {
            mw.paragraph("In PowerShell run:");
            mw.commands(
                self.sprintf("{s}{s}", .{
                    \\git clone https://github.com/tigerbeetledb/tigerbeetle
                    \\cd tigerbeetle
                    \\git submodule update --init --recursive
                    \\.\scripts\install_zig.bat
                    \\
                    ,
                    language.developer_setup_pwsh_commands,
                }),
            );
        } else {
            mw.paragraph("Not yet supported.");
        }

        const root = git_root(self.arena);
        try mw.save(self.sprintf("{s}/src/clients/{s}/README.md", .{ root, language.directory }));
    }

    fn generate_sample_readmes(self: Generator, mw: *MarkdownWriter) !void {
        var language = self.language;

        for (samples) |sample| {
            mw.reset();
            mw.paragraph(
                \\This file is generated by
                \\[/src/clients/docs_generate.zig](/src/clients/docs_generate.zig).
                ,
            );

            var main_file_name = if (std.mem.eql(u8, language.directory, "go") or
                std.mem.eql(u8, language.directory, "node"))
                "main"
            else
                "Main";

            mw.print(
                \\# {s} {s} Sample
                \\
                \\Code for this sample is in [./{s}{s}.{s}](./{s}{s}.{s}).
                \\
                \\
            , .{
                sample.proper_name,
                language.proper_name,
                language.test_source_path,
                main_file_name,
                language.extension,
                language.test_source_path,
                main_file_name,
                language.extension,
            });

            mw.header(2, "Prerequisites");

            self.generate_language_setup_steps(
                mw,
                self.sprintf(
                    "First, clone this repo and `cd` into `tigerbeetle/src/clients/{s}/samples/{s}`.",
                    .{
                        language.directory,
                        sample.directory,
                    },
                ),
                false,
            );

            mw.header(2, "Start the TigerBeetle server");
            mw.paragraph(
                \\Follow steps in the repo README to start a **single
                \\server** [from a single
                \\binary](/README.md#single-binary) or [in a Docker
                \\container](/README.md#with-docker).
                \\
                \\If you are not running on port `localhost:3000`, set
                \\the environment variable `TB_ADDRESS` to the full
                \\address of the TigerBeetle server you started.
            );

            mw.header(2, "Run this sample");
            mw.paragraph("Now you can run this sample:");
            mw.commands(language.run_commands);

            mw.header(2, "Walkthrough");
            mw.paragraph("Here's what this project does.");
            mw.paragraph(sample.long_description);

            const root = try git_root(self.arena);
            try mw.save(self.sprintf("{s}/src/clients/{s}/samples/{s}/README.md", .{
                root,
                language.directory,
                sample.directory,
            }));
        }
    }
};

pub fn main() !void {
    var skipLanguage = [_]bool{false} ** languages.len;
    var validate = true;
    var generate = true;
    var validateOnly: []const u8 = "";

    var global_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer global_arena.deinit();

    var args = std.process.args();
    _ = args.next(global_arena.allocator());
    while (args.next(global_arena.allocator())) |arg_or_err| {
        const arg = arg_or_err catch {
            std.debug.print("Could not parse all arguments.\n", .{});
            return error.CouldNotParseArguments;
        };

        if (std.mem.eql(u8, arg, "--language")) {
            var filter = try (args.next(global_arena.allocator()).?);
            skipLanguage = [_]bool{true} ** languages.len;
            for (languages) |language, i| {
                if (std.mem.eql(u8, filter, language.directory)) {
                    skipLanguage[i] = false;
                }
            }
        }

        if (std.mem.eql(u8, arg, "--validate")) {
            validateOnly = try (args.next(global_arena.allocator()).?);
        }

        if (std.mem.eql(u8, arg, "--no-validate")) {
            validate = false;
        }

        if (std.mem.eql(u8, arg, "--no-generate")) {
            generate = false;
        }
    }

    for (languages) |language, i| {
        if (skipLanguage[i]) {
            continue;
        }

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();

        const allocator = arena.allocator();
        var buf = std.ArrayList(u8).init(allocator);
        var mw = MarkdownWriter.init(&buf);

        var generator = try Generator.init(&arena, language);
        if (validate) {
            generator.print("Validating");

            for (Generator.tests) |t| {
                var found = false;
                if (validateOnly.len > 0) {
                    var parts = std.mem.split(u8, validateOnly, ",");
                    while (parts.next()) |name| {
                        if (std.mem.eql(u8, name, t.name)) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        generator.printf(
                            "Skipping test [{s}]",
                            .{t.name},
                        );
                        continue;
                    }
                }

                const root = try git_root(&arena);
                try std.os.chdir(root);
                try t.validate(generator);
            }
        }

        if (generate) {
            generator.print("Generating main README");
            try generator.generate_main_readme(&mw);

            generator.print("Generating sample READMEs");
            try generator.generate_sample_readmes(&mw);
        }
    }
}
