const builtin = @import("builtin");
const std = @import("std");

const Docs = @import("./docs_types.zig").Docs;
const go = @import("./go/docs.zig").GoDocs;
const node = @import("./node/docs.zig").NodeDocs;

const languages = [_]Docs{ go, node };

const MarkdownWriter = struct {
    buf: *std.ArrayList(u8),
    writer: std.ArrayList(u8).Writer,

    fn init(buf: *std.ArrayList(u8)) MarkdownWriter {
        return MarkdownWriter{ .buf = buf, .writer = buf.writer() };
    }

    fn header(mw: *MarkdownWriter, comptime n: i8, content: []const u8) void {
        mw.print(("#" ** n) ++ " {s}\n\n", .{content});
    }

    fn paragraph(mw: *MarkdownWriter, content: []const u8) void {
        // Don't print empty lines.
        if (content.len == 0) {
            return;
        }
        mw.print("{s}\n\n", .{content});
    }

    fn code(mw: *MarkdownWriter, language: []const u8, content: []const u8) void {
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

    fn print(mw: *MarkdownWriter, comptime fmt: []const u8, args: anytype) void {
        mw.writer.print(fmt, args) catch unreachable;
    }

    fn reset(mw: *MarkdownWriter) void {
        mw.buf.clearRetainingCapacity();
    }

    fn diffOnDisk(mw: *MarkdownWriter, filename: []const u8) !bool {
        const file = try std.fs.cwd().createFile(filename, .{ .read = true, .truncate = false });
        const fSize = (try file.stat()).size;
        if (fSize != mw.buf.items.len) {
            return true;
        }

        var buf = std.mem.zeroes([4096]u8);
        var cursor: usize = 0;
        while (cursor < fSize) {
            var maxCanRead = if (fSize - cursor > 4096) 4096 else fSize - cursor;
            _ = try file.read(buf[0..maxCanRead]);
            if (std.mem.eql(u8, buf[0..], mw.buf.items[cursor..maxCanRead])) {
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

        const file = try std.fs.cwd().openFile(filename, .{ .write = true });
        defer file.close();

        try file.setEndPos(0);
        try file.writeAll(mw.buf.items);
    }
};

const Generator = struct {
    allocator: std.mem.Allocator,
    language: Docs,

    const DockerCommand = struct {
        cmds: []const u8,
        mount: ?[][]const u8,
        env: ?[][]const u8,
    };

    fn run(self: Generator, cmd: [][]const u8) !void {
        var cp = try std.ChildProcess.init(cmd, self.allocator);
        var res = try cp.spawnAndWait();
        switch (res) {
            .Exited => |code| {
                if (code != 0) {
                    std.process.exit(1);
                }
            },

            else => unreachable,
        }
    }

    fn run_in_docker(self: Generator, cmd: DockerCommand) !void {
        self.print(try std.fmt.allocPrint(self.allocator, "Running command in Docker: {s}", .{cmd.cmds}));

        var full_cmd = std.ArrayList([]const u8).init(self.allocator);
        try full_cmd.appendSlice(&[_][]const u8{
            "docker",
            "run",
        });
        if (cmd.mount) |mounts| {
            for (mounts) |entry| {
                try full_cmd.append("-v");
                try full_cmd.append(entry);
            }
        }

        if (cmd.env) |envs| {
            for (envs) |entry| {
                try full_cmd.append("-e");
                try full_cmd.append(entry);
            }
        }

        try full_cmd.appendSlice(&[_][]const u8{
            self.language.test_linux_docker_image,
            "bash",
            "-c",
            cmd.cmds,
        });

        self.print(try std.fmt.allocPrint(self.allocator, "[Debug] Command: {s}\n", .{full_cmd.items}));
        try self.run(full_cmd.items);
    }

    fn run_with_file_in_docker(self: Generator, file: []const u8, fileName: ?[]const u8, cmd: DockerCommand) !void {
        // Delete the directory if it already exists.
        std.fs.cwd().deleteTree("/tmp/wrk") catch {};
        std.fs.cwd().makeDir("/tmp/wrk") catch {};

        var tmp_file_name = try std.fmt.allocPrint(
            self.allocator,
            "/tmp/wrk/test.{s}",
            .{self.language.extension},
        );
        if (fileName) |name| {
            tmp_file_name = try std.fmt.allocPrint(
                self.allocator,
                "/tmp/wrk/{s}",
                .{name},
            );
        }
        self.print(try std.fmt.allocPrint(self.allocator, "Mounting in file:\n```{s}\n```", .{file}));
        var tmp_file = try std.fs.cwd().createFile(tmp_file_name, .{
            .truncate = true,
        });
        _ = try tmp_file.write(file);

        var full_cmd = std.ArrayList(u8).init(self.allocator);
        defer full_cmd.deinit();

        try full_cmd.writer().print("cd /tmp/wrk", .{});

        var install_lines = std.mem.split(u8, self.language.install_commands, "\n");
        while (install_lines.next()) |line| {
            try full_cmd.writer().print(" && {s}", .{line});
        }

        try full_cmd.writer().print(" && {s}", .{cmd.cmds});

        var mount = std.ArrayList([]const u8).init(self.allocator);
        if (cmd.mount) |m| {
            try mount.appendSlice(m);
        }
        try mount.append("/tmp/wrk:/tmp/wrk");

        try self.run_in_docker(
            .{
                .cmds = full_cmd.items,
                .mount = mount.items,
                .env = cmd.env,
            },
        );

        tmp_file.close();

        // Don't delete the temp file so the parent calling this can
        // use it if it wants. Cleanup always only happens in the
        // beginning of the function.
    }

    fn build_file_in_docker(self: Generator, file: []const u8) !void {
        try self.run_with_file_in_docker(
            file,
            null,
            .{
                .cmds = self.language.install_sample_file_build_commands,
                .mount = null,
                .env = null,
            },
        );
    }

    fn print(self: Generator, msg: []const u8) void {
        std.debug.print("[{s}] {s}\n", .{ self.language.markdown_name, msg });
    }

    fn validate(self: Generator) !void {
        // Test the sample file
        self.print("Building minimal sample file");
        try self.build_file_in_docker(self.language.install_sample_file);

        // Test major parts of sample code
        var sample = try self.make_aggregate_sample();
        self.print("Building aggregate sample file");
        try self.build_file_in_docker(sample);

        var env = std.ArrayList([]const u8).init(self.allocator);
        for (&[_][]const u8{
            "GIT_SHA",
            "GITHUB_REPOSITORY",
        }) |env_var_name| {
            if (std.os.getenv(env_var_name)) |value| {
                try env.append(try std.fmt.allocPrint(
                    self.allocator,
                    "{s}={s}",
                    .{
                        env_var_name,
                        value,
                    },
                ));
            }
        }

        try self.run_with_file_in_docker(
            self.language.developer_setup_bash_commands,
            "setup.sh",
            .{
                // Important to `bash -e` here since setup.sh won't
                // necessarily have `set -e` inside.
                .cmds = "apt-get update -y && apt-get install -y xz-utils && bash -e setup.sh",
                .env = env.items,
                .mount = null,
            },
        );
    }

    // This will not include every snippet but it includes as much as //
    // reasonable. Both so we can type-check as much as possible and also so
    // we can produce a building sample file for READMEs.
    fn make_aggregate_sample(self: Generator) ![]const u8 {
        var parts = [_][]const u8{
            self.language.test_main_prefix,
            self.language.client_object_example,
            self.language.create_accounts_example,
            self.language.account_flags_example,
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
        var aggregate = std.ArrayList(u8).init(self.allocator);
        for (parts) |part| {
            try aggregate.writer().print("{s}\n", .{part});
        }
        return aggregate.items;
    }

    fn make_and_format_aggregate_sample(self: Generator) ![]const u8 {
        var sample = try self.make_aggregate_sample();
        try self.run_with_file_in_docker(
            sample,
            null,
            .{
                .cmds = self.language.code_format_commands,
                .env = null,
                .mount = null,
            },
        );

        // This is the place where run_with_file_in_docker places the file.
        var formatted_file_name = try std.fmt.allocPrint(
            self.allocator,
            "/tmp/wrk/test.{s}",
            .{self.language.extension},
        );
        var formatted_file = try std.fs.cwd().openFile(formatted_file_name, .{
            .read = true,
        });

        const file_size = try formatted_file.getEndPos();
        var formatted = try self.allocator.alloc(u8, file_size);
        _ = try formatted_file.read(formatted);

        // Temp file cleanup

        try std.fs.cwd().deleteFile(formatted_file_name);

        return formatted;
    }

    fn generate(self: Generator, mw: *MarkdownWriter) !void {
        var language = self.language;

        mw.paragraph(
            \\This file is generated by
            \\[src/clients/docs_generate.zig](/src/clients/docs_generate.zig).
        );

        mw.header(1, language.name);
        mw.paragraph(language.description);

        mw.header(3, "Prerequisites");
        mw.paragraph(language.prerequisites);

        mw.header(2, "Setup");

        mw.commands(language.install_commands);
        mw.print("Create `test.{s}` and copy this into it:\n\n", .{language.extension});
        mw.code(language.markdown_name, language.install_sample_file);
        mw.paragraph("And run:");
        mw.commands(language.install_sample_file_test_commands);

        mw.paragraph(language.install_documentation);

        if (language.examples.len != 0) {
            mw.header(2, "Examples");
            mw.paragraph(language.examples);
        }

        mw.header(2, "Creating a Client");
        mw.paragraph(
            \\A client is created with a cluster ID and replica
            \\addresses for all replicas in the cluster. The cluster
            \\ID and replica addresses are both chosen by the system that
            \\starts the TigerBeetle cluster.
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
            \\For example, to link `account0` and `account1`, where `account0`
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
        mw.paragraph(
            \\The example above shows that the account in index 1 failed
            \\with error 1. This error here means that `account1` and
            \\`account3` were created successfully. But `account2` was not
            \\created.
        );
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
            \\
            \\In this example, account `137` exists while account `138` does not.
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
            \\
            \\In this example, transfer `1` exists while transfer `2` does not.
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
        // Bash setup
        mw.header(3, "On Linux and macOS");
        mw.commands(language.developer_setup_bash_commands);

        // Windows setup
        mw.header(3, "On Windows");
        if (language.developer_setup_windows_commands.len > 0) {
            mw.commands(language.developer_setup_windows_commands);
        } else {
            mw.paragraph("Unsupported.");
        }

        try mw.save(language.readme);
    }
};

pub fn main() !void {
    var args = std.process.args();
    var skipLanguage = [_]bool{false} ** languages.len;
    var validate = true;
    var generate = true;
    while (args.nextPosix()) |arg| {
        if (std.mem.eql(u8, arg, "--only")) {
            var filter = args.nextPosix().?;
            skipLanguage = [_]bool{true} ** languages.len;
            for (languages) |language, i| {
                if (std.mem.indexOf(u8, filter, language.markdown_name)) |_| {
                    skipLanguage[i] = false;
                }
            }
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

        var generator = Generator{ .allocator = allocator, .language = language };
        if (validate) {
            generator.print("Validating");
            try generator.validate();
        }

        if (generate) {
            generator.print("Generating");
            try generator.generate(&mw);
        }
    }
}
