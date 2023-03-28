const builtin = @import("builtin");
const std = @import("std");

const Docs = @import("./docs_types.zig").Docs;
const go = @import("./go/docs.zig").GoDocs;
const node = @import("./node/docs.zig").NodeDocs;
const java = @import("./java/docs.zig").JavaDocs;

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
        const file = try std.fs.cwd().createFile(
            filename,
            .{ .read = true, .truncate = false },
        );
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

        try file.setEndPos(0);
        try file.writeAll(mw.buf.items);
    }
};

const Generator = struct {
    allocator: std.mem.Allocator,
    language: Docs,
    test_file_name: []const u8,

    const DockerCommand = struct {
        cmds: []const u8,
        mount: ?[][]const u8,
        env: ?[][]const u8,
    };

    const docker_mount_path = "/tmp/wrk";

    fn init(
        allocator: std.mem.Allocator,
        language: Docs,
    ) !Generator {
        var test_file_name = language.test_file_name;
        if (test_file_name.len == 0) {
            test_file_name = "test";
        }

        return Generator{
            .allocator = allocator,
            .language = language,
            .test_file_name = test_file_name,
        };
    }

    const TmpDir = struct {
        dir: std.testing.TmpDir,
        path: []const u8,

        fn init(allocator: std.mem.Allocator) !TmpDir {
            var tmp_dir = std.testing.tmpDir(.{});
            return TmpDir{
                .dir = tmp_dir,
                .path = try tmp_dir.dir.realpathAlloc(allocator, "."),
            };
        }

        fn cleanup(self: *TmpDir) void {
            self.dir.cleanup();
        }
    };

    fn exec(
        self: Generator,
        cmd: []const []const u8,
    ) !std.ChildProcess.ExecResult {
        var res = try std.ChildProcess.exec(.{
            .allocator = self.allocator,
            .argv = cmd,
        });
        switch (res.term) {
            .Exited => |code| {
                if (code != 0) {
                    std.os.exit(1);
                }
            },

            else => unreachable,
        }

        return res;
    }

    fn run(self: Generator, cmd: []const []const u8) !void {
        var cp = try std.ChildProcess.init(cmd, self.allocator);
        var res = try cp.spawnAndWait();
        switch (res) {
            .Exited => |code| {
                if (code != 0) {
                    std.os.exit(1);
                }
            },

            else => unreachable,
        }
    }

    fn run_in_docker(self: Generator, cmd: DockerCommand) !void {
        self.printf(
            "Running command in Docker: {s}",
            .{cmd.cmds},
        );

        var full_cmd = std.ArrayList([]const u8).init(self.allocator);
        defer full_cmd.deinit();

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
            "sh",
            "-c",
            cmd.cmds,
        });

        self.printf(
            "[Debug] Command: {s}\n",
            .{full_cmd.items},
        );
        try self.run(full_cmd.items);
    }

    fn ensure_path(self: Generator, path: []const u8) !void {
        self.printf(
            "[Debug] Ensuring path: {s}",
            .{path},
        );
        var dir = try std.fs.openDirAbsolute("/", .{});
        defer dir.close();
        try dir.makePath(path);
    }

    fn run_with_file_in_docker(
        self: Generator,
        tmp_dir: TmpDir,
        file: []const u8,
        file_name: ?[]const u8,
        cmd: DockerCommand,
    ) !void {
        var tmp_file_name = self.sprintf(
            "{s}/{s}{s}.{s}",
            .{
                tmp_dir.path,
                self.language.test_source_path,
                self.test_file_name,
                self.language.extension,
            },
        );
        if (file_name) |name| {
            tmp_file_name = self.sprintf(
                "{s}/{s}",
                .{ tmp_dir.path, name },
            );
        }

        try self.ensure_path(std.fs.path.dirname(tmp_file_name).?);

        self.printf(
            "Mounting in file ({s}):\n```\n{s}\n```",
            .{ tmp_file_name, file },
        );

        var tmp_file = try std.fs.cwd().createFile(tmp_file_name, .{
            .truncate = true,
        });
        defer tmp_file.close();
        _ = try tmp_file.write(file);

        var full_cmd = std.ArrayList(u8).init(self.allocator);
        defer full_cmd.deinit();

        try full_cmd.writer().print(
            "cd {s} && apk add -U build-base git xz",
            .{docker_mount_path},
        );
        if (self.language.install_prereqs.len > 0) {
            try full_cmd.writer().print(
                " && {s}",
                .{self.language.install_prereqs},
            );
        }

        try full_cmd.writer().print(" && {s}", .{cmd.cmds});

        var mount = std.ArrayList([]const u8).init(self.allocator);
        defer mount.deinit();
        if (cmd.mount) |m| {
            try mount.appendSlice(m);
        }
        try mount.append(self.sprintf(
            "{s}:{s}",
            .{ tmp_dir.path, docker_mount_path },
        ));

        try self.run_in_docker(
            .{
                .cmds = full_cmd.items,
                .mount = mount.items,
                .env = cmd.env,
            },
        );
    }

    fn get_current_commit_and_repo_env(self: Generator) !std.ArrayList([]const u8) {
        var env = std.ArrayList([]const u8).init(self.allocator);

        for (&[_][]const u8{
            "GIT_SHA",
            // TODO: this won't run correctly on forks that are testing
            // *locally*. But it will run correctly for forks being tested in
            // Github Actions.
            // Could fix it by parsing `git remote -v` or something.
            "GITHUB_REPOSITORY",
        }) |env_var_name| {
            if (std.os.getenv(env_var_name)) |value| {
                try env.append(self.sprintf(
                    "{s}={s}",
                    .{
                        env_var_name,
                        value,
                    },
                ));
            }
        }

        // GIT_SHA is correct in CI but locally it won't exist. So
        // if we're local (GIT_SHA isn't set) then grab the current
        // commit. The current commit isn't correct in CI because CI
        // is run against pseudo/temporary branches.
        if (env.items.len == 0 or !std.mem.startsWith(u8, env.items[0], "GIT_SHA=")) {
            var cp = try self.exec(&[_][]const u8{ "git", "rev-parse", "HEAD" });
            try env.append(self.sprintf(
                "GIT_SHA={s}",
                .{cp.stdout},
            ));
        }

        return env;
    }

    fn write_shell_newlines_into_single_line(into: *std.ArrayList(u8), from: []const u8) !void {
        if (from.len == 0) {
            return;
        }

        var lines = std.mem.split(u8, from, "\n");
        while (lines.next()) |line| {
            try into.writer().print("{s} && ", .{line});
        }
    }

    fn build_file_in_docker(self: Generator, tmp_dir: TmpDir, file: []const u8, run_setup_tests: bool) !void {
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

        var env = try self.get_current_commit_and_repo_env();
        defer env.deinit();

        var cmd = std.ArrayList(u8).init(self.allocator);
        defer cmd.deinit();

        // Build against current commit and set up project to use current build.
        if (run_setup_tests) {
            try cmd.appendSlice("export TEST=true && set -x && ");
        }

        // Join together various setup commands that are shown to the
        // user (and not) into a single command we can run in Docker.
        try cmd.appendSlice(" ( ");
        try write_shell_newlines_into_single_line(
            &cmd,
            self.language.developer_setup_sh_commands,
        );
        try write_shell_newlines_into_single_line(
            &cmd,
            self.language.current_commit_pre_install_commands,
        );
        // The above commands all end with ` && `
        try cmd.appendSlice("echo ok ) && "); // Setup commands within parens ( ) so that cwd doesn't change outside

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
            self.language.install_sample_file_build_commands,
        );

        // The above commands all end with ` && `
        try cmd.appendSlice("echo ok");

        try self.run_with_file_in_docker(
            tmp_dir,
            file,
            null,
            .{
                .cmds = cmd.items,
                .mount = null,
                .env = env.items,
            },
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
            self.allocator,
            msg,
            obj,
        ) catch unreachable;
    }

    fn validateMinimal(self: Generator) !void {
        // Test the sample file
        self.print("Building minimal sample file");
        var tmp_dir = try TmpDir.init(self.allocator);
        defer tmp_dir.cleanup();
        try self.build_file_in_docker(tmp_dir, self.language.install_sample_file, true);
    }

    fn validateAggregate(self: Generator) !void {
        // Test major parts of sample code
        var sample = try self.make_aggregate_sample();
        self.print("Building aggregate sample file");
        var tmp_dir = try TmpDir.init(self.allocator);
        defer tmp_dir.cleanup();
        try self.build_file_in_docker(tmp_dir, sample, false);
    }

    const tests = [_]struct {
        name: []const u8,
        validate: fn (Generator) anyerror!void,
    }{
        .{
            .name = "minimal",
            .validate = validateMinimal,
        },
        .{
            .name = "aggregate",
            .validate = validateAggregate,
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
        var aggregate = std.ArrayList(u8).init(self.allocator);
        for (parts) |part| {
            try aggregate.writer().print("{s}\n", .{part});
        }
        return aggregate.items;
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
        mw.paragraph(
            \\Linux >= 5.6 is the only production environment we
            \\support. But for ease of development we support macOS and
            \\Windows unless otherwise noted.
        );
        mw.paragraph(language.prerequisites);

        mw.header(2, "Setup");

        if (language.project_file.len > 0) {
            mw.print(
                "First, create `{s}` and copy this into it:\n\n",
                .{language.project_file_name},
            );
            mw.code(language.markdown_name, language.project_file);
        }

        mw.paragraph("Run:");
        mw.commands(language.install_commands);
        mw.print("Now, create `{s}{s}.{s}` and copy this into it:\n\n", .{
            self.language.test_source_path,
            self.test_file_name,
            language.extension,
        });
        mw.code(language.markdown_name, language.install_sample_file);
        mw.paragraph("Finally, build and run:");
        mw.commands(language.install_sample_file_test_commands);

        mw.paragraph(
            \\Now that all prerequisites and depencies are correctly set
            \\up, let's dig into using TigerBeetle.
            ,
        );

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
        mw.commands(language.developer_setup_sh_commands);

        // Windows setup
        mw.header(3, "On Windows");
        if (language.developer_setup_pwsh_commands.len > 0) {
            mw.paragraph("In PowerShell run:");
            mw.commands(language.developer_setup_pwsh_commands);
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
    var validateOnly: []const u8 = "";

    while (args.nextPosix()) |arg| {
        if (std.mem.eql(u8, arg, "--language")) {
            var filter = args.nextPosix().?;
            skipLanguage = [_]bool{true} ** languages.len;
            for (languages) |language, i| {
                if (std.mem.eql(u8, filter, language.markdown_name)) {
                    skipLanguage[i] = false;
                }
            }
        }

        if (std.mem.eql(u8, arg, "--validate")) {
            validateOnly = args.nextPosix().?;
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

        var generator = try Generator.init(allocator, language);
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

                try t.validate(generator);
            }
        }

        if (generate) {
            generator.print("Generating");
            try generator.generate(&mw);
        }
    }
}
