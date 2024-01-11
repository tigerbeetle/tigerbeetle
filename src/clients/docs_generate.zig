const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;

const flags = @import("../flags.zig");
const stdx = @import("../stdx.zig");

const Docs = @import("./docs_types.zig").Docs;
const go = @import("./go/docs.zig").GoDocs;
const node = @import("./node/docs.zig").NodeDocs;
const java = @import("./java/docs.zig").JavaDocs;
const dotnet = @import("./dotnet/docs.zig").DotnetDocs;
const samples = @import("./docs_samples.zig").samples;
const TmpDir = @import("./shutil.zig").TmpDir;
const file_or_directory_exists =
    @import("./shutil.zig").file_or_directory_exists;
const git_root = @import("./shutil.zig").git_root;

const languages = [_]Docs{ go, node, java, dotnet };

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
        mw.print("{s}\n", .{content});
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

    fn diff_on_disk(mw: *MarkdownWriter, filename: []const u8) !enum { same, different } {
        const file = std.fs.cwd().openFile(filename, .{}) catch {
            // If the file isn't accessible/doesn't exist, yes, we
            // should overwrite it.
            return .different;
        };

        var buf = std.mem.zeroes([4096]u8);
        var remaining = mw.buf.items;

        while (true) {
            var n = try file.read(&buf);
            if (n == 0) {
                return if (remaining.len == 0) .same else .different;
            }

            if (std.mem.startsWith(u8, remaining, buf[0..n])) {
                remaining = remaining[n..];
            } else {
                return .different;
            }
        }
    }

    // save() only actually writes the buffer to disk if it has
    // changed compared to what's on disk, so that file modify time stays
    // reasonable.
    fn save(mw: *MarkdownWriter, filename: []const u8) !void {
        // Ensure a single trailing newline.
        assert(std.mem.endsWith(u8, mw.buf.items, "\n\n"));
        _ = mw.buf.pop();
        assert(!std.mem.endsWith(u8, mw.buf.items, "\n\n"));
        assert(std.mem.endsWith(u8, mw.buf.items, "\n"));

        if (try mw.diff_on_disk(filename) == .different) {
            try std.fs.cwd().writeFile(filename, mw.buf.items);
        }
    }
};

const Generator = struct {
    arena: *std.heap.ArenaAllocator,
    language: Docs,
    test_file_name: []const u8,
    walkthrough: []const u8,

    fn init(
        arena: *std.heap.ArenaAllocator,
        language: Docs,
    ) !Generator {
        var test_file_name = language.test_file_name;
        if (test_file_name.len == 0) {
            test_file_name = "main";
        }

        var result = Generator{
            .arena = arena,
            .language = language,
            .test_file_name = test_file_name,
            .walkthrough = &.{},
        };
        try result.read_walkthrough();
        assert(result.walkthrough.len > 0);
        return result;
    }

    fn read_walkthrough(self: *Generator) !void {
        assert(self.walkthrough.len == 0);

        const root = try git_root(self.arena);
        self.walkthrough = try std.fs.cwd().readFileAlloc(
            self.arena.allocator(),
            self.sprintf("{s}/src/clients/{s}/samples/walkthrough/{s}{s}.{s}", .{
                root,
                self.language.directory,
                self.language.test_source_path,
                self.test_file_name,
                self.language.extension,
            }),
            1024 * 1024,
        );
    }

    fn ensure_path(self: Generator, path: []const u8) !void {
        self.printf(
            "[Debug] Ensuring path: {s}",
            .{path},
        );
        try std.fs.cwd().makePath(path);
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

    fn generate_language_setup_steps(
        self: Generator,
        mw: *MarkdownWriter,
        directory_info: []const u8,
        include_project_file: bool,
    ) void {
        var language = self.language;

        const windows_supported: []const u8 = if (language.developer_setup_pwsh_commands.len > 0)
            " and Windows"
        else
            ". Windows is not yet supported";
        mw.print(
            \\Linux >= 5.6 is the only production environment we
            \\support. But for ease of development we also support macOS{s}.
            \\
        , .{windows_supported});
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

    fn sample_exists(self: Generator, sample: @TypeOf(samples[0])) !bool {
        const root = try git_root(self.arena);
        return file_or_directory_exists(try std.fmt.allocPrint(
            self.arena.allocator(),
            "{s}/src/clients/{s}/samples/{s}/",
            .{ root, self.language.directory, sample.directory },
        ));
    }

    // Pulls a single "section" of code out of the entire walkthrough sample.
    //
    // A section is delimited by a pair of `section:SECTION_NAME` and `endsection:SECTION_NAME`
    // comments. If there are several such pairs, their contents is concatenated (see the `imports`
    // section in the Java sample for a motivational example for concatenation behavior).
    fn code_section(self: Generator, mw: *MarkdownWriter, section_name: []const u8) void {
        var code = std.ArrayList(u8).init(self.arena.allocator());

        const section_start = self.sprintf("section:{s}\n", .{section_name});
        const section_end = self.sprintf("endsection:{s}\n", .{section_name});

        var text = self.walkthrough;
        for (0..10) |_| {
            text = (stdx.cut(text, section_start) orelse break).suffix;

            const section_cut = stdx.cut(text, section_end).?;
            text = section_cut.suffix;

            var section = section_cut.prefix;
            section = section[0..std.mem.lastIndexOf(u8, section, "\n").?];

            var indent_min: usize = std.math.maxInt(usize);
            var lines = std.mem.split(u8, section, "\n");
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                var indent_line: usize = 0;
                while (line[indent_line] == ' ' or line[indent_line] == '\t') indent_line += 1;
                indent_min = @min(indent_min, indent_line);
            }
            assert(indent_min < 16);

            lines = std.mem.split(u8, section, "\n");
            while (lines.next()) |line| {
                if (line.len > 0) {
                    assert(line.len > indent_min);
                    code.appendSlice(line[indent_min..]) catch unreachable;
                }
                code.append('\n') catch unreachable;
            }
        } else @panic("too many parts in a section");
        assert(code.pop() == '\n');

        assert(code.items.len > 0);
        mw.code(self.language.markdown_name, code.items);
    }

    fn generate_main_readme(self: Generator, mw: *MarkdownWriter) !void {
        var language = self.language;

        mw.print(
            \\---
            \\title: {s}
            \\---
            \\
            \\<!-- This file is generated by
            \\     [/src/clients/docs_generate.zig](/src/clients/docs_generate.zig). -->
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
        self.code_section(mw, "imports");
        mw.paragraph("Finally, build and run:");
        mw.commands(language.run_commands);

        mw.paragraph(
            \\Now that all prerequisites and dependencies are correctly set
            \\up, let's dig into using TigerBeetle.
            ,
        );

        mw.header(2, "Sample projects");
        mw.paragraph(
            \\This document is primarily a reference guide to
            \\the client. Below are various sample projects demonstrating
            \\features of TigerBeetle.
            ,
        );
        // Absolute paths here are necessary for resolving within the docs site.
        for (samples) |sample| {
            if (!try self.sample_exists(sample)) {
                continue;
            }

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
            \\Clients are thread-safe and a single instance should be shared
            \\between multiple concurrent tasks.
            \\
            \\Multiple clients are useful when connecting to more than
            \\one TigerBeetle cluster.
            \\
            \\In this example the cluster ID is `0` and there is one
            \\replica. The address is read from the `TB_ADDRESS`
            \\environment variable and defaults to port `3000`.
        );
        self.code_section(mw, "client");
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
        self.code_section(mw, "create-accounts");
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
        self.code_section(mw, "account-flags");

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

        self.code_section(mw, "create-accounts-errors");

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
        self.code_section(mw, "lookup-accounts");

        mw.header(2, "Create Transfers");
        mw.paragraph(
            \\This creates a journal entry between two accounts.
            \\
            \\See details for transfer fields in the [Transfers
            \\reference](https://docs.tigerbeetle.com/reference/transfers).
        );
        self.code_section(mw, "create-transfers");

        mw.header(3, "Response and Errors");
        mw.paragraph(
            \\The response is an empty array if all transfers were created
            \\successfully. If the response is non-empty, each object in the
            \\response array contains error information for a transfer that
            \\failed. The error object contains an error code and the index of the
            \\transfer in the request batch.
            \\
            \\See all error conditions in the [create_transfers
            \\reference](https://docs.tigerbeetle.com/reference/operations/create_transfers).
        );
        self.code_section(mw, "create-transfers-errors");

        mw.paragraph(language.create_transfers_errors_documentation);

        mw.header(2, "Batching");
        mw.paragraph(
            \\TigerBeetle performance is maximized when you batch
            \\API requests. The client does not do this automatically for
            \\you. So, for example, you *can* insert 1 million transfers
            \\one at a time like so:
        );
        self.code_section(mw, "no-batch");
        mw.paragraph(
            \\But the insert rate will be a *fraction* of
            \\potential. Instead, **always batch what you can**.
            \\
            \\The maximum batch size is set in the TigerBeetle server. The default
            \\is 8190.
        );
        self.code_section(mw, "batch");

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
        self.code_section(mw, "transfer-flags-link");

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
        self.code_section(mw, "transfer-flags-post");

        mw.header(4, "Void a Pending Transfer");
        mw.paragraph(
            \\In contrast, with `flags` set to `void_pending_transfer`,
            \\TigerBeetle will void the transfer. TigerBeetle will roll
            \\back the changes to `debits_pending` and `credits_pending` of the
            \\appropriate accounts and **not** apply them to the `debits_posted` and
            \\`credits_posted` balances.
        );
        self.code_section(mw, "transfer-flags-void");

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
        self.code_section(mw, "lookup-transfers");

        mw.header(2, "Get Account Transfers");
        mw.paragraph(
            \\NOTE: This is a preview API that is subject to breaking changes once we have
            \\a stable querying API.
            \\
            \\Fetches the transfers involving a given account, allowing basic filter and pagination
            \\capabilities.
            \\
            \\The transfers in the response are sorted by `timestamp` in chronological or
            \\reverse-chronological order.
        );
        self.code_section(mw, "get-account-transfers");

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
        self.code_section(mw, "linked-events");

        mw.header(2, "Development Setup");
        if (language.developer_setup_documentation.len > 0) {
            mw.print("{s}\n\n", .{language.developer_setup_documentation});
        }

        // Shell setup
        mw.header(3, "On Linux and macOS");
        mw.paragraph("In a POSIX shell run:");
        mw.commands(
            self.sprintf("{s}{s}", .{
                \\git clone https://github.com/tigerbeetle/tigerbeetle
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
                    \\git clone https://github.com/tigerbeetle/tigerbeetle
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

        const root = try git_root(self.arena);
        try mw.save(self.sprintf("{s}/src/clients/{s}/README.md", .{ root, language.directory }));
    }

    fn generate_sample_readmes(self: Generator, mw: *MarkdownWriter) !void {
        var language = self.language;

        for (samples) |sample| {
            if (!try self.sample_exists(sample)) {
                continue;
            }

            mw.reset();
            mw.paragraph(
                \\This file is generated by
                \\[/src/clients/docs_generate.zig](/src/clients/docs_generate.zig).
                ,
            );

            var main_file_name = if (std.mem.eql(u8, language.directory, "go") or
                std.mem.eql(u8, language.directory, "node"))
                "main"
            else if (std.mem.eql(u8, language.directory, "dotnet"))
                "Program"
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
                    \\First, clone this repo and `cd` into `tigerbeetle/src/clients/{s}/samples/{s}`.
                , .{ language.directory, sample.directory }),
                false,
            );

            mw.header(2, "Start the TigerBeetle server");
            mw.paragraph(
                \\Follow steps in the repo README to [run
                \\TigerBeetle](/README.md#running-tigerbeetle).
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

const CliArgs = struct {
    language: ?[]const u8 = null,
    no_generate: bool = false,
    keep_tmp: bool = false,
};

pub fn main() !void {
    var skip_language = [_]bool{false} ** languages.len;

    var global_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer global_arena.deinit();

    var args = try std.process.argsWithAllocator(global_arena.allocator());
    defer args.deinit();

    assert(args.skip());

    const cli_args = flags.parse(&args, CliArgs);

    if (cli_args.language) |filter| {
        skip_language = .{true} ** languages.len;

        var parts = std.mem.split(u8, filter, ",");
        while (parts.next()) |part| {
            for (languages, 0..) |language, i| {
                if (std.mem.eql(u8, language.directory, part)) {
                    skip_language[i] = false;
                    break;
                }
            } else flags.fatal("--language: unknown language '{s}'", .{part});
        }
    }

    for (languages, 0..) |language, i| {
        if (skip_language[i]) {
            continue;
        }

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();

        const allocator = arena.allocator();
        var buf = std.ArrayList(u8).init(allocator);
        var mw = MarkdownWriter.init(&buf);

        var generator = try Generator.init(&arena, language);

        if (!cli_args.no_generate) {
            generator.print("Generating main README");
            try generator.generate_main_readme(&mw);

            generator.print("Generating sample READMEs");
            try generator.generate_sample_readmes(&mw);
        }
    }
}
