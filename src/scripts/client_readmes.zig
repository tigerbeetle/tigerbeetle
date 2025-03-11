//! All language clients are sufficiently similar, and, for that reason, we generate the
//! corresponding READMEs by splicing language-specific code snippets into a language agnostic
//! template. This file handles the generation process.
//!
//! Code generation is written as a test that checks that generated READMEs are fresh. If they are
//! not, the file on disk is updated, and the overall test fails.

const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const Shell = @import("../shell.zig");
const Docs = @import("../clients/docs_types.zig").Docs;
const Sample = @import("../clients/docs_types.zig").Sample;
const samples = @import("../clients/docs_samples.zig").samples;

const Language = @import("./ci.zig").Language;

const LanguageDocs = .{
    .go = @import("../clients/go/docs.zig").GoDocs,
    .node = @import("../clients/node/docs.zig").NodeDocs,
    .java = @import("../clients/java/docs.zig").JavaDocs,
    .dotnet = @import("../clients/dotnet/docs.zig").DotnetDocs,
    .python = @import("../clients/python/docs.zig").PythonDocs,
};

pub fn test_freshness(
    shell: *Shell,
    gpa: std.mem.Allocator,
    language: Language,
) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const docs = switch (language) {
        inline else => |l| @field(LanguageDocs, @tagName(l)),
    };

    const walkthrough_path = try shell.fmt(
        "./samples/walkthrough/{s}{s}.{s}",
        .{ docs.test_source_path, docs.test_file_name, docs.extension },
    );
    const walkthrough = try shell.cwd.readFileAlloc(
        arena.allocator(),
        walkthrough_path,
        1024 * 1024,
    );

    var ctx = Context{
        .shell = shell,
        .arena = arena.allocator(),
        .buffer = std.ArrayList(u8).init(arena.allocator()),
        .docs = docs,
        .walkthrough = walkthrough,
    };

    var updated_any = false;

    { // Root README.md.
        ctx.buffer.clearRetainingCapacity();
        try readme_root(&ctx);

        const update = try shell.file_ensure_content("README.md", ctx.buffer.items, .{});
        updated_any = updated_any or (update == .updated);
    }

    for (samples) |sample| { // Per-sample README.md

        ctx.buffer.clearRetainingCapacity();
        try readme_sample(&ctx, sample);

        const sample_readme = try shell.fmt("samples/{s}/README.md", .{sample.directory});
        const update = try shell.file_ensure_content(sample_readme, ctx.buffer.items, .{});
        updated_any = updated_any or (update == .updated);
    }

    if (updated_any) return error.DocsUpdated;
}

// Can't use `@src().file` here as it uses forward slashes on Windows.
const this_file = "/src/scripts/client_readmes.zig";

fn readme_root(ctx: *Context) !void {
    assert(ctx.buffer.items.len == 0);

    ctx.print(
        \\<!-- This file is generated by [{s}]({s}). -->
        \\
    , .{ this_file, this_file });

    ctx.header(1, ctx.docs.name);
    ctx.paragraph(ctx.docs.description);

    {
        ctx.header(2, "Prerequisites");
        ctx.print(
            \\Linux >= 5.6 is the only production environment we
            \\support. But for ease of development we also support macOS and Windows.
            \\
        , .{});
        ctx.paragraph(ctx.docs.prerequisites);
    }

    {
        ctx.header(2, "Setup");
        ctx.paragraph("First, create a directory for your project and `cd` into the directory.");

        if (ctx.docs.project_file.len > 0) {
            ctx.print(
                "Then create `{s}` and copy this into it:\n\n",
                .{ctx.docs.project_file_name},
            );
            const project_file_language = stdx.cut(ctx.docs.project_file_name, ".").?.suffix;
            ctx.code(project_file_language, ctx.docs.project_file);
        }

        ctx.paragraph("Then, install the TigerBeetle client:");
        ctx.commands(ctx.docs.install_commands);

        ctx.print("Now, create `{s}{s}.{s}` and copy this into it:\n\n", .{
            ctx.docs.test_source_path,
            ctx.docs.test_file_name,
            ctx.docs.extension,
        });
        ctx.code_section("imports");
        ctx.paragraph("Finally, build and run:");
        ctx.commands(ctx.docs.run_commands);

        ctx.paragraph(
            \\Now that all prerequisites and dependencies are correctly set
            \\up, let's dig into using TigerBeetle.
            ,
        );
    }

    {
        ctx.header(2, "Sample projects");
        ctx.paragraph(
            \\This document is primarily a reference guide to
            \\the client. Below are various sample projects demonstrating
            \\features of TigerBeetle.
            ,
        );
        // Absolute paths here are necessary for resolving within the docs site.
        for (samples) |sample| {
            if (try ctx.sample_exists(sample)) {
                ctx.print("* [{s}](/src/clients/{s}/samples/{s}/): {s}\n", .{
                    sample.proper_name,
                    ctx.docs.directory,
                    sample.directory,
                    sample.short_description,
                });
            }
        }

        if (ctx.docs.examples.len != 0) {
            ctx.paragraph(ctx.docs.examples);
        }
    }

    {
        ctx.header(2, "Creating a Client");
        ctx.paragraph(
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
        ctx.code_section("client");
        ctx.paragraph(ctx.docs.client_object_documentation);

        ctx.paragraph(
            \\The following are valid addresses:
            \\* `3000` (interpreted as `127.0.0.1:3000`)
            \\* `127.0.0.1:3000` (interpreted as `127.0.0.1:3000`)
            \\* `127.0.0.1` (interpreted as `127.0.0.1:3001`, `3001` is the default port)
        );
    }

    {
        ctx.header(2, "Creating Accounts");
        ctx.paragraph(
            \\See details for account fields in the [Accounts
            \\reference](https://docs.tigerbeetle.com/reference/account).
        );
        ctx.code_section("create-accounts");

        ctx.paragraph(
            \\See details for the recommended ID scheme in
            \\[time-based identifiers](https://docs.tigerbeetle.com/coding/data-modeling#tigerbeetle-time-based-identifiers-recommended).
        );

        ctx.paragraph(ctx.docs.create_accounts_documentation);

        ctx.header(3, "Account Flags");
        ctx.paragraph(
            \\The account flags value is a bitfield. See details for
            \\these flags in the [Accounts
            \\reference](https://docs.tigerbeetle.com/reference/account#flags).
        );
        ctx.paragraph(ctx.docs.account_flags_documentation);

        ctx.paragraph(
            \\For example, to link two accounts where the first account
            \\additionally has the `debits_must_not_exceed_credits` constraint:
        );
        ctx.code_section("account-flags");

        ctx.header(3, "Response and Errors");
        ctx.paragraph(
            \\The response is an empty array if all accounts were
            \\created successfully. If the response is non-empty, each
            \\object in the response array contains error information
            \\for an account that failed. The error object contains an
            \\error code and the index of the account in the request
            \\batch.
            \\
            \\See all error conditions in the [create_accounts
            \\reference](https://docs.tigerbeetle.com/reference/requests/create_accounts).
        );

        ctx.code_section("create-accounts-errors");

        ctx.paragraph(ctx.docs.create_accounts_errors_documentation);
    }

    {
        ctx.header(2, "Account Lookup");
        ctx.paragraph(
            \\Account lookup is batched, like account creation. Pass
            \\in all IDs to fetch. The account for each matched ID is returned.
            \\
            \\If no account matches an ID, no object is returned for
            \\that account. So the order of accounts in the response is
            \\not necessarily the same as the order of IDs in the
            \\request. You can refer to the ID field in the response to
            \\distinguish accounts.
        );
        ctx.code_section("lookup-accounts");
    }

    {
        ctx.header(2, "Create Transfers");
        ctx.paragraph(
            \\This creates a journal entry between two accounts.
            \\
            \\See details for transfer fields in the [Transfers
            \\reference](https://docs.tigerbeetle.com/reference/transfer).
        );
        ctx.code_section("create-transfers");

        ctx.paragraph(
            \\See details for the recommended ID scheme in
            \\[time-based identifiers](https://docs.tigerbeetle.com/coding/data-modeling#tigerbeetle-time-based-identifiers-recommended).
        );

        ctx.header(3, "Response and Errors");
        ctx.paragraph(
            \\The response is an empty array if all transfers were created
            \\successfully. If the response is non-empty, each object in the
            \\response array contains error information for a transfer that
            \\failed. The error object contains an error code and the index of the
            \\transfer in the request batch.
            \\
            \\See all error conditions in the [create_transfers
            \\reference](https://docs.tigerbeetle.com/reference/requests/create_transfers).
        );
        ctx.code_section("create-transfers-errors");

        ctx.paragraph(ctx.docs.create_transfers_errors_documentation);
    }

    {
        ctx.header(2, "Batching");
        ctx.paragraph(
            \\TigerBeetle performance is maximized when you batch
            \\API requests. The client does not do this automatically for
            \\you. So, for example, you *can* insert 1 million transfers
            \\one at a time like so:
        );
        ctx.code_section("no-batch");
        ctx.paragraph(
            \\But the insert rate will be a *fraction* of
            \\potential. Instead, **always batch what you can**.
            \\
            \\The maximum batch size is set in the TigerBeetle server. The default
            \\is 8190.
        );
        ctx.code_section("batch");

        ctx.header(3, "Queues and Workers");
        ctx.paragraph(
            \\If you are making requests to TigerBeetle from workers
            \\pulling jobs from a queue, you can batch requests to
            \\TigerBeetle by having the worker act on multiple jobs from
            \\the queue at once rather than one at a time. i.e. pulling
            \\multiple jobs from the queue rather than just one.
        );
    }

    {
        ctx.header(2, "Transfer Flags");
        ctx.paragraph(
            \\The transfer `flags` value is a bitfield. See details for these flags in
            \\the [Transfers
            \\reference](https://docs.tigerbeetle.com/reference/transfer#flags).
        );
        ctx.paragraph(ctx.docs.transfer_flags_documentation);
        ctx.paragraph("For example, to link `transfer0` and `transfer1`:");
        ctx.code_section("transfer-flags-link");

        ctx.header(3, "Two-Phase Transfers");
        ctx.paragraph(
            \\Two-phase transfers are supported natively by toggling the appropriate
            \\flag. TigerBeetle will then adjust the `credits_pending` and
            \\`debits_pending` fields of the appropriate accounts. A corresponding
            \\post pending transfer then needs to be sent to post or void the
            \\transfer.
        );
        ctx.header(4, "Post a Pending Transfer");
        ctx.paragraph(
            \\With `flags` set to `post_pending_transfer`,
            \\TigerBeetle will post the transfer. TigerBeetle will atomically roll
            \\back the changes to `debits_pending` and `credits_pending` of the
            \\appropriate accounts and apply them to the `debits_posted` and
            \\`credits_posted` balances.
        );
        ctx.code_section("transfer-flags-post");

        ctx.header(4, "Void a Pending Transfer");
        ctx.paragraph(
            \\In contrast, with `flags` set to `void_pending_transfer`,
            \\TigerBeetle will void the transfer. TigerBeetle will roll
            \\back the changes to `debits_pending` and `credits_pending` of the
            \\appropriate accounts and **not** apply them to the `debits_posted` and
            \\`credits_posted` balances.
        );
        ctx.code_section("transfer-flags-void");
    }

    {
        ctx.header(2, "Transfer Lookup");
        ctx.paragraph(
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
        ctx.code_section("lookup-transfers");
    }

    {
        ctx.header(2, "Get Account Transfers");
        ctx.paragraph(
            \\NOTE: This is a preview API that is subject to breaking changes once we have
            \\a stable querying API.
            \\
            \\Fetches the transfers involving a given account, allowing basic filter and pagination
            \\capabilities.
            \\
            \\The transfers in the response are sorted by `timestamp` in chronological or
            \\reverse-chronological order.
        );
        ctx.code_section("get-account-transfers");
    }

    {
        ctx.header(2, "Get Account Balances");
        ctx.paragraph(
            \\NOTE: This is a preview API that is subject to breaking changes once we have
            \\a stable querying API.
            \\
            \\Fetches the point-in-time balances of a given account, allowing basic filter and
            \\pagination capabilities.
            \\
            \\Only accounts created with the flag
            \\[`history`](https://docs.tigerbeetle.com/reference/account#flagshistory) set retain
            \\[historical balances](https://docs.tigerbeetle.com/reference/requests/get_account_balances).
            \\
            \\The balances in the response are sorted by `timestamp` in chronological or
            \\reverse-chronological order.
        );
        ctx.code_section("get-account-balances");
    }

    {
        ctx.header(2, "Query Accounts");
        ctx.paragraph(
            \\NOTE: This is a preview API that is subject to breaking changes once we have
            \\a stable querying API.
            \\
            \\Query accounts by the intersection of some fields and by timestamp range.
            \\
            \\The accounts in the response are sorted by `timestamp` in chronological or
            \\reverse-chronological order.
        );
        ctx.code_section("query-accounts");
    }

    {
        ctx.header(2, "Query Transfers");
        ctx.paragraph(
            \\NOTE: This is a preview API that is subject to breaking changes once we have
            \\a stable querying API.
            \\
            \\Query transfers by the intersection of some fields and by timestamp range.
            \\
            \\The transfers in the response are sorted by `timestamp` in chronological or
            \\reverse-chronological order.
        );
        ctx.code_section("query-transfers");
    }

    {
        ctx.header(2, "Linked Events");
        ctx.paragraph(
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
        ctx.code_section("linked-events");
    }

    {
        ctx.header(2, "Imported Events");
        ctx.paragraph(
            \\When the `imported` flag is specified for an account when creating accounts or
            \\a transfer when creating transfers, it allows importing historical events with
            \\a user-defined timestamp.
            \\
            \\The entire batch of events must be set with the flag `imported`.
            \\
            \\It's recommended to submit the whole batch as a `linked` chain of events, ensuring that
            \\if any event fails, none of them are committed, preserving the last timestamp unchanged.
            \\This approach gives the application a chance to correct failed imported events, re-submitting
            \\the batch again with the same user-defined timestamps.
        );
        ctx.code_section("imported-events");
    }

    ctx.ensure_final_newline();
}

fn readme_sample(ctx: *Context, sample: Sample) !void {
    ctx.print(
        \\<!-- This file is generated by [{s}]({s}). -->
        \\
    , .{ this_file, this_file });

    ctx.print(
        \\# {s} {s} Sample
        \\
        \\Code for this sample is in [./{s}{s}.{s}](./{s}{s}.{s}).
        \\
        \\
    , .{
        sample.proper_name,
        ctx.docs.proper_name,
        ctx.docs.test_source_path,
        ctx.docs.test_file_name,
        ctx.docs.extension,
        ctx.docs.test_source_path,
        ctx.docs.test_file_name,
        ctx.docs.extension,
    });

    {
        ctx.header(2, "Prerequisites");
        ctx.print(
            \\Linux >= 5.6 is the only production environment we
            \\support. But for ease of development we also support macOS and Windows.
            \\
        , .{});
        ctx.paragraph(ctx.docs.prerequisites);
    }

    {
        ctx.header(2, "Setup");
        ctx.paragraph(try ctx.shell.fmt(
            \\First, clone this repo and `cd` into `tigerbeetle/src/clients/{s}/samples/{s}`.
        , .{ ctx.docs.directory, sample.directory }));

        ctx.paragraph("Then, install the TigerBeetle client:");
        ctx.commands(ctx.docs.install_commands);
    }

    {
        ctx.header(2, "Start the TigerBeetle server");
        ctx.paragraph(
            \\Follow steps in the repo README to [run
            \\TigerBeetle](/README.md#running-tigerbeetle).
            \\
            \\If you are not running on port `localhost:3000`, set
            \\the environment variable `TB_ADDRESS` to the full
            \\address of the TigerBeetle server you started.
        );
    }

    {
        ctx.header(2, "Run this sample");
        ctx.paragraph("Now you can run this sample:");
        ctx.commands(ctx.docs.run_commands);
    }

    {
        ctx.header(2, "Walkthrough");
        ctx.paragraph("Here's what this project does.");
        ctx.paragraph(sample.long_description);
    }

    ctx.ensure_final_newline();
}

const Context = struct {
    shell: *Shell,
    arena: std.mem.Allocator,
    buffer: std.ArrayList(u8),
    docs: Docs,
    walkthrough: []const u8,

    fn sample_exists(ctx: *Context, sample: @TypeOf(samples[0])) !bool {
        const sample_directory = try ctx.shell.fmt("samples/{s}/", .{sample.directory});
        return try ctx.shell.dir_exists(sample_directory);
    }

    // Pulls a single "section" of code out of the entire walkthrough sample.
    //
    // A section is delimited by a pair of `section:SECTION_NAME` and `endsection:SECTION_NAME`
    // comments. If there are several such pairs, their contents is concatenated (see the `imports`
    // section in the Java sample for a motivational example for concatenation behavior).
    fn read_section(ctx: *Context, section_name: []const u8) []const u8 {
        var section_content = std.ArrayList(u8).init(ctx.arena);
        const section_start =
            ctx.shell.fmt("section:{s}\n", .{section_name}) catch @panic("OOM");
        const section_end =
            ctx.shell.fmt("endsection:{s}\n", .{section_name}) catch @panic("OOM");

        var text = ctx.walkthrough;
        for (0..10) |_| {
            text = (stdx.cut(text, section_start) orelse break).suffix;

            const section_cut = stdx.cut(text, section_end).?;
            text = section_cut.suffix;

            var section = section_cut.prefix;
            section = section[0..std.mem.lastIndexOfScalar(u8, section, '\n').?];

            var indent_min: usize = std.math.maxInt(usize);
            var lines = std.mem.splitScalar(u8, section, '\n');
            while (lines.next()) |line| {
                if (line.len == 0) continue;
                var indent_line: usize = 0;
                while (line[indent_line] == ' ' or line[indent_line] == '\t') indent_line += 1;
                indent_min = @min(indent_min, indent_line);
            }
            assert(indent_min < 18);

            lines = std.mem.splitScalar(u8, section, '\n');
            while (lines.next()) |line| {
                if (line.len > 0) {
                    assert(line.len > indent_min);
                    section_content.appendSlice(line[indent_min..]) catch unreachable;
                }
                section_content.append('\n') catch unreachable;
            }
        } else @panic("too many parts in a section");
        assert(section_content.pop() == '\n');

        const result = section_content.items;
        assert(result.len > 0);
        return result;
    }

    fn header(ctx: *Context, comptime level: u8, content: []const u8) void {
        ctx.print(("#" ** level) ++ " {s}\n\n", .{content});
    }

    fn paragraph(ctx: *Context, content: []const u8) void {
        // Don't print empty lines.
        if (content.len == 0) return;
        ctx.print("{s}\n\n", .{content});
    }

    fn code(ctx: *Context, language: []const u8, content: []const u8) void {
        // Don't print empty lines.
        if (content.len == 0) return;
        ctx.print("```{s}\n{s}\n```\n\n", .{ language, content });
    }

    fn code_section(ctx: *Context, section_name: []const u8) void {
        const section_content = ctx.read_section(section_name);
        ctx.code(ctx.docs.markdown_name, section_content);
    }

    fn commands(ctx: *Context, content: []const u8) void {
        ctx.code("console", content);
    }

    fn print(ctx: *Context, comptime fmt: []const u8, args: anytype) void {
        ctx.buffer.writer().print(fmt, args) catch @panic("OOM");
    }

    fn ensure_final_newline(ctx: *Context) void {
        assert(ctx.buffer.pop() == '\n');
        assert(std.mem.endsWith(u8, ctx.buffer.items, "\n"));
        assert(!std.mem.endsWith(u8, ctx.buffer.items, "\n\n"));
    }
};
