const std = @import("std");
const builtin = @import("builtin");

const assert = std.debug.assert;

const flags = @import("../flags.zig");

const TmpDir = @import("./shutil.zig").TmpDir;
const shell_wrap = @import("./shutil.zig").shell_wrap;
const run_shell = @import("./shutil.zig").run_shell;
const binary_filename = @import("./shutil.zig").binary_filename;
const run_many_with_tb = @import("./run_with_tb.zig").run_many_with_tb;

const tb_client_command_base =
    "{s} client --cluster=0 --addresses=$" ++
    (if (builtin.os.tag == .windows) "env:" else "") ++
    "TB_ADDRESS --command=\"{s}\"";

fn tb_client_command(
    arena: *std.heap.ArenaAllocator,
    tb_binary: []const u8,
    command: []const u8,
) ![]const []const u8 {
    return shell_wrap(
        arena,
        try std.fmt.allocPrint(
            arena.allocator(),
            tb_client_command_base,
            .{ tb_binary, command },
        ),
    );
}

fn tb_client_command_json_out(
    arena: *std.heap.ArenaAllocator,
    tb_binary: []const u8,
    command: []const u8,
    out_name: []const u8,
) ![]const []const u8 {
    return shell_wrap(
        arena,
        try std.fmt.allocPrint(
            arena.allocator(),
            tb_client_command_base ++ " > {s}",
            .{ tb_binary, command, out_name },
        ),
    );
}

fn expect_json_file_equals(
    arena: *std.heap.ArenaAllocator,
    expected: anytype,
    out_directory_name: []const u8,
    out_file_name: []const u8,
) !void {
    const out_file = try std.fs.cwd().readFileAlloc(
        arena.allocator(),
        try std.fmt.allocPrint(
            arena.allocator(),
            "{s}/{s}",
            .{ out_directory_name, out_file_name },
        ),
        std.math.maxInt(usize),
    );
    // On Windows, there's sometimes a bunch of garbage in this
    // file -- only in Github Actions CI, not on Phil's Windows
    // machines. So filter that out so Zig's JSON parser doesn't
    // break. But rather than relying only on Windows machines to
    // exercise this logic, let's just strip out all non-printable
    // characters all the time.
    var out_file_filtered = std.ArrayList(u8).init(arena.allocator());
    for (out_file) |c| {
        // Filter out characters within printable range.
        if (c >= 33 and c <= 126) {
            try out_file_filtered.append(c);
        }
    }

    // Fails to compile in std.json.parse with the default
    // branch quota.
    @setEvalBranchQuota(5000);
    var parsed_container = try std.json.parseFromSlice(
        @TypeOf(expected),
        arena.allocator(),
        out_file_filtered.items,
        .{},
    );
    defer parsed_container.deinit();

    const parsed = &parsed_container.value;

    inline for (@typeInfo(@TypeOf(expected)).Struct.fields) |field| {
        if (comptime std.mem.eql(u8, field.name, "timestamp")) {
            // We won't know up front what it is, so just make sure it's not blank.
            try std.testing.expect(!std.mem.eql(u8, parsed.timestamp, ""));
        } else if (comptime std.mem.eql(u8, field.name, "flags")) {
            try std.testing.expect(expected.flags.len == parsed.flags.len);
            for (expected.flags, 0..) |flag, i| {
                try std.testing.expectEqualSlices(u8, flag, parsed.flags[i]);
            }
        } else {
            try std.testing.expectEqualSlices(
                u8,
                @field(expected, field.name),
                @field(parsed, field.name),
            );
        }
    }
}

// Copy of tb.Account for testing the CLI client. All integers are
// strings except for flags which is an array of strings.
const JSONAccount = struct {
    id: []const u8,
    debits_pending: []const u8,
    debits_posted: []const u8,
    credits_pending: []const u8,
    credits_posted: []const u8,
    user_data_128: []const u8,
    user_data_64: []const u8,
    user_data_32: []const u8,
    ledger: []const u8,
    code: []const u8,
    flags: []const []const u8,
    timestamp: []const u8,
};

// Copy of tb.Transfer for testing the CLI client. All integers are
// strings except for flags which is an array of strings.
const JSONTransfer = struct {
    id: []const u8,
    debit_account_id: []const u8,
    credit_account_id: []const u8,
    amount: []const u8,
    pending_id: []const u8,
    user_data_128: []const u8,
    user_data_64: []const u8,
    user_data_32: []const u8,
    timeout: []const u8,
    ledger: []const u8,
    code: []const u8,
    flags: [][]const u8,
    timestamp: []const u8,
};

fn test_basic_accounts_and_transfers(
    arena: *std.heap.ArenaAllocator,
    tb_binary: []const u8,
    tmp_dir: []const u8,
) !void {
    const allocator = arena.allocator();
    var expected_account1 = JSONAccount{
        .id = "1",
        .debits_pending = "0",
        .debits_posted = "10",
        .credits_pending = "0",
        .credits_posted = "0",
        .user_data_128 = "0",
        .user_data_64 = "0",
        .user_data_32 = "0",
        .ledger = "700",
        .code = "10",
        .flags = &[_][]const u8{"linked"},
        .timestamp = "",
    };
    var expected_account2 = JSONAccount{
        .id = "2",
        .debits_pending = "0",
        .debits_posted = "0",
        .credits_pending = "0",
        .credits_posted = "10",
        .user_data_128 = "0",
        .user_data_64 = "0",
        .user_data_32 = "0",
        .ledger = "700",
        .code = "10",
        .flags = &[_][]const u8{},
        .timestamp = "",
    };

    var expected_transfer = JSONTransfer{
        .id = "1",
        .debit_account_id = "1",
        .credit_account_id = "2",
        .amount = "10",
        .pending_id = "0",
        .user_data_128 = "0",
        .user_data_64 = "0",
        .user_data_32 = "0",
        .timeout = "0",
        .ledger = "700",
        .code = "10",
        .flags = &[_][]const u8{},
        .timestamp = "",
    };

    try run_many_with_tb(
        arena,
        &[_][]const []const u8{
            try tb_client_command(
                arena,
                tb_binary,
                "create_accounts id=1 flags=linked code=10 ledger=700, id=2 code=10 ledger=700",
            ),
            try tb_client_command(arena, tb_binary,
                \\create_transfers id=1 debit_account_id=1
                \\ credit_account_id=2 amount=10 ledger=700 code=10
            ),
            try tb_client_command_json_out(
                arena,
                tb_binary,
                "lookup_accounts id=1",
                try std.fmt.allocPrint(allocator, "{s}/out_account1", .{tmp_dir}),
            ),
            try tb_client_command_json_out(
                arena,
                tb_binary,
                "lookup_accounts id=2",
                try std.fmt.allocPrint(allocator, "{s}/out_account2", .{tmp_dir}),
            ),
            try tb_client_command_json_out(
                arena,
                tb_binary,
                "lookup_transfers id=1",
                try std.fmt.allocPrint(allocator, "{s}/out_transfer", .{tmp_dir}),
            ),
        },
        ".",
    );

    try expect_json_file_equals(arena, expected_account1, tmp_dir, "out_account1");
    try expect_json_file_equals(arena, expected_account2, tmp_dir, "out_account2");
    try expect_json_file_equals(arena, expected_transfer, tmp_dir, "out_transfer");
}

const CliArgs = struct {
    keep_tmp: bool = false,
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var args = try std.process.argsWithAllocator(arena.allocator());
    defer args.deinit();

    assert(args.skip());

    const cli_args = flags.parse(&args, CliArgs);

    var t = try TmpDir.init(&arena);
    defer if (!cli_args.keep_tmp) {
        t.deinit();
    };

    const tb_binary = try binary_filename(&arena, &[_][]const u8{"tigerbeetle"});

    try test_basic_accounts_and_transfers(&arena, tb_binary, t.path);
}
