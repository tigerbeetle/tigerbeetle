const std = @import("std");
const stdx = @import("stdx");

const CLIArgs = struct {
    zig_exe: []const u8,
    tb_objcopy: []const u8,
    llvm_objcopy: ?[]const u8 = null,
    iterations: usize = 100,
    seed: ?u64 = null,
    tmp: []const u8 = "/tmp",
};

const edge_lengths = [_]usize{
    1,   2,   7,   8,    15,   16,   31,   32,   63,   64,   127,  128,  255,  256,
    511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049, 4095, 4096, 4097, 8191,
};

pub fn main() !void {
    var gpa_state: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer if (gpa_state.deinit() != .ok) @panic("memory leaked");

    const gpa = gpa_state.allocator();

    var arguments = try std.process.argsWithAllocator(gpa);
    defer arguments.deinit();

    const command_line = stdx.flags(&arguments, CLIArgs);
    var prng = stdx.PRNG.from_seed(command_line.seed orelse std.crypto.random.int(u64));

    var tmp_dir_name_buffer: [64]u8 = undefined;
    const tmp_dir_name = try std.fmt.bufPrint(
        &tmp_dir_name_buffer,
        "{s}/tb_objcopy_fuzz_{d}",
        .{ command_line.tmp, prng.int(u64) },
    );
    try std.fs.cwd().makePath(tmp_dir_name);
    defer std.fs.cwd().deleteTree(tmp_dir_name) catch {};

    const fixture_elf = try build_linux_fixture(gpa, command_line, tmp_dir_name);
    defer gpa.free(fixture_elf);

    const fixture_pe = try std.fmt.allocPrint(gpa, "{s}/fixture.exe", .{tmp_dir_name});
    defer gpa.free(fixture_pe);

    try build_windows_fixture(gpa, command_line, tmp_dir_name, fixture_pe);

    var fixtures = std.ArrayList([]const u8).init(gpa);
    defer fixtures.deinit();

    try fixtures.append(fixture_elf);
    try fixtures.append(fixture_pe);

    for (0..command_line.iterations) |iteration| {
        for (fixtures.items) |fixture| {
            try run_case(gpa, &prng, command_line, tmp_dir_name, fixture, iteration);
        }
    }
}

fn run_case(
    gpa: std.mem.Allocator,
    prng: *stdx.PRNG,
    command_line: CLIArgs,
    tmp_dir: []const u8,
    fixture: []const u8,
    iteration: usize,
) !void {
    const case_dir = try std.fmt.allocPrint(gpa, "{s}/case-{d}", .{ tmp_dir, iteration });
    defer gpa.free(case_dir);

    try std.fs.cwd().makePath(case_dir);
    defer std.fs.cwd().deleteTree(case_dir) catch {};

    const body_data = try random_bytes_file(
        gpa,
        prng,
        case_dir,
        "body.bin",
        fuzz_length(prng, iteration * 3),
    );
    defer gpa.free(body_data.path);

    const header_data = try random_bytes_file(
        gpa,
        prng,
        case_dir,
        "header.bin",
        fuzz_length(prng, iteration * 3 + 1),
    );
    defer gpa.free(header_data.path);

    const header_replacement_data = try random_bytes_file(
        gpa,
        prng,
        case_dir,
        "header_replacement.bin",
        fuzz_length(prng, iteration * 3 + 2),
    );
    defer gpa.free(header_replacement_data.path);

    const tb_copy = try std.fmt.allocPrint(gpa, "{s}/tb_copy.bin", .{case_dir});
    defer gpa.free(tb_copy);

    const tb_working = try std.fmt.allocPrint(gpa, "{s}/tb_working.bin", .{case_dir});
    defer gpa.free(tb_working);

    const llvm_working = try std.fmt.allocPrint(gpa, "{s}/llvm_working.bin", .{case_dir});
    defer gpa.free(llvm_working);

    const tb_removed = try std.fmt.allocPrint(gpa, "{s}/tb_removed.bin", .{case_dir});
    defer gpa.free(tb_removed);

    const llvm_removed = try std.fmt.allocPrint(gpa, "{s}/llvm_removed.bin", .{case_dir});
    defer gpa.free(llvm_removed);

    try run_tool(gpa, command_line.tb_objcopy, &.{
        "--enable-deterministic-archives",
        fixture,
        tb_copy,
    });
    try run_tool(gpa, command_line.tb_objcopy, &.{
        "--enable-deterministic-archives",
        tb_copy,
        tb_working,
    });

    if (command_line.llvm_objcopy) |llvm_objcopy| {
        try run_tool(gpa, llvm_objcopy, &.{
            "--enable-deterministic-archives",
            fixture,
            llvm_working,
        });
        try assert_equal_stage(gpa, tb_copy, llvm_working, "copy", case_dir);
    }

    const add_mvb = try std.fmt.allocPrint(gpa, ".tb_mvb={s}", .{body_data.path});
    defer gpa.free(add_mvb);

    const add_mvh = try std.fmt.allocPrint(gpa, ".tb_mvh={s}", .{header_data.path});
    defer gpa.free(add_mvh);

    try run_tool(gpa, command_line.tb_objcopy, &.{
        "--enable-deterministic-archives",
        "--keep-undefined",
        "--add-section",
        add_mvb,
        "--set-section-flags",
        ".tb_mvb=contents,noload,readonly",
        "--add-section",
        add_mvh,
        "--set-section-flags",
        ".tb_mvh=contents,noload,readonly",
        tb_working,
    });
    if (command_line.llvm_objcopy) |llvm_objcopy| {
        try run_tool(gpa, llvm_objcopy, &.{
            "--enable-deterministic-archives",
            "--keep-undefined",
            "--add-section",
            add_mvb,
            "--set-section-flags",
            ".tb_mvb=contents,noload,readonly",
            "--add-section",
            add_mvh,
            "--set-section-flags",
            ".tb_mvh=contents,noload,readonly",
            llvm_working,
        });
        try assert_equal_stage(gpa, tb_working, llvm_working, "add", case_dir);
    }

    const add_mvh_replacement = try std.fmt.allocPrint(
        gpa,
        ".tb_mvh={s}",
        .{header_replacement_data.path},
    );
    defer gpa.free(add_mvh_replacement);

    try run_tool(gpa, command_line.tb_objcopy, &.{
        "--enable-deterministic-archives",
        "--keep-undefined",
        "--remove-section",
        ".tb_mvh",
        "--add-section",
        add_mvh_replacement,
        "--set-section-flags",
        ".tb_mvh=contents,noload,readonly",
        tb_working,
    });
    if (command_line.llvm_objcopy) |llvm_objcopy| {
        try run_tool(gpa, llvm_objcopy, &.{
            "--enable-deterministic-archives",
            "--keep-undefined",
            "--remove-section",
            ".tb_mvh",
            "--add-section",
            add_mvh_replacement,
            "--set-section-flags",
            ".tb_mvh=contents,noload,readonly",
            llvm_working,
        });
        try assert_equal_stage(gpa, tb_working, llvm_working, "header replace", case_dir);
    }

    try run_tool(gpa, command_line.tb_objcopy, &.{
        "--enable-deterministic-archives",
        "--keep-undefined",
        "--remove-section",
        ".tb_mvb",
        "--remove-section",
        ".tb_mvh",
        tb_working,
        tb_removed,
    });
    if (command_line.llvm_objcopy) |llvm_objcopy| {
        try run_tool(gpa, llvm_objcopy, &.{
            "--enable-deterministic-archives",
            "--keep-undefined",
            "--remove-section",
            ".tb_mvb",
            "--remove-section",
            ".tb_mvh",
            llvm_working,
            llvm_removed,
        });
        try assert_equal_stage(gpa, tb_removed, llvm_removed, "remove", case_dir);
    }
    try assert_equal_stage(gpa, tb_removed, tb_copy, "roundtrip remove", case_dir);
}

const RandomBytesFile = struct {
    path: []const u8,
};

fn random_bytes_file(
    gpa: std.mem.Allocator,
    prng: *stdx.PRNG,
    dir: []const u8,
    name: []const u8,
    length_bytes: usize,
) !RandomBytesFile {
    const path = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ dir, name });
    const bytes = try gpa.alloc(u8, length_bytes);
    defer gpa.free(bytes);

    prng.fill(bytes);
    try std.fs.cwd().writeFile(.{
        .sub_path = path,
        .data = bytes,
    });
    return .{ .path = path };
}

fn fuzz_length(prng: *stdx.PRNG, index: usize) usize {
    if (index < edge_lengths.len) return edge_lengths[index];
    return 1 + prng.int_inclusive(usize, 8191);
}

fn run_tool(
    gpa: std.mem.Allocator,
    executable_path: []const u8,
    arguments: []const []const u8,
) !void {
    var argv = std.ArrayList([]const u8).init(gpa);
    defer argv.deinit();

    try argv.append(executable_path);
    for (arguments) |argument| try argv.append(argument);

    var child = std.process.Child.init(argv.items, gpa);
    const term = try child.spawnAndWait();
    if (term != .Exited or term.Exited != 0) return error.ToolFailed;
}

fn build_linux_fixture(
    gpa: std.mem.Allocator,
    command_line: CLIArgs,
    tmp_dir: []const u8,
) ![]const u8 {
    const source_path = try std.fmt.allocPrint(gpa, "{s}/fixture.zig", .{tmp_dir});
    defer gpa.free(source_path);

    try std.fs.cwd().writeFile(.{
        .sub_path = source_path,
        .data =
        \\pub fn main() void {}
        ,
    });

    const output_path = try std.fmt.allocPrint(gpa, "{s}/fixture", .{tmp_dir});
    const emit_bin_arg = try std.fmt.allocPrint(gpa, "-femit-bin={s}", .{output_path});
    defer gpa.free(emit_bin_arg);

    var child = std.process.Child.init(&.{
        command_line.zig_exe,
        "build-exe",
        source_path,
        "-O",
        "ReleaseSafe",
        "-fstrip",
        "--zig-lib-dir",
        "zig/lib",
        "--cache-dir",
        ".zig-cache",
        "--global-cache-dir",
        ".zig-cache",
        emit_bin_arg,
    }, gpa);
    const term = try child.spawnAndWait();
    if (term != .Exited or term.Exited != 0) return error.FixtureBuildFailed;
    return output_path;
}

fn assert_equal_files(gpa: std.mem.Allocator, a: []const u8, b: []const u8) !void {
    const a_bytes = try std.fs.cwd().readFileAlloc(gpa, a, std.math.maxInt(usize));
    defer gpa.free(a_bytes);

    const b_bytes = try std.fs.cwd().readFileAlloc(gpa, b, std.math.maxInt(usize));
    defer gpa.free(b_bytes);

    if (!std.mem.eql(u8, a_bytes, b_bytes)) return error.BytesMismatch;
}

fn assert_equal_stage(
    gpa: std.mem.Allocator,
    got: []const u8,
    want: []const u8,
    stage: []const u8,
    case_dir: []const u8,
) !void {
    assert_equal_files(gpa, got, want) catch |err| {
        std.log.err("mismatch after {s} in {s}", .{ stage, case_dir });
        return err;
    };
}

fn build_windows_fixture(
    gpa: std.mem.Allocator,
    command_line: CLIArgs,
    tmp_dir: []const u8,
    output_path: []const u8,
) !void {
    const source_path = try std.fmt.allocPrint(gpa, "{s}/fixture.zig", .{tmp_dir});
    defer gpa.free(source_path);

    try std.fs.cwd().writeFile(.{
        .sub_path = source_path,
        .data =
        \\pub fn main() void {}
        ,
    });

    const emit_bin_arg = try std.fmt.allocPrint(gpa, "-femit-bin={s}", .{output_path});
    defer gpa.free(emit_bin_arg);

    var child = std.process.Child.init(&.{
        command_line.zig_exe,
        "build-exe",
        source_path,
        "-O",
        "ReleaseSafe",
        "-fstrip",
        "-target",
        "x86_64-windows",
        "--zig-lib-dir",
        "zig/lib",
        "--cache-dir",
        ".zig-cache",
        "--global-cache-dir",
        ".zig-cache",
        emit_bin_arg,
    }, gpa);
    const term = try child.spawnAndWait();
    if (term != .Exited or term.Exited != 0) return error.FixtureBuildFailed;
}
