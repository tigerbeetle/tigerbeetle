//! Custom build step to prepare multiversion binaries.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const log = std.log;

const multiversioning = @import("./multiversioning.zig");
const flags = @import("flags.zig");
const Shell = @import("shell.zig");

const multiversion_binary_size_max = multiversioning.multiversion_binary_size_max;
const MultiversionHeader = multiversioning.MultiversionHeader;
const section_to_macho_cpu = multiversioning.section_to_macho_cpu;

const Target = union(enum) {
    const Arch = enum { x86_64, aarch64 };

    linux: Arch,
    windows: Arch,
    macos, // Universal binary packing both x86_64 and aarch64 versions.

    pub fn parse(str: []const u8) !Target {
        const targets = .{
            .{ "x86_64-linux", .{ .linux = .x86_64 } },
            .{ "aarch64-linux", .{ .linux = .aarch64 } },
            .{ "x86_64-windows", .{ .windows = .x86_64 } },
            .{ "aarch64-windows", .{ .windows = .aarch64 } },
            .{ "macos", .macos },
        };

        inline for (targets) |t| if (std.mem.eql(u8, str, t[0])) return t[1];
        return error.InvalidTarget;
    }
};

const CLIArgs = struct {
    target: []const u8,
    debug: bool = false,
    llvm_objcopy: []const u8,
    tigerbeetle_current: ?[]const u8 = null,
    tigerbeetle_current_x86_64: ?[]const u8 = null, // NB: Will be x86-64 on the CLI!
    tigerbeetle_current_aarch64: ?[]const u8 = null,
    tigerbeetle_past: []const u8,
    output: []const u8,
    tmp: []const u8,
};

// These are the options for cli_args.tigerbeetle_current. Ideally, they should be passed at
// runtime, but passing them at comptime is more convenient.
const vsr_options = @import("vsr_options");

pub fn main() !void {
    var allocator: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer {
        if (allocator.deinit() != .ok) {
            @panic("memory leaked");
        }
    }
    const gpa = allocator.allocator();

    const shell = try Shell.create(gpa);
    defer shell.destroy();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    const cli_args = flags.parse(&args, CLIArgs);

    const tmp_dir_path = try shell.fmt("{s}/{d}", .{
        cli_args.tmp,
        std.crypto.random.int(u64),
    });
    var tmp_dir = try std.fs.cwd().makeOpenPath(tmp_dir_path, .{});
    defer {
        tmp_dir.close();
        std.fs.cwd().deleteTree(tmp_dir_path) catch {};
    }

    const target = try Target.parse(cli_args.target);

    // When we fetch llvm-objcopy in build.zig, there isn't an easy way to mark it as
    // executable, so do it here.
    try shell.file_make_executable(cli_args.llvm_objcopy);

    switch (target) {
        .windows, .linux => try build_multiversion_single_arch(shell, .{
            .llvm_objcopy = cli_args.llvm_objcopy,
            .tmp_path = tmp_dir_path,
            .target = target,
            .debug = cli_args.debug,
            .tigerbeetle_current = cli_args.tigerbeetle_current.?,
            .tigerbeetle_past = cli_args.tigerbeetle_past,
            .output = cli_args.output,
        }),
        .macos => try build_multiversion_universal(shell, .{
            .llvm_objcopy = cli_args.llvm_objcopy,
            .tmp_path = tmp_dir_path,
            .target = target,
            .debug = cli_args.debug,
            .tigerbeetle_current_x86_64 = cli_args.tigerbeetle_current_x86_64.?,
            .tigerbeetle_current_aarch64 = cli_args.tigerbeetle_current_aarch64.?,
            .tigerbeetle_past = cli_args.tigerbeetle_past,
            .output = cli_args.output,
        }),
    }
}

fn build_multiversion_single_arch(shell: *Shell, options: struct {
    llvm_objcopy: []const u8,
    tmp_path: []const u8,
    target: Target,
    debug: bool,
    tigerbeetle_current: []const u8,
    tigerbeetle_past: []const u8,
    output: []const u8,
}) !void {
    assert(options.target != .macos);

    // We will be modifying this binary in-place.
    const tigerbeetle_working = try shell.fmt("{s}/tigerbeetle-working", .{options.tmp_path});

    const current_checksum = try make_deterministic(shell, .{
        .llvm_objcopy = options.llvm_objcopy,
        .source = options.tigerbeetle_current,
        .output = tigerbeetle_working,
    });

    const sections = .{
        .header_zero = try shell.fmt("{s}/multiversion-zero.header", .{options.tmp_path}),
        .header = try shell.fmt("{s}/multiversion.header", .{options.tmp_path}),
        .body = try shell.fmt("{s}/multiversion.body", .{options.tmp_path}),
    };

    // Explicitly write out zeros for the header, to compute the checksum.
    try shell.cwd.writeFile(.{
        .sub_path = sections.header_zero,
        .data = std.mem.asBytes(&std.mem.zeroes(MultiversionHeader)),
        .flags = .{ .exclusive = true },
    });

    const past_versions = try build_multiversion_body(shell, .{
        .llvm_objcopy = options.llvm_objcopy,
        .tmp_path = options.tmp_path,
        .target = options.target,
        .arch = switch (options.target) {
            inline .windows, .linux => |arch| arch,
            .macos => unreachable,
        },
        .tigerbeetle_past = options.tigerbeetle_past,
        .output = sections.body,
    });

    // Use objcopy to add in our new body, as well as its header - even though the
    // header is still zero!
    try shell.exec(
        \\{llvm_objcopy} --enable-deterministic-archives --keep-undefined
        \\
        \\    --add-section .tb_mvb={body}
        \\    --set-section-flags .tb_mvb=contents,noload,readonly
        \\
        \\    --add-section .tb_mvh={header_zero}
        \\    --set-section-flags .tb_mvh=contents,noload,readonly
        \\
        \\    {working}
    , .{
        .llvm_objcopy = options.llvm_objcopy,
        .body = sections.body,
        .header_zero = sections.header_zero,
        .working = tigerbeetle_working,
    });

    const checksum_binary_without_header = try checksum_file(
        shell,
        tigerbeetle_working,
        multiversioning.multiversion_binary_size_max,
    );

    var header: MultiversionHeader = .{
        .current_release = (try multiversioning.Release.parse(vsr_options.release.?)).value,
        .current_checksum = current_checksum,
        .current_flags = .{
            .debug = options.debug,
            .visit = true,
        },
        .past = past_versions.past_releases,
        .checksum_binary_without_header = checksum_binary_without_header,
        .current_release_client_min = (try multiversioning.Release.parse(
            vsr_options.release_client_min.?,
        )).value,
        .current_git_commit = try git_sha_to_binary(&vsr_options.git_commit.?),
    };
    header.checksum_header = header.calculate_header_checksum();

    try shell.cwd.writeFile(.{
        .sub_path = sections.header,
        .data = std.mem.asBytes(&header),
        .flags = .{ .exclusive = true },
    });

    // Replace the header with the final version.
    try shell.exec(
        \\{llvm_objcopy} --enable-deterministic-archives --keep-undefined
        \\
        \\   --remove-section .tb_mvh
        \\   --add-section .tb_mvh={header}
        \\   --set-section-flags .tb_mvh=contents,noload,readonly
        \\
        \\  {working}
    , .{
        .header = sections.header,
        .llvm_objcopy = options.llvm_objcopy,
        .working = tigerbeetle_working,
    });

    try shell.cwd.copyFile(tigerbeetle_working, shell.cwd, options.output, .{});

    if (self_check_enabled(options.target)) {
        try self_check(shell, options.output, past_versions.unpacked);
    }
}

fn build_multiversion_universal(shell: *Shell, options: struct {
    llvm_objcopy: []const u8,
    tmp_path: []const u8,
    target: Target,
    debug: bool,
    tigerbeetle_current_x86_64: []const u8,
    tigerbeetle_current_aarch64: []const u8,
    tigerbeetle_past: []const u8,
    output: []const u8,
}) !void {
    assert(options.target == .macos);

    const tigerbeetle_zero_header = try shell.fmt("{s}/tigerbeetle-zero-header", .{
        options.tmp_path,
    });

    const sections = .{
        .header_zero = try shell.fmt("{s}/multiversion-zero.header", .{options.tmp_path}),
        .x86_64 = .{
            .header = try shell.fmt("{s}/multiversion-x86_64.header", .{options.tmp_path}),
            .body = try shell.fmt("{s}/multiversion-x86_64.body", .{options.tmp_path}),
        },
        .aarch64 = .{
            .header = try shell.fmt("{s}/multiversion-aarch64.header", .{options.tmp_path}),
            .body = try shell.fmt("{s}/multiversion-aarch64.body", .{options.tmp_path}),
        },
    };

    // Explicitly write out zeros for the header, to compute the checksum.
    try shell.cwd.writeFile(.{
        .sub_path = sections.header_zero,
        .data = std.mem.asBytes(&std.mem.zeroes(MultiversionHeader)),
        .flags = .{ .exclusive = true },
    });

    assert(builtin.target.cpu.arch == .x86_64 or builtin.target.cpu.arch == .aarch64);
    const past_versions_aarch64 = try build_multiversion_body(shell, .{
        .llvm_objcopy = options.llvm_objcopy,
        .tmp_path = options.tmp_path,
        .target = .macos,
        .arch = .x86_64,
        .tigerbeetle_past = options.tigerbeetle_past,
        .output = sections.aarch64.body,
    });

    const past_versions_x86_64 = try build_multiversion_body(shell, .{
        .llvm_objcopy = options.llvm_objcopy,
        .tmp_path = options.tmp_path,
        .target = .macos,
        .arch = .aarch64,
        .tigerbeetle_past = options.tigerbeetle_past,
        .output = sections.x86_64.body,
    });
    assert(past_versions_aarch64.past_releases.count == past_versions_x86_64.past_releases.count);

    try macos_universal_binary_build(
        shell,
        tigerbeetle_zero_header,
        &.{
            .{
                .cpu_type = std.macho.CPU_TYPE_ARM64,
                .cpu_subtype = std.macho.CPU_SUBTYPE_ARM_ALL,
                .path = options.tigerbeetle_current_aarch64,
            },
            .{
                .cpu_type = std.macho.CPU_TYPE_X86_64,
                .cpu_subtype = std.macho.CPU_SUBTYPE_X86_64_ALL,
                .path = options.tigerbeetle_current_x86_64,
            },
            .{
                .cpu_type = @intFromEnum(section_to_macho_cpu.tb_mvb_aarch64),
                .cpu_subtype = 0x00000000,
                .path = sections.aarch64.body,
            },
            .{
                .cpu_type = @intFromEnum(section_to_macho_cpu.tb_mvh_aarch64),
                .cpu_subtype = 0x00000000,
                .path = sections.header_zero,
            },
            .{
                .cpu_type = @intFromEnum(section_to_macho_cpu.tb_mvb_x86_64),
                .cpu_subtype = 0x00000000,
                .path = sections.x86_64.body,
            },
            .{
                .cpu_type = @intFromEnum(section_to_macho_cpu.tb_mvh_x86_64),
                .cpu_subtype = 0x00000000,
                .path = sections.header_zero,
            },
        },
    );
    const checksum_binary_without_header = try checksum_file(
        shell,
        tigerbeetle_zero_header,
        multiversion_binary_size_max,
    );

    inline for (
        .{ options.tigerbeetle_current_aarch64, options.tigerbeetle_current_x86_64 },
        .{ past_versions_aarch64, past_versions_x86_64 },
        .{ sections.aarch64.header, sections.x86_64.header },
    ) |tigerbeetle_current, past_versions, header_name| {
        const current_checksum = try checksum_file(
            shell,
            tigerbeetle_current,
            multiversion_binary_size_max,
        );

        var header = multiversioning.MultiversionHeader{
            .current_release = (try multiversioning.Release.parse(vsr_options.release.?)).value,
            .current_checksum = current_checksum,
            .current_flags = .{
                .debug = options.debug,
                .visit = true,
            },
            .past = past_versions.past_releases,
            .checksum_binary_without_header = checksum_binary_without_header,
            .current_release_client_min = (try multiversioning.Release.parse(
                vsr_options.release_client_min.?,
            )).value,
            .current_git_commit = try git_sha_to_binary(&vsr_options.git_commit.?),
        };
        header.checksum_header = header.calculate_header_checksum();

        try shell.cwd.writeFile(.{
            .sub_path = header_name,
            .data = std.mem.asBytes(&header),
            .flags = .{ .exclusive = true },
        });
    }

    try macos_universal_binary_build(shell, options.output, &.{
        .{
            .cpu_type = std.macho.CPU_TYPE_ARM64,
            .cpu_subtype = std.macho.CPU_SUBTYPE_ARM_ALL,
            .path = options.tigerbeetle_current_aarch64,
        },
        .{
            .cpu_type = std.macho.CPU_TYPE_X86_64,
            .cpu_subtype = std.macho.CPU_SUBTYPE_X86_64_ALL,
            .path = options.tigerbeetle_current_x86_64,
        },
        .{
            .cpu_type = @intFromEnum(section_to_macho_cpu.tb_mvb_aarch64),
            .cpu_subtype = 0x00000000,
            .path = sections.aarch64.body,
        },
        .{
            .cpu_type = @intFromEnum(section_to_macho_cpu.tb_mvh_aarch64),
            .cpu_subtype = 0x00000000,
            .path = sections.aarch64.header,
        },
        .{
            .cpu_type = @intFromEnum(section_to_macho_cpu.tb_mvb_x86_64),
            .cpu_subtype = 0x00000000,
            .path = sections.x86_64.body,
        },
        .{
            .cpu_type = @intFromEnum(section_to_macho_cpu.tb_mvh_x86_64),
            .cpu_subtype = 0x00000000,
            .path = sections.x86_64.header,
        },
    });
}

fn make_deterministic(shell: *Shell, options: struct {
    llvm_objcopy: []const u8,
    source: []const u8,
    output: []const u8,
}) !u128 {
    // Copy the object using llvm-objcopy before taking our hash. This is to ensure we're
    // round trip deterministic between adding and removing sections:
    // `llvm-objcopy --add-section ... src dst_added` followed by
    // `llvm-objcopy --remove-section ... dst_added src_back` means
    // checksum(src) == checksum(src_back)
    // Note: actually don't think this is needed, we could assert it?
    try shell.exec(
        \\{llvm_objcopy} --enable-deterministic-archives
        \\    {source} {working}
    , .{
        .llvm_objcopy = options.llvm_objcopy,
        .source = options.source,
        .working = options.output,
    });

    return try checksum_file(
        shell,
        options.output,
        multiversioning.multiversion_binary_size_max,
    );
}

fn build_multiversion_body(shell: *Shell, options: struct {
    llvm_objcopy: []const u8,
    tmp_path: []const u8,
    target: Target,
    arch: Target.Arch,
    tigerbeetle_past: []const u8,
    output: []const u8,
}) !struct {
    past_releases: MultiversionHeader.PastReleases,
    unpacked: []const []const u8,
} {
    const past_binary_contents: []align(8) const u8 = try shell.cwd.readFileAllocOptions(
        shell.arena.allocator(),
        options.tigerbeetle_past,
        multiversion_binary_size_max,
        null,
        8,
        null,
    );

    const parsed_offsets = switch (options.target) {
        .windows => try multiversioning.parse_pe(past_binary_contents),
        .macos => try multiversioning.parse_macho(past_binary_contents),
        .linux => try multiversioning.parse_elf(past_binary_contents),
    };
    const arch_offsets = switch (options.arch) {
        .x86_64 => parsed_offsets.x86_64.?,
        .aarch64 => parsed_offsets.aarch64.?,
    };

    const header_bytes =
        past_binary_contents[arch_offsets.header_offset..][0..@sizeOf(MultiversionHeader)];

    var header = try MultiversionHeader.init_from_bytes(header_bytes);
    if (header.current_release == (try multiversioning.Release.parse("0.15.4")).value) {
        // current_git_commit and current_release_client_min were added after 0.15.4. These are the
        // values for that release.
        header.current_git_commit = try git_sha_to_binary(
            "14abaeabd09bd7c78a95b6b990748f3612b3e4cc",
        );
        header.current_release_client_min = (try multiversioning.Release.parse("0.15.3")).value;
    }

    var unpacked = std.ArrayList([]const u8).init(shell.arena.allocator());
    var releases = multiversioning.ListU32{};
    var checksums = multiversioning.ListU128{};
    var offsets = multiversioning.ListU32{};
    var sizes = multiversioning.ListU32{};
    var flags_ = multiversioning.ListFlag{};
    var git_commits = multiversioning.ListGitCommit{};
    var release_client_mins = multiversioning.ListU32{};

    // Extract the old current release - this is the release that was the current release, and not
    // embedded in the past pack.
    const old_current_release = header.current_release;
    const old_current_release_output_name = try shell.fmt("{s}/tigerbeetle-past-{}-{s}", .{
        options.tmp_path,
        multiversioning.Release{ .value = old_current_release },
        @tagName(options.arch),
    });

    if (options.target == .macos) {
        const cpu_type, const cpu_subtype = switch (options.arch) {
            .aarch64 => .{ std.macho.CPU_TYPE_ARM64, std.macho.CPU_SUBTYPE_ARM_ALL },
            .x86_64 => .{ std.macho.CPU_TYPE_X86_64, std.macho.CPU_SUBTYPE_X86_64_ALL },
        };

        try macos_universal_binary_extract(
            shell,
            options.tigerbeetle_past,
            .{ .cpu_type = cpu_type, .cpu_subtype = cpu_subtype },
            old_current_release_output_name,
        );
    } else {
        try shell.exec(
            \\{llvm_objcopy} --enable-deterministic-archives --keep-undefined
            \\     --remove-section .tb_mvb --remove-section .tb_mvh
            \\     {tigerbeetle_past} {tigerbeetle_old_current}
        , .{
            .llvm_objcopy = options.llvm_objcopy,
            .tigerbeetle_past = options.tigerbeetle_past,
            .tigerbeetle_old_current = old_current_release_output_name,
        });
    }

    if (builtin.os.tag != .windows) {
        const old_current_release_fd = try shell.cwd.openFile(old_current_release_output_name, .{
            .mode = .write_only,
        });
        defer old_current_release_fd.close();
        try old_current_release_fd.chmod(0o777);
    }

    // It's important to verify the previous current_release checksum - it can't be verified at
    // runtime by multiversioning.zig, since it relies on objcopy to extract.
    assert(header.current_checksum == try checksum_file(
        shell,
        old_current_release_output_name,
        multiversion_binary_size_max,
    ));

    const old_current_release_size: u32 = @intCast(
        (try shell.cwd.statFile(old_current_release_output_name)).size,
    );

    // You can have as many releases as you want, as long as it's 6 or less.
    // This is made up of:
    // * up to 4 releases from the old past pack (extracted from the release downloaded),
    // * 1 old current release (extracted from the release downloaded),
    // * 1 current release (that was just built).
    // This will be improved soon:
    // https://github.com/tigerbeetle/tigerbeetle/pull/2165#discussion_r1698114401
    //
    // No size limits are explicitly checked here; they're validated later by using the
    // `multiversion` subcommand to test the final built binary against all past binaries that are
    // included.
    const past_count = @min(4, header.past.count);

    const past_starting_index = header.past.count - past_count;

    for (
        header.past.releases[past_starting_index..][0..past_count],
        header.past.offsets[past_starting_index..][0..past_count],
        header.past.sizes[past_starting_index..][0..past_count],
        header.past.checksums[past_starting_index..][0..past_count],
        header.past.flags[past_starting_index..][0..past_count],
        header.past.git_commits[past_starting_index..][0..past_count],
        header.past.release_client_mins[past_starting_index..][0..past_count],
    ) |
        past_release,
        past_offset,
        past_size,
        past_checksum,
        past_flag,
        past_commit,
        past_release_client_min,
    | {
        const past_name = try shell.fmt("{s}/tigerbeetle-past-{}-{s}", .{
            options.tmp_path,
            multiversioning.Release{ .value = past_release },
            @tagName(options.arch),
        });
        const mode_exec = if (builtin.os.tag == .windows) 0 else 0o777;
        try shell.cwd.writeFile(.{
            .sub_path = past_name,
            .data = past_binary_contents[arch_offsets.body_offset..][past_offset..][0..past_size],
            .flags = .{ .exclusive = true, .mode = mode_exec },
        });
        try unpacked.append(past_name);

        // This is double-checked later when validating at runtime with the binary.
        assert(past_checksum == try checksum_file(
            shell,
            past_name,
            multiversion_binary_size_max,
        ));

        const offset = blk: {
            var offset: u32 = 0;
            for (sizes.const_slice()) |size| {
                offset += size;
            }
            break :blk offset;
        };

        releases.append_assume_capacity(past_release);
        checksums.append_assume_capacity(past_checksum);
        offsets.append_assume_capacity(offset);
        sizes.append_assume_capacity(past_size);
        flags_.append_assume_capacity(past_flag);
        git_commits.append_assume_capacity(past_commit);
        release_client_mins.append_assume_capacity(past_release_client_min);
    }

    const old_current_release_offset = blk: {
        var offset: u32 = 0;
        for (sizes.const_slice()) |s| {
            offset += s;
        }
        break :blk offset;
    };

    const old_current_release_flags = blk: {
        var old_current_release_flags = header.current_flags;

        // Visit https://github.com/tigerbeetle/tigerbeetle/pull/2181.
        old_current_release_flags.visit = true;

        break :blk old_current_release_flags;
    };

    // All of these are in ascending order, so the old current release goes last:

    releases.append_assume_capacity(old_current_release);
    checksums.append_assume_capacity(header.current_checksum);
    offsets.append_assume_capacity(old_current_release_offset);
    sizes.append_assume_capacity(old_current_release_size);
    flags_.append_assume_capacity(old_current_release_flags);
    git_commits.append_assume_capacity(header.current_git_commit);
    release_client_mins.append_assume_capacity(header.current_release_client_min);
    try unpacked.append(old_current_release_output_name);

    const body_file = try shell.cwd.createFile(options.output, .{ .exclusive = true });
    defer body_file.close();

    for (
        releases.const_slice(),
        offsets.const_slice(),
        sizes.const_slice(),
    ) |release, offset, size| {
        const past_name = try shell.fmt("{s}/tigerbeetle-past-{}-{s}", .{
            options.tmp_path,
            multiversioning.Release{ .value = release },
            @tagName(options.arch),
        });
        const contents = try shell.cwd.readFileAlloc(shell.arena.allocator(), past_name, size);
        try body_file.pwriteAll(contents, offset);
    }

    return .{
        // past_count + 1 to include the old current release.
        .past_releases = MultiversionHeader.PastReleases.init(past_count + 1, .{
            .releases = releases.const_slice(),
            .checksums = checksums.const_slice(),
            .offsets = offsets.const_slice(),
            .sizes = sizes.const_slice(),
            .flags = flags_.const_slice(),
            .git_commits = git_commits.const_slice(),
            .release_client_mins = release_client_mins.const_slice(),
        }),
        .unpacked = unpacked.items,
    };
}

/// Does the same thing as llvm-lipo (builds a universal binary) but allows building binaries
/// that have deprecated architectures. This is used by multiversioning on macOS, where these
/// deprecated architectures hold the multiversion header and body.
/// It's much easier to embed and read them here, then to do it in the inner MachO binary, like
/// we do with ELF or PE.
fn macos_universal_binary_build(
    shell: *Shell,
    output_path: []const u8,
    binaries: []const struct {
        cpu_type: i32,
        cpu_subtype: i32,
        path: []const u8,
    },
) !void {
    // The offset start is relative to the end of the headers, rounded up to the alignment.
    const alignment_power = 14;
    const alignment = 1 << alignment_power;

    // Ensure alignment of 2^14 == 16384 to match macOS.
    comptime assert(alignment == 16384);

    const headers_size = @sizeOf(std.macho.fat_header) +
        @sizeOf(std.macho.fat_arch) * binaries.len;
    assert(headers_size < alignment);

    const binary_headers = try shell.arena.allocator().alloc(std.macho.fat_arch, binaries.len);

    var current_offset: u32 = alignment;
    for (binaries, binary_headers) |binary, *binary_header| {
        const binary_size: u32 = @intCast(
            (try shell.cwd.statFile(binary.path)).size,
        );

        // The Mach-O header is big-endian...
        binary_header.* = std.macho.fat_arch{
            .cputype = @byteSwap(binary.cpu_type),
            .cpusubtype = @byteSwap(binary.cpu_subtype),
            .offset = @byteSwap(current_offset),
            .size = @byteSwap(binary_size),
            .@"align" = @byteSwap(@as(u32, alignment_power)),
        };

        current_offset += binary_size;
        current_offset = std.mem.alignForward(u32, current_offset, alignment);
    }

    var output_file = try shell.project_root.createFile(output_path, .{
        .exclusive = true,
        .mode = if (builtin.target.os.tag == .windows) 0 else 0o777,
    });
    defer output_file.close();

    const fat_header = std.macho.fat_header{
        .magic = std.macho.FAT_CIGAM,
        .nfat_arch = @byteSwap(@as(u32, @intCast(binaries.len))),
    };
    assert(@sizeOf(std.macho.fat_header) == 8);
    try output_file.writeAll(std.mem.asBytes(&fat_header));

    assert(@sizeOf(std.macho.fat_arch) == 20);
    try output_file.writeAll(std.mem.sliceAsBytes(binary_headers));

    try output_file.seekTo(alignment);

    for (binaries, binary_headers) |binary, binary_header| {
        const binary_contents = try shell.project_root.readFileAlloc(
            shell.arena.allocator(),
            binary.path,
            multiversion_binary_size_max,
        );
        assert(binary_contents.len == @byteSwap(binary_header.size));

        try output_file.seekTo(@byteSwap(binary_header.offset));
        try output_file.writeAll(binary_contents);
    }
}

/// Does the opposite of macos_universal_binary_build: allows extracting inner binaries from a
/// universal binary.
fn macos_universal_binary_extract(
    shell: *Shell,
    input_path: []const u8,
    filter: struct { cpu_type: i32, cpu_subtype: i32 },
    output_path: []const u8,
) !void {
    const binary_contents = try shell.cwd.readFileAlloc(
        shell.arena.allocator(),
        input_path,
        multiversion_binary_size_max,
    );

    const fat_header = std.mem.bytesAsValue(
        std.macho.fat_header,
        binary_contents[0..@sizeOf(std.macho.fat_header)],
    );
    assert(fat_header.magic == std.macho.FAT_CIGAM);

    for (0..@byteSwap(fat_header.nfat_arch)) |i| {
        const header_offset = @sizeOf(std.macho.fat_header) + @sizeOf(std.macho.fat_arch) * i;
        const fat_arch = std.mem.bytesAsValue(
            std.macho.fat_arch,
            binary_contents[header_offset..][0..@sizeOf(std.macho.fat_arch)],
        );
        assert(@byteSwap(fat_arch.@"align") == 14);

        if (@byteSwap(fat_arch.cputype) == filter.cpu_type and
            @byteSwap(fat_arch.cpusubtype) == filter.cpu_subtype)
        {
            const offset = @byteSwap(fat_arch.offset);
            const size = @byteSwap(fat_arch.size);

            try shell.cwd.writeFile(.{
                .sub_path = output_path,
                .data = binary_contents[offset..][0..size],
                .flags = .{ .exclusive = true },
            });

            break;
        }
    } else {
        @panic("no matching inner binary found.");
    }
}

fn self_check_enabled(target: Target) bool {
    return switch (target) {
        .linux => |arch| builtin.target.os.tag == .linux and switch (arch) {
            .x86_64 => builtin.target.cpu.arch == .x86_64,
            .aarch64 => builtin.target.cpu.arch == .aarch64,
        },
        .windows => |arch| builtin.target.os.tag == .windows and switch (arch) {
            .x86_64 => builtin.target.cpu.arch == .x86_64,
            .aarch64 => builtin.target.cpu.arch == .aarch64,
        },
        .macos => builtin.target.os.tag == .macos,
    };
}

fn self_check(shell: *Shell, multiversion: []const u8, past_releases: []const []const u8) !void {
    assert(past_releases.len > 0);
    try shell.exec_options(
        .{ .echo = false },
        "{multiversion} multiversion {multiversion}",
        .{ .multiversion = multiversion },
    );
    for (past_releases) |past_release| {
        // 0.15.3 didn't have the multiversion subcommand since it was the epoch.
        if (std.mem.indexOf(u8, past_release, "0.15.3") != null) continue;
        try shell.exec_options(
            .{ .echo = false },
            "{past_release} multiversion {multiversion}",
            .{ .multiversion = multiversion, .past_release = past_release },
        );
    }
}

fn checksum_file(shell: *Shell, path: []const u8, size_max: u32) !u128 {
    const contents = try shell.cwd.readFileAlloc(shell.arena.allocator(), path, size_max);
    return multiversioning.checksum.checksum(contents);
}

fn git_sha_to_binary(commit: []const u8) ![20]u8 {
    assert(commit.len == 40);

    var commit_bytes: [20]u8 = std.mem.zeroes([20]u8);
    for (0..@divExact(commit.len, 2)) |i| {
        const byte = try std.fmt.parseInt(u8, commit[i * 2 ..][0..2], 16);
        commit_bytes[i] = byte;
    }

    var commit_roundtrip: [40]u8 = undefined;
    assert(std.mem.eql(u8, try std.fmt.bufPrint(
        &commit_roundtrip,
        "{s}",
        .{std.fmt.fmtSliceHexLower(&commit_bytes)},
    ), commit));

    return commit_bytes;
}
