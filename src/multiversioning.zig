const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;
const os = std.os;
const linux = os.linux;
const native_endian = @import("builtin").target.cpu.arch.endian();
const constants = @import("constants.zig");

const elf = std.elf;

// Re-export to make release code easier.
pub const checksum = @import("vsr/checksum.zig").checksum;

/// Creates a virtual file backed by memory.
pub fn open_memory_file(name: [*:0]const u8) os.fd_t {
    return @intCast(linux.memfd_create(name, 0));
}

pub fn execveat(dirfd: i32, path: [*:0]const u8, argv: [*:null]const ?[*:0]const u8, envp: [*:null]const ?[*:0]const u8, flags: i32) usize {
    return std.os.linux.syscall5(
        .execveat,
        @as(usize, @bitCast(@as(isize, dirfd))),
        @intFromPtr(path),
        @intFromPtr(argv),
        @intFromPtr(envp),
        @as(usize, @bitCast(@as(isize, flags))),
    );
}

/// A ReleaseList is ordered from lowest-to-highest version.
pub const ReleaseList = stdx.BoundedArray(Release, constants.vsr_releases_max);

pub const Release = extern struct {
    value: u32,

    comptime {
        assert(@sizeOf(Release) == 4);
        assert(@sizeOf(Release) == @sizeOf(ReleaseTriple));
        assert(stdx.no_padding(Release));
    }

    pub const zero = Release.from(.{ .major = 0, .minor = 0, .patch = 0 });
    // Minimum is used for all development builds, to distinguish them from production deployments.
    pub const minimum = Release.from(.{ .major = 0, .minor = 0, .patch = 1 });

    pub fn from(release_triple: ReleaseTriple) Release {
        return std.mem.bytesAsValue(Release, std.mem.asBytes(&release_triple)).*;
    }

    pub fn triple(release: *const Release) ReleaseTriple {
        return std.mem.bytesAsValue(ReleaseTriple, std.mem.asBytes(release)).*;
    }

    pub fn format(
        release: Release,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        const release_triple = release.triple();
        return writer.print("{}.{}.{}", .{
            release_triple.major,
            release_triple.minor,
            release_triple.patch,
        });
    }

    pub fn max(a: Release, b: Release) Release {
        if (a.value > b.value) {
            return a;
        } else {
            return b;
        }
    }
};

pub const ReleaseTriple = extern struct {
    patch: u8,
    minor: u8,
    major: u16,

    comptime {
        assert(@sizeOf(ReleaseTriple) == 4);
        assert(stdx.no_padding(ReleaseTriple));
    }

    pub fn parse(string: []const u8) error{InvalidRelease}!ReleaseTriple {
        var parts = std.mem.splitScalar(u8, string, '.');
        const major = parts.first();
        const minor = parts.next() orelse return error.InvalidRelease;
        const patch = parts.next() orelse return error.InvalidRelease;
        if (parts.next() != null) return error.InvalidRelease;
        return .{
            .major = std.fmt.parseUnsigned(u16, major, 10) catch return error.InvalidRelease,
            .minor = std.fmt.parseUnsigned(u8, minor, 10) catch return error.InvalidRelease,
            .patch = std.fmt.parseUnsigned(u8, patch, 10) catch return error.InvalidRelease,
        };
    }
};

test "ReleaseTriple.parse" {
    const tests = [_]struct {
        string: []const u8,
        result: error{InvalidRelease}!ReleaseTriple,
    }{
        // Valid:
        .{ .string = "0.0.1", .result = .{ .major = 0, .minor = 0, .patch = 1 } },
        .{ .string = "0.1.0", .result = .{ .major = 0, .minor = 1, .patch = 0 } },
        .{ .string = "1.0.0", .result = .{ .major = 1, .minor = 0, .patch = 0 } },

        // Invalid characters:
        .{ .string = "v0.0.1", .result = error.InvalidRelease },
        .{ .string = "0.0.1v", .result = error.InvalidRelease },
        // Invalid separators:
        .{ .string = "0.0.0.1", .result = error.InvalidRelease },
        .{ .string = "0..0.1", .result = error.InvalidRelease },
        // Overflow (and near-overflow):
        .{ .string = "0.0.255", .result = .{ .major = 0, .minor = 0, .patch = 255 } },
        .{ .string = "0.0.256", .result = error.InvalidRelease },
        .{ .string = "0.255.0", .result = .{ .major = 0, .minor = 255, .patch = 0 } },
        .{ .string = "0.256.0", .result = error.InvalidRelease },
        .{ .string = "65535.0.0", .result = .{ .major = 65535, .minor = 0, .patch = 0 } },
        .{ .string = "65536.0.0", .result = error.InvalidRelease },
    };
    for (tests) |t| {
        try std.testing.expectEqualDeep(ReleaseTriple.parse(t.string), t.result);
    }
}

pub const MultiVersionMetadata = extern struct {
    // When slicing into the binary, checksum(section[past_offset..past_offset+past_size]) == past_checksum.
    // This is then validated when the binary is written to a memfd or similar.
    // `current_checksum` becomes a `past_checksum` when put in here by our release builder.
    // Offsets are relative to the start of the `.tigerbeetle.multiversion.pack` section.
    pub const PastVersionPack = extern struct {
        count: u32 = 0,

        // Technically -1 on all of these since we have to account for current version...
        versions: [constants.vsr_releases_max]u32 = std.mem.zeroes([constants.vsr_releases_max]u32),
        checksums: [constants.vsr_releases_max]u128 = std.mem.zeroes([constants.vsr_releases_max]u128),
        offsets: [constants.vsr_releases_max]u32 = std.mem.zeroes([constants.vsr_releases_max]u32),
        sizes: [constants.vsr_releases_max]u32 = std.mem.zeroes([constants.vsr_releases_max]u32),

        pub fn init(params: struct { count: u32, versions: []const u32, checksums: []const u128, offsets: []const u32, sizes: []const u32 }) PastVersionPack {
            assert(params.versions.len == params.count);
            assert(params.checksums.len == params.count);
            assert(params.offsets.len == params.count);
            assert(params.sizes.len == params.count);

            var pack = PastVersionPack{};
            pack.count = params.count;

            std.mem.copy(u32, &pack.versions, params.versions);
            std.mem.copy(u128, &pack.checksums, params.checksums);
            std.mem.copy(u32, &pack.offsets, params.offsets);
            std.mem.copy(u32, &pack.sizes, params.sizes);

            return pack;
        }
    };

    /// We can't write out and exec our current version, so store it separately.
    current_version: u32,

    /// The AEGIS128L checksum of the direct output of `zig build` before any objcopy magic has been
    /// performed.
    /// Used when extracting binaries from past releases to ensure a hash chain.
    current_checksum: u128,

    /// The AEGIS128L checksum of the binary, if the `.tigerbeetle.multiversion.metadata` section
    /// were zero'd out.
    /// Used to validate that the binary itself is not corrupt. Putting this in requires a bit of
    /// trickery: we inject a dummy `.tigerbeetle.multiversion.metadata` section of the correct
    /// size, compute the hash, and then update it - so that we hash the final ELF headers.
    /// Used to ensure we don't try and exec into a corrupt binary.
    checksum_without_metadata: u128 = 0,

    past: PastVersionPack = .{},

    /// Covers MultiVersionMetadata[0..@sizeOf(MultiVersionMetadata) - @sizeOf(u128)].
    checksum_metadata: u128 = undefined,

    pub fn from_bytes(bytes: []const u8) !MultiVersionMetadata {
        const self: *const MultiVersionMetadata = @ptrCast(@alignCast(bytes));

        const checksum_calculated = self.calculate_metadata_checksum();

        if (checksum_calculated != self.checksum_metadata) {
            return error.ChecksumMismatch;
        }

        return self.*;
    }

    pub fn calculate_metadata_checksum(self: *const MultiVersionMetadata) u128 {
        // Checksum must have been set by this point.
        assert(self.checksum_without_metadata != 0);

        const self_without_checksum = std.mem.asBytes(self)[0 .. @sizeOf(MultiVersionMetadata) - @sizeOf(u128)];

        return checksum(self_without_checksum);
    }

    pub fn releases_bundled(self: *const MultiVersionMetadata) ReleaseList {
        var release_list = ReleaseList{};

        for (self.past.versions[0..self.past.count]) |version| {
            release_list.append_assume_capacity(Release{ .value = version });
        }

        release_list.append_assume_capacity(Release{ .value = self.current_version });

        return release_list;
    }

    comptime {
        // Changing this will affect the structure stored on disk, and has implications for past clients trying to read!
        assert(constants.vsr_releases_max == 64);
    }
};

pub const MultiVersion = struct {
    pack_offset: u32,
    metadata: MultiVersionMetadata,

    /// Used for validation, to know what to zero.
    metadata_offset: u32,

    /// Used to validate the full contents of a binary.
    pub fn validate(self: *const MultiVersion, binary_path: []const u8, allocator: std.mem.Allocator) !void {
        // The metadata checksum is validated when creating the MultiVersionMetadata object itself.

        const file = try std.fs.openFileAbsolute(binary_path, .{ .mode = .read_only });
        defer file.close();

        // FIXME: Remove use of allocator! Do this streaming!
        const contents = try file.readToEndAlloc(allocator, 128 * 1024 * 1024);
        defer allocator.free(contents);
        @memset(contents[self.metadata_offset..][0..@sizeOf(MultiVersionMetadata)], 0);

        const checksum_calculated = checksum(contents);
        std.log.info("validate(): checksum_calculated={} metadata.checksum_without_metadata={}", .{ checksum_calculated, self.metadata.checksum_without_metadata });

        if (self.metadata.checksum_without_metadata != checksum_calculated) {
            return error.ChecksumMismatch;
        }
    }

    pub fn from_own_binary() !MultiVersion {
        // openSelfExe is a thing, but don't use it for now because we might want our event loop to handle opening
        // in future. (can open() block?)
        var self_binary_buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
        var self_binary_path = try std.fs.selfExePath(&self_binary_buf);

        return MultiVersion.from_elf(self_binary_path);
    }

    pub fn from_elf(binary_path: []const u8) !MultiVersion {
        var hdr_buf: [@sizeOf(elf.Elf64_Ehdr)]u8 align(@alignOf(elf.Elf64_Ehdr)) = undefined;

        const file = try std.fs.openFileAbsolute(binary_path, .{ .mode = .read_only });
        defer file.close();

        assert(hdr_buf.len == try os.read(file.handle, &hdr_buf));

        const elf_header = try elf.Header.parse(&hdr_buf);

        // NB: We don't want this function to panic! It might be run on a garbage file, in which
        // case panicing could impact availability. Rather, turn these asserts to error returns.

        // TigerBeetle only supports little endian on 64 bit platforms.
        try assert_or_error(elf_header.endian == .Little, error.WrongEndian);
        try assert_or_error(elf_header.is_64, error.Not64bit);

        // Only support "simple" ELF string tables.
        try assert_or_error(elf_header.shstrndx < elf.SHN_LORESERVE, error.LongStringTable);
        try assert_or_error(elf_header.shstrndx != elf.SHN_UNDEF, error.LongStringTable);

        // First, read the string table.
        const string_max = 512;
        var shdr: elf.Elf64_Shdr = undefined;
        const offset = elf_header.shoff + @sizeOf(@TypeOf(shdr)) * elf_header.shstrndx;

        try file.seekTo(offset);
        try assert_or_error(@sizeOf(elf.Elf64_Shdr) == try os.read(file.handle, std.mem.asBytes(&shdr)), error.InvalidSectionRead);
        try assert_or_error(shdr.sh_type == elf.SHT_STRTAB, error.InvalidStringTable);
        try assert_or_error(shdr.sh_size > 0, error.InvalidStringTable);
        try assert_or_error(shdr.sh_size < string_max, error.InvalidStringTable);

        var string_buf: [string_max]u8 = undefined;
        try file.seekTo(shdr.sh_offset);
        try assert_or_error(try os.read(file.handle, string_buf[0..shdr.sh_size]) == shdr.sh_size, error.InvalidRead);

        ///////////////

        var pack_offset: ?u32 = null;
        for (0..elf_header.shnum) |index| {
            const offset2 = elf_header.shoff + @sizeOf(@TypeOf(shdr)) * index;

            try file.seekTo(offset2);
            try assert_or_error(@sizeOf(elf.Elf64_Shdr) == try os.read(file.handle, std.mem.asBytes(&shdr)), error.InvalidSectionRead);

            // try assert_or_error(index < string_buf.len, error.);
            const name = std.mem.sliceTo(@as([*:0]const u8, @ptrCast((&string_buf).ptr + shdr.sh_name)), 0);

            if (std.mem.eql(u8, name, ".tigerbeetle.multiversion.pack")) {
                // Our pack must be the second-last section in the file.
                try assert_or_error(pack_offset == null, error.MultipleMultiversionPack);
                try assert_or_error(index == elf_header.shnum - 2, error.InvalidMultiversionPack);
                pack_offset = @intCast(shdr.sh_offset);
            } else if (std.mem.eql(u8, name, ".tigerbeetle.multiversion.metadata")) {
                try assert_or_error(shdr.sh_size == @sizeOf(MultiVersionMetadata), error.InvalidMultiversionMetadata);

                // Our metadata must be the last section in the file.
                try assert_or_error(index == elf_header.shnum - 1, error.InvalidMultiversionMetadata);

                try file.seekTo(shdr.sh_offset);
                var mvm_buf: [@sizeOf(MultiVersionMetadata)]u8 align(@alignOf(MultiVersionMetadata)) = undefined;
                try assert_or_error(@sizeOf(MultiVersionMetadata) == try os.read(file.handle, &mvm_buf), error.InvalidMetadataRead);

                try assert_or_error(pack_offset != null, error.InvalidMultiversionPack);

                var mvm = try MultiVersionMetadata.from_bytes(&mvm_buf);

                return .{
                    .metadata = mvm,
                    .pack_offset = pack_offset.?,
                    .metadata_offset = @intCast(shdr.sh_offset),
                };
            }
        } else {
            return error.NoSectionFound;
        }
    }

    pub fn from_macho(binary_path: []const u8) !MultiVersion {
        _ = binary_path;
        return error.Unimplemented;
    }

    pub fn from_pe(binary_path: []const u8) !MultiVersion {
        _ = binary_path;
        return error.Unimplemented;
    }
};

// FIXME: Actually make this !noreturn, and handle the error with a @panic in the caller.
pub fn exec_release(allocator: std.mem.Allocator, release: Release) noreturn {
    defer @panic("impossible");

    var self_binary_buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    var self_binary_path = std.fs.selfExePath(&self_binary_buf) catch @panic("couldnt find self path");

    std.log.info("exec_release: self_binary_path={s}", .{self_binary_path});

    const multi_version_info = MultiVersion.from_own_binary() catch @panic("couldnt parse metadata");
    const metadata = multi_version_info.metadata;

    const index = blk: {
        for (metadata.past.versions[0..metadata.past.count], 0..) |version, index| {
            if (release.value == version) {
                break :blk index;
            }
        } else {
            @panic("version not found in multiversion metadata");
        }
    };

    const binary_offset = metadata.past.offsets[index];
    const binary_size = metadata.past.sizes[index];
    const binary_checksum = metadata.past.checksums[index];

    const fd = open_memory_file("tigerbeetle-exec-release");

    // Explicit allocation ensures we're not in .static on our allocator yet.
    var buf = allocator.alloc(u8, binary_size) catch @panic("couldnt allocate memory");

    const file = try std.fs.openFileAbsolute(self_binary_path, .{ .mode = .read_only });
    try file.seekTo(multi_version_info.pack_offset + binary_offset);

    assert(binary_size == os.read(file.handle, buf) catch @panic("bad read"));

    const bytes_written = os.write(fd, buf) catch @panic("couldnt write to memfd");
    assert(bytes_written == binary_size);

    const checksum_read = checksum(buf);
    const checksum_expected = binary_checksum;
    const checksum_written = blk: {
        var buf_validate = allocator.alloc(u8, binary_size) catch @panic("couldnt allocate memory");
        os.lseek_SET(fd, 0) catch @panic("bad seek");
        assert(binary_size == os.read(fd, buf_validate) catch @panic("bad read"));
        break :blk checksum(buf_validate);
    };

    std.log.info("checking checksums...", .{});

    assert(checksum_read == checksum_expected);
    assert(checksum_written == checksum_expected);

    // Hacky, just use argc_argv_poitner directly. This is platform specific in any case, and we'll
    // want to have a verification function.
    const argv_buf = allocator.allocSentinel(?[*:0]const u8, os.argv.len, null) catch @panic("couldnt allocate");
    for (os.argv, 0..) |arg, i| {
        argv_buf[i] = arg;
    }

    std.log.info("about to execveat", .{});

    // TODO: Do we want to pass env variables? Probably.
    const env = [_:null]?[*:0]u8{};

    if (execveat(fd, "", argv_buf, env[0..env.len], 0x1000) == 0) {
        unreachable;
    } else {
        @panic("execveat failed");
    }
}

pub fn exec_self() !noreturn {
    var self_binary_buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    var self_binary_path = try std.fs.selfExePath(&self_binary_buf);

    // TODO: Do we want to pass env variables? Probably.
    const env = [_:null]?[*:0]u8{};

    self_binary_buf[self_binary_path.len] = 0;
    var self_binary_path_zero: [*:0]const u8 = self_binary_buf[0..self_binary_path.len :0];

    std.log.info("about to execveZ", .{});
    return std.os.execveZ(self_binary_path_zero, env[0..env.len], env[0..env.len]);

    // Either we returned an error above, or the new process exec'd.
}

fn assert_or_error(condition: bool, err: anyerror) !void {
    if (!condition) return err;
}
