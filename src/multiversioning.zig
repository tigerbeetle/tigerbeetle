const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;
const os = std.os;
const linux = os.linux;
const native_endian = @import("builtin").target.cpu.arch.endian();
const constants = @import("constants.zig");
const IO = @import("io.zig").IO;

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

    /// Parses an instance from a slice of bytes - returns a copy.
    pub fn from_bytes(bytes: []const u8) !MultiVersionMetadata {
        var self: MultiVersionMetadata = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&self), bytes);

        const checksum_calculated = self.calculate_metadata_checksum();

        if (checksum_calculated != self.checksum_metadata) {
            return error.ChecksumMismatch;
        }

        return self;
    }

    pub fn calculate_metadata_checksum(self: *const MultiVersionMetadata) u128 {
        // Checksum must have been set by this point.
        assert(self.checksum_without_metadata != 0);

        const self_without_checksum = std.mem.asBytes(self)[0 .. @sizeOf(MultiVersionMetadata) - @sizeOf(u128)];

        return checksum(self_without_checksum);
    }

    comptime {
        // Changing this will affect the structure stored on disk, and has implications for past clients trying to read!
        assert(constants.vsr_releases_max == 64);
    }
};

pub const MultiVersion = struct {
    pub const Callback = *const fn (*MultiVersion, anyerror!void) void;
    pub const MultiVersionError = IO.ReadError || error{ FileOpenError, ShortRead, InvalidELFHeader, WrongEndian, Not64bit, LongStringTable, InvalidStringTable };

    read_buffer: []u8,
    exe_path: []const u8,
    elf_string_buffer: []u8,
    elf_header: elf.Header = undefined,
    io: *IO,

    completion: IO.Completion = undefined,
    completion_timeout: IO.Completion = undefined,
    file: ?std.fs.File = null,

    pack_offset: ?u32 = null,
    metadata: MultiVersionMetadata = undefined,
    releases_bundled: ReleaseList = .{},

    /// Used for validation, to know what to zero.
    metadata_offset: u32 = undefined,

    stage: union(enum) {
        init,
        read_elf_header,
        read_elf_string_table_section,
        read_elf_string_table,
        read_elf_section: u64,
        read_multiversion_metadata,
        ready,
        err: anyerror,
    } = .init,

    callback: ?Callback = null,

    pub fn init(allocator: std.mem.Allocator, io: *IO, exe_path: []const u8) !MultiVersion {
        const read_buffer = try allocator.alloc(u8, 2048);
        const elf_string_buffer = try allocator.alloc(u8, 1024);

        // // openSelfExe is a thing, but don't use it for now because we might want our event loop to handle opening
        // // in future. (can open() block?)
        // var exe_path = try std.fs.selfExePathAlloc(allocator);

        return MultiVersion{
            .read_buffer = read_buffer,
            .elf_string_buffer = elf_string_buffer,
            .exe_path = exe_path,

            .io = io,
        };
    }

    fn on_timeout(
        self: *MultiVersion,
        completion: *IO.Completion,
        result: IO.TimeoutError!void,
    ) void {
        _ = result catch unreachable;
        _ = completion;

        self.read_from_elf(on_timeout_read_from_elf_callback);
    }

    pub fn on_timeout_read_from_elf_callback(self: *MultiVersion, result: anyerror!void) void {
        std.log.info("hello from here: r={!}", .{result});
        _ = result catch void;

        self.io.timeout(
            *MultiVersion,
            self,
            on_timeout,
            &self.completion_timeout,
            @as(u63, @intCast(1000 * std.time.ns_per_ms)),
        );
    }

    pub fn deinit(self: *MultiVersion, allocator: std.mem.Allocator) void {
        allocator.free(self.read_buffer);
        allocator.free(self.elf_string_buffer);
    }

    pub fn reset(self: *MultiVersion) void {
        self.* = .{
            .read_buffer = self.read_buffer,
            .elf_string_buffer = self.elf_string_buffer,
            .exe_path = self.exe_path,
            .io = self.io,
        };
    }

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

    /// The below methods can all be called while a Replica is running normally. They need to
    /// follow all standard conventions - no dynamic memory allocation (not that it would be
    /// allowed!), no blocking syscalls, and use our event loop.
    pub fn read_from_elf(self: *MultiVersion, callback: ?Callback) void {
        self.callback = callback;

        // Can opening a file block?
        self.file = std.fs.openFileAbsolute(self.exe_path, .{ .mode = .read_only }) catch return self.handle_error(error.FileOpenError);

        self.read_from_elf_header();
    }

    fn read_from_elf_header(self: *MultiVersion) void {
        self.stage = .read_elf_header;
        self.io.read(
            *MultiVersion,
            self,
            on_read_from_elf_header,
            &self.completion,
            self.file.?.handle,
            self.read_buffer[0..@sizeOf(elf.Elf64_Ehdr)],
            0,
        );
    }

    fn on_read_from_elf_header(self: *MultiVersion, completion: *IO.Completion, result: IO.ReadError!usize) void {
        _ = completion;
        const read_bytes = result catch |e| return self.handle_error(e);

        assert(self.stage == .read_elf_header);
        if (read_bytes != @sizeOf(elf.Elf64_Ehdr)) return self.handle_error(error.ShortRead);

        // Alignment trickery.
        var hdr_buf: [@sizeOf(elf.Elf64_Ehdr)]u8 align(@alignOf(elf.Elf64_Ehdr)) = undefined;
        std.mem.copy(u8, &hdr_buf, self.read_buffer[0..read_bytes]);

        const elf_header = elf.Header.parse(&hdr_buf) catch return self.handle_error(error.InvalidELFHeader);

        // TigerBeetle only supports little endian on 64 bit platforms.
        if (elf_header.endian != .Little) return self.handle_error(error.WrongEndian);
        if (!elf_header.is_64) return self.handle_error(error.Not64bit);

        // Only support "simple" ELF string tables.
        if (elf_header.shstrndx >= elf.SHN_LORESERVE) return self.handle_error(error.LongStringTable);
        if (elf_header.shstrndx == elf.SHN_UNDEF) return self.handle_error(error.LongStringTable);

        // First, read the string table section.
        self.stage = .read_elf_string_table_section;
        const offset = elf_header.shoff + @sizeOf(elf.Elf64_Shdr) * elf_header.shstrndx;
        self.elf_header = elf_header;
        self.io.read(
            *MultiVersion,
            self,
            on_read_elf_string_table_section,
            &self.completion,
            self.file.?.handle,
            self.read_buffer[0..@sizeOf(elf.Elf64_Shdr)],
            offset,
        );
    }

    fn on_read_elf_string_table_section(self: *MultiVersion, completion: *IO.Completion, result: IO.ReadError!usize) void {
        _ = completion;
        const read_bytes = result catch |e| return self.handle_error(e);

        assert(self.stage == .read_elf_string_table_section);
        if (read_bytes != @sizeOf(elf.Elf64_Shdr)) return self.handle_error(error.ShortRead);

        var shdr: elf.Elf64_Shdr = undefined;
        std.mem.copy(u8, std.mem.asBytes(&shdr), self.read_buffer[0..read_bytes]);

        if (shdr.sh_type != elf.SHT_STRTAB) return self.handle_error(error.InvalidStringTable);
        if (shdr.sh_size <= 0) return self.handle_error(error.InvalidStringTable);
        if (shdr.sh_size >= self.read_buffer.len) return self.handle_error(error.InvalidStringTable);

        self.stage = .read_elf_string_table;

        const offset = shdr.sh_offset;
        self.io.read(
            *MultiVersion,
            self,
            on_read_elf_string_table,
            &self.completion,
            self.file.?.handle,
            self.read_buffer[0..shdr.sh_size],
            offset,
        );
    }

    fn on_read_elf_string_table(self: *MultiVersion, completion: *IO.Completion, result: IO.ReadError!usize) void {
        _ = completion;
        const read_bytes = result catch |e| return self.handle_error(e);

        assert(self.stage == .read_elf_string_table);
        assert(read_bytes < self.elf_string_buffer.len);
        // FIXME:
        // try assert_or_error(try os.read(file.handle, string_buf[0..shdr.sh_size]) == shdr.sh_size, error.InvalidRead);

        std.mem.copy(u8, self.elf_string_buffer, self.read_buffer[0..read_bytes]);

        // Kick off reading our ELF sections
        self.stage = .{ .read_elf_section = 0 };
        self.read_elf_section();
    }

    fn read_elf_section(self: *MultiVersion) void {
        assert(self.stage == .read_elf_section);

        const offset = self.elf_header.shoff + @sizeOf(elf.Elf64_Shdr) * self.stage.read_elf_section;
        self.io.read(
            *MultiVersion,
            self,
            on_read_elf_section,
            &self.completion,
            self.file.?.handle,
            self.read_buffer[0..@sizeOf(elf.Elf64_Shdr)],
            offset,
        );
    }

    fn on_read_elf_section(self: *MultiVersion, completion: *IO.Completion, result: IO.ReadError!usize) void {
        _ = completion;
        const read_bytes = result catch |e| return self.handle_error(e);

        assert(self.stage == .read_elf_section);
        assert(read_bytes == @sizeOf(elf.Elf64_Shdr));

        var shdr: elf.Elf64_Shdr = undefined;
        std.mem.copy(u8, std.mem.asBytes(&shdr), self.read_buffer[0..read_bytes]);
        // try assert_or_error(index < string_buf.len, error.);
        const name = std.mem.sliceTo(@as([*:0]const u8, @ptrCast((&self.elf_string_buffer).ptr + shdr.sh_name)), 0);

        if (std.mem.eql(u8, name, ".tigerbeetle.multiversion.pack")) {
            // Our pack must be the second-last section in the file.
            // FIXME: WTF
            // if (self.pack_offset != null) return self.handle_error(error.MultipleMultiversionPack);
            // if (self.stage.read_elf_section != self.elf_header.shnum - 2) return self.handle_error(error.InvalidMultiversionPack);

            self.pack_offset = @intCast(shdr.sh_offset);
        } else if (std.mem.eql(u8, name, ".tigerbeetle.multiversion.metadata")) {
            // Our metadata must be the last section in the file.
            if (shdr.sh_size != @sizeOf(MultiVersionMetadata)) return self.handle_error(error.InvalidMultiversionMetadata);
            if (self.stage.read_elf_section != self.elf_header.shnum - 1) return self.handle_error(error.InvalidMultiversionMetadata);

            self.stage = .read_multiversion_metadata;

            const offset = shdr.sh_offset;
            self.metadata_offset = @intCast(offset);
            self.io.read(
                *MultiVersion,
                self,
                on_read_multiversion_metadata,
                &self.completion,
                self.file.?.handle,
                self.read_buffer[0..@sizeOf(MultiVersionMetadata)],
                offset,
            );
            return;
        }

        self.stage.read_elf_section += 1;
        if (self.stage.read_elf_section < self.elf_header.shnum) {
            self.read_elf_section();
        } else {
            return self.handle_error(error.NoMetadataSections);
        }
    }

    fn on_read_multiversion_metadata(self: *MultiVersion, completion: *IO.Completion, result: MultiVersionError!usize) void {
        const read_bytes = result catch |e| return self.handle_error(e);

        _ = completion;
        assert(self.stage == .read_multiversion_metadata);
        assert(read_bytes == @sizeOf(MultiVersionMetadata)); // FIXME: Shouldn't be an assertion failure.

        // try assert_or_error(@sizeOf(MultiVersionMetadata) == try os.read(file.handle, &mvm_buf), error.InvalidMetadataRead);

        // try assert_or_error(pack_offset != null, error.InvalidMultiversionPack);

        // assert(self.pack_offset != null);
        self.metadata = MultiVersionMetadata.from_bytes(self.read_buffer[0..read_bytes]) catch |e| return self.handle_error(e);

        // Update the releases_bundled list.
        self.releases_bundled.clear();
        for (self.metadata.past.versions[0..self.metadata.past.count]) |version| {
            self.releases_bundled.append_assume_capacity(Release{ .value = version });
        }

        self.releases_bundled.append_assume_capacity(Release{ .value = self.metadata.current_version });
        self.stage = .ready;
        self.file.?.close();

        if (self.callback) |callback| {
            callback(self, {});
        }
    }

    fn handle_error(self: *MultiVersion, result: anyerror) void {
        if (self.file) |*file| {
            file.close();
        }
        std.log.err("binary does not contain a valid multiversion pack: {}", .{result});

        self.releases_bundled.clear();
        // self.releases_bundled.append_assume_capacity(config.process.release);

        self.stage = .{ .err = result };

        if (self.callback) |callback| {
            callback(self, result);
        }
    }

    pub fn tick_until_ready(self: *MultiVersion) !void {
        // Either set a callback, or use this psuedo-sync interface.
        assert(self.callback == null);

        while (self.stage != .ready and self.stage != .err) {
            try self.io.tick();
        }

        if (self.stage == .err) {
            return self.stage.err;
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

/// exec_release is called before a replica is fully open, and before we've transitioned to not
/// allocating. Therefore, we can use standard `os.read` blocking syscalls.
pub fn exec_release(allocator: std.mem.Allocator, io: *IO, release: Release) !noreturn {
    var self_exe_path = try std.fs.selfExePathAlloc(allocator);
    defer allocator.free(self_exe_path);

    var multiversion = try MultiVersion.init(allocator, io, self_exe_path);
    multiversion.read_from_elf(null);
    try multiversion.tick_until_ready();

    const metadata = multiversion.metadata;

    const index = blk: {
        for (metadata.past.versions[0..metadata.past.count], 0..) |version, index| {
            if (release.value == version) {
                break :blk index;
            }
        } else {
            return error.VersionNotFound;
        }
    };

    const binary_offset = metadata.past.offsets[index];
    const binary_size = metadata.past.sizes[index];
    const binary_checksum = metadata.past.checksums[index];

    const fd = open_memory_file("tigerbeetle-exec-release");

    // Explicit allocation ensures we're not in .static on our allocator yet.
    var buf = try allocator.alloc(u8, binary_size);

    const file = try std.fs.openFileAbsolute(multiversion.exe_path, .{ .mode = .read_only });
    try file.seekTo(multiversion.pack_offset.? + binary_offset);

    const bytes_read = try os.read(file.handle, buf);
    assert(bytes_read == binary_size);

    // We could use std.fs.copy_file here, but it's not public, and probably not worth the effort
    // to vendor.
    const bytes_written = try os.write(fd, buf);
    assert(bytes_written == binary_size);

    const checksum_read = checksum(buf);
    const checksum_expected = binary_checksum;
    const checksum_written = blk: {
        var buf_validate = try allocator.alloc(u8, binary_size);
        try os.lseek_SET(fd, 0);
        const bytes_read_checksum = try os.read(fd, buf_validate);
        assert(bytes_read_checksum == binary_size);
        break :blk checksum(buf_validate);
    };

    assert(checksum_read == checksum_expected);
    assert(checksum_written == checksum_expected);

    // Hacky, just use argc_argv_poitner directly. This is platform specific in any case, and we'll
    // want to have a verification function.
    const argv_buf = try allocator.allocSentinel(?[*:0]const u8, os.argv.len, null);
    for (os.argv, 0..) |arg, i| {
        argv_buf[i] = arg;
    }

    // TODO: Do we want to pass env variables? Probably.
    const env = [_:null]?[*:0]u8{};

    std.log.info("Executing release {}...\n", .{release});
    if (execveat(fd, "", argv_buf, env[0..env.len], 0x1000) == 0) {
        unreachable;
    } else {
        return error.ExecveatFailed;
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
