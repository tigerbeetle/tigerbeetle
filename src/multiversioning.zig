const builtin = @import("builtin");
const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;
const os = std.os;
const native_endian = @import("builtin").target.cpu.arch.endian();
const constants = @import("constants.zig");
const IO = @import("io.zig").IO;

const elf = std.elf;

// Re-export to make release code easier.
pub const checksum = @import("vsr/checksum.zig");
pub const multiversion_binary_size_max = constants.multiversion_binary_size_max;

const log_multiversioning = std.log.scoped(.multiversioning);

/// Creates a virtual file backed by memory.
fn open_memory_file(name: [*:0]const u8) os.fd_t {
    const mfd_cloexec = 0x0001;

    return @intCast(os.linux.memfd_create(name, mfd_cloexec));
}

// TODO(zig): Zig 0.11 doesn't have execveat.
// Once that's available, this can be removed.
fn execveat(
    dirfd: i32,
    path: [*:0]const u8,
    argv: [*:null]const ?[*:0]const u8,
    envp: [*:null]const ?[*:0]const u8,
    flags: i32,
) usize {
    return os.linux.syscall5(
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

    pub fn parse(string: []const u8) !Release {
        return Release.from(try ReleaseTriple.parse(string));
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

pub const MultiversionMetadata = extern struct {
    // When slicing into the binary:
    // checksum(section[past_offset..past_offset+past_size]) == past_checksum.
    // This is then validated when the binary is written to a memfd or similar.
    // TODO: Might be nicer as an AoS? It's control plane state.
    pub const PastVersionPack = extern struct {
        /// The maximum number of past releases is one less, because the current release version is
        /// stored outside the PastVersionPack.
        const past_releases_max = constants.vsr_releases_max - 1;

        count: u32 = 0,

        versions: [past_releases_max]u32 = std.mem.zeroes([past_releases_max]u32),
        checksums: [past_releases_max]u128 = std.mem.zeroes([past_releases_max]u128),

        /// Offsets are relative to the start of the pack (`.tbmvp`) offset.
        offsets: [past_releases_max]u32 = std.mem.zeroes([past_releases_max]u32),

        sizes: [past_releases_max]u32 = std.mem.zeroes([past_releases_max]u32),

        /// Normally version upgrades are allowed to skip to the latest. If a corresponding release
        /// is set to true here, it must be visited on the way to the newest version.
        visits: [past_releases_max]bool = std.mem.zeroes([past_releases_max]bool),

        pub fn init(count: u32, past_versions: struct {
            versions: []const u32,
            checksums: []const u128,
            offsets: []const u32,
            sizes: []const u32,
            visits: []const bool,
        }) PastVersionPack {
            assert(past_versions.versions.len == count);
            assert(past_versions.checksums.len == count);
            assert(past_versions.offsets.len == count);
            assert(past_versions.sizes.len == count);
            assert(past_versions.visits.len == count);
            assert(count <= past_releases_max);

            var pack = PastVersionPack{};
            pack.count = count;

            std.mem.copy(u32, &pack.versions, past_versions.versions);
            std.mem.copy(u128, &pack.checksums, past_versions.checksums);
            std.mem.copy(u32, &pack.offsets, past_versions.offsets);
            std.mem.copy(u32, &pack.sizes, past_versions.sizes);
            std.mem.copy(bool, &pack.visits, past_versions.visits);

            return pack;
        }
    };

    /// The current version is executed differently to past versions embedded in the pack, so store
    /// it separately. See exec_latest vs exec_release.
    current_version: u32,

    /// The AEGIS128L checksum of the direct output of `zig build`, for the current_version, before
    /// any objcopy or build magic has been performed.
    /// Used when extracting binaries from past releases to ensure a hash chain - this will be put
    /// in the entry in past.checksums for this release by the build script.
    current_checksum: u128,

    /// The AEGIS128L checksum of the binary, if the `.tbmvm` section were zeroed out. Used to
    /// validate that the binary itself is not corrupt. Putting this in requires a bit of trickery:
    /// - inject a zero `.tbmvm` section of the correct size,
    /// - compute the hash,
    /// - update the section with the correct data.
    /// Used to ensure we don't try and exec into a corrupt binary.
    checksum_without_metadata: u128 = 0,

    past: PastVersionPack = .{},
    reserved: [16384]u8 = std.mem.zeroes([16384]u8),

    /// Covers MultiversionMetadata[0..@sizeOf(MultiversionMetadata) - @sizeOf(u128)].
    checksum_metadata: u128 = undefined,

    /// Parses an instance from a slice of bytes and validates its checksum. Returns a copy.
    pub fn from_bytes(bytes: []const u8) !MultiversionMetadata {
        var self: MultiversionMetadata = undefined;
        stdx.copy_disjoint(.exact, u8, std.mem.asBytes(&self), bytes);

        const checksum_calculated = self.calculate_metadata_checksum();

        if (checksum_calculated != self.checksum_metadata) {
            return error.ChecksumMismatch;
        }

        return self;
    }

    pub fn calculate_metadata_checksum(self: *const MultiversionMetadata) u128 {
        // Checksum must have been set by this point.
        assert(self.checksum_without_metadata != 0);

        const self_without_checksum = std.mem.asBytes(
            self,
        )[0 .. @sizeOf(MultiversionMetadata) - @sizeOf(u128)];

        return checksum.checksum(self_without_checksum);
    }

    /// Given a version, return all the versions:
    /// * Older than the specified from_version,
    /// * Newer than the current from_version, up to and including a newer one with the `visits`
    ///   flag set.
    // FIXME: Add test
    pub fn advertisable(self: *const MultiversionMetadata, from_version: Release) ReleaseList {
        var release_list: ReleaseList = .{};

        for (0..self.past.count) |i| {
            release_list.append_assume_capacity(Release{ .value = self.past.versions[i] });
            if (self.past.versions[i] <= from_version.value) {
                continue;
            } else if (self.past.visits[i]) {
                break;
            }
        }

        // FIXME: Add current_version ONLY if needed
        release_list.append_assume_capacity(Release{ .value = self.current_version });

        assert(release_list.count() > 0);
        return release_list;
    }

    comptime {
        // Changing these will affect the structure stored on disk, which has implications for past
        // clients trying to read!
        assert(constants.vsr_releases_max == 64);
        assert(PastVersionPack.past_releases_max == 63);
        assert(@sizeOf(MultiversionMetadata) == 18288);
        assert(@offsetOf(MultiversionMetadata, "checksum_metadata") == 18272);
    }
};

const memfd_path = "tigerbeetle-multiversion";

pub const Multiversion = struct {
    pub const Callback = *const fn (*Multiversion, anyerror!void) void;
    const ArgsEnvp = struct {
        // Coerces to [*:null]const ?[*:0]const u8 but lets us keep information to free the memory
        // later.
        args: [:null]?[*:0]const u8,
        envp: [*:null]const ?[*:0]const u8,
    };

    io: *IO,

    exe_path: [:0]const u8,
    args_envp: ArgsEnvp,

    source_buffer: []u8,
    source_fd: ?os.fd_t = null,

    target_fd: os.fd_t,
    target_pack_offset: ?usize = null,
    target_metadata: ?MultiversionMetadata = null,
    releases_bundled: ReleaseList = .{},

    completion: IO.Completion = undefined,

    timeout_completion: IO.Completion = undefined,
    timeout_statx: os.linux.Statx = undefined,
    timeout_statx_previous: union(enum) { none, previous: os.linux.Statx, err } = .none,
    timeout_enabled: bool = true,

    stage: union(enum) {
        init,

        source_stat,
        source_open,
        source_read,
        source_swap,

        ready,
        err: anyerror,
    } = .init,

    callback: ?Callback = null,

    pub fn init(allocator: std.mem.Allocator, io: *IO, exe_path: [:0]const u8) !Multiversion {
        assert(builtin.target.os.tag == .linux);
        assert(std.fs.path.isAbsolute(exe_path));

        // To keep the invariant that whatever has been advertised can be executed, while allowing
        // new binaries to be put in place, double buffering is used:
        // * source_buffer is where the in-progress data lives,
        // * target_fd is where the advertised data lives.
        // This does impact memory usage.
        const source_buffer = try allocator.alloc(u8, constants.multiversion_binary_size_max);

        // Only Linux has a nice API for executing from an in-memory file. For macOS and Windows,
        // a standard named temporary file will be used instead.
        const target_fd: os.fd_t = switch (builtin.target.os.tag) {
            .linux => blk: {
                const fd = open_memory_file(memfd_path);
                try os.ftruncate(fd, constants.multiversion_binary_size_max);

                break :blk fd;
            },

            // TODO: macOS / Windows support.
            else => IO.INVALID_FILE,
        };

        // We can pass through our env as-is to exec. We have to manipulate the types
        // here somewhat: they're cast in start.zig and we can't access `argc_argv_ptr`
        // directly. process.zig does the same trick in execve().
        //
        // For args, modify them so that argv[0] is exe_path. This allows our memfd executed binary
        // to find its way back to the real file on disk.
        const args = try allocator.allocSentinel(?[*:0]const u8, os.argv.len, null);
        errdefer allocator.free(args);

        args[0] = try allocator.dupeZ(u8, exe_path);
        errdefer allocator.free(args[0]);

        for (1..os.argv.len) |i| args[i] = os.argv[i];

        const args_envp = .{
            .args = args,
            .envp = @as([*:null]const ?[*:0]const u8, @ptrCast(os.environ.ptr)),
        };

        return .{
            .io = io,

            .exe_path = exe_path,
            .args_envp = args_envp,

            .source_buffer = source_buffer,
            .target_fd = target_fd,
        };
    }

    pub fn start(self: *Multiversion) void {
        self.read_from_binary(null);
        self.tick_until_ready() catch {
            // If there's been an error starting up multiversioning, don't disable it, but
            // advertise only the current version in memory.
            self.releases_bundled.clear();
            self.releases_bundled.append_assume_capacity(constants.config.process.release);
        };

        log_multiversioning.info("enabling automatic on-disk version detection.", .{});
        self.start_timeout();
    }

    pub fn deinit(self: *Multiversion, allocator: std.mem.Allocator) void {
        os.close(self.target_fd);
        self.target_fd = IO.INVALID_FILE;

        allocator.free(self.source_buffer);
        allocator.free(std.mem.span(self.args_envp.args[0].?));
        allocator.free(self.args_envp.args);

        self.timeout_enabled = false;
    }

    pub fn start_timeout(self: *Multiversion) void {
        if (!self.timeout_enabled) return;

        self.io.timeout(
            *Multiversion,
            self,
            on_timeout,
            &self.timeout_completion,
            @as(u63, @intCast(1000 * std.time.ns_per_ms)),
        );
    }

    fn on_timeout(
        self: *Multiversion,
        _: *IO.Completion,
        result: IO.TimeoutError!void,
    ) void {
        assert(self.stage == .init or self.stage == .ready or self.stage == .err);

        _ = result catch unreachable;
        if (!self.timeout_enabled) return;

        self.stage = .source_stat;
        self.io.statx(
            *Multiversion,
            self,
            on_statx,
            &self.completion,
            os.AT.FDCWD,
            self.exe_path,
            0,
            os.linux.STATX_BASIC_STATS,
            &self.timeout_statx,
        );
    }

    fn on_statx(self: *Multiversion, _: *IO.Completion, result: anyerror!void) void {
        _ = result catch |e| {
            self.timeout_statx_previous = .err;
            self.start_timeout();

            return self.handle_error(e);
        };

        // Zero the atime, so we can compare the rest of the struct directly.
        self.timeout_statx.atime = std.mem.zeroes(os.linux.statx_timestamp);

        if (self.timeout_statx_previous == .err or
            (self.timeout_statx_previous == .previous and !stdx.equal_bytes(
            os.linux.Statx,
            &self.timeout_statx_previous.previous,
            &self.timeout_statx,
        ))) {
            log_multiversioning.info("binary {s} - change detected.", .{self.exe_path});

            self.stage = .init;
            self.read_from_binary(on_read_from_binary_statx);
        } else {
            self.stage = .init;
            self.start_timeout();
        }

        self.timeout_statx_previous = .{ .previous = self.timeout_statx };
    }

    fn on_read_from_binary_statx(self: *Multiversion, result: anyerror!void) void {
        self.start_timeout();

        _ = result catch return;
    }

    pub fn read_from_binary(self: *Multiversion, callback: ?Callback) void {
        self.callback = callback;
        self.open_source_binary_file();
    }

    fn open_source_binary_file(self: *Multiversion) void {
        assert(self.stage == .init or self.stage == .ready or self.stage == .err);
        self.stage = .source_open;

        self.io.openat(
            *Multiversion,
            self,
            on_open_source_binary_file,
            &self.completion,
            IO.INVALID_FILE,
            self.exe_path,
            os.O.RDONLY,
            0,
        );
    }

    fn on_open_source_binary_file(
        self: *Multiversion,
        _: *IO.Completion,
        result: IO.OpenatError!os.fd_t,
    ) void {
        assert(self.stage == .source_open);
        const fd = result catch |e| return self.handle_error(e);

        self.stage = .source_read;
        self.source_fd = fd;
        self.io.read(
            *Multiversion,
            self,
            on_read_source_file,
            &self.completion,
            self.source_fd.?,
            self.source_buffer,
            0,
        );
    }

    fn on_read_source_file(
        self: *Multiversion,
        _: *IO.Completion,
        result: IO.ReadError!usize,
    ) void {
        assert(self.stage == .source_read);

        os.close(self.source_fd.?);
        self.source_fd = null;

        const bytes_read = result catch |e| return self.handle_error(e);
        const source_buffer = self.source_buffer[0..bytes_read];

        self.stage = .source_swap;
        self.swap_source_and_target(source_buffer) catch |e| return self.handle_error(e);
    }

    fn swap_source_and_target(self: *Multiversion, source_buffer: []u8) !void {
        assert(self.stage == .source_swap);

        const offsets = switch (builtin.target.os.tag) {
            .linux => try self.parse_elf(),
            else => @panic("multiversion unimplemented"),
        };

        if (offsets.metadata + @sizeOf(MultiversionMetadata) > source_buffer.len) {
            return error.FileTooSmall;
        }

        // `from_bytes` validates the metadata checksum internally.
        const source_buffer_metadata = source_buffer[offsets.metadata..][0..@sizeOf(
            MultiversionMetadata,
        )];
        const metadata = try MultiversionMetadata.from_bytes(source_buffer_metadata);

        // Zero the metadata section in memory, to compute the hash, before copying it back.
        @memset(source_buffer_metadata, 0);
        const source_buffer_checksum = checksum.checksum(source_buffer);
        stdx.copy_disjoint(
            .exact,
            u8,
            source_buffer_metadata,
            std.mem.asBytes(&metadata),
        );

        if (source_buffer_checksum != metadata.checksum_without_metadata)
            return error.ChecksumMismatch;

        // Potentially update the releases_bundled list, if all our checks pass:
        // 1. The release on disk includes the release we're running.
        // 2. The existing releases_bundled, of any versions newer than current, is a subset
        //    of the new advertisable releases.
        var advertisable_includes_running = false;
        var advertisable_is_forward_superset = true; // FIXME: calculate this.
        const advertisable = metadata.advertisable(constants.config.process.release);
        for (advertisable.const_slice()) |release| {
            if (release.value == constants.config.process.release.value) {
                advertisable_includes_running = true;
            }
        }

        if (!advertisable_includes_running) return error.RunningVersionNotIncluded;
        if (!advertisable_is_forward_superset) return error.NotSuperset;

        // Log out the releases bundled; both old and new. Only if this was a change detection run
        // and not from startup.
        if (self.timeout_statx_previous != .none)
            log_multiversioning.info("releases_bundled old: {any}", .{
                self.releases_bundled.inner.buffer[0..self.releases_bundled.inner.len],
            });
        defer if (self.timeout_statx_previous != .none)
            log_multiversioning.info("releases_bundled new: {any}", .{
                self.releases_bundled.inner.buffer[0..self.releases_bundled.inner.len],
            });

        // The below flip needs to happen atomically:
        // * update the releases_bundled to be what's in the source,
        // * update the target_fd to have the same contents as the source.
        //
        // Since target_fd points to a memfd on Linux, this is functionally a memcpy. On other
        // platforms, it's blocking IO - which is acceptable for development.
        self.releases_bundled.clear();
        for (advertisable.const_slice()) |release| {
            self.releases_bundled.append_assume_capacity(release);
        }

        // While these look like blocking IO operations, on a memfd they're memory manipulation.
        // TODO: Would panic'ing be a better option? On Linux, these should never fail. On other
        // platforms where target_fd might be backed by a file, they could...
        errdefer log_multiversioning.err("target binary update failed - " ++
            "this replica might fail to automatically restart!", .{});

        try os.lseek_SET(self.target_fd, 0);
        const bytes_written = try os.write(self.target_fd, source_buffer);
        if (source_buffer.len != bytes_written) return error.ShortWrite;

        self.target_metadata = metadata;
        self.target_pack_offset = offsets.pack;

        self.stage = .ready;

        if (self.callback) |callback| {
            self.callback = null;
            callback(self, {});
        }
    }

    fn parse_elf(self: *Multiversion) !struct { pack: usize, metadata: usize } {
        assert(self.stage == .source_swap);

        var hdr_buf: [@sizeOf(elf.Elf64_Ehdr)]u8 align(@alignOf(elf.Elf64_Ehdr)) = undefined;
        std.mem.copy(u8, &hdr_buf, self.source_buffer[0..@sizeOf(elf.Elf64_Ehdr)]);

        const elf_header = try elf.Header.parse(&hdr_buf);

        // TigerBeetle only supports little endian on 64 bit platforms.
        if (elf_header.endian != .Little) return error.WrongEndian;
        if (!elf_header.is_64) return error.Not64bit;

        // Only support "simple" ELF string tables.
        if (elf_header.shstrndx >= elf.SHN_LORESERVE) return error.LongStringTable;
        if (elf_header.shstrndx == elf.SHN_UNDEF) return error.LongStringTable;

        // First, read the string table section.
        const string_table_shdr_offset = elf_header.shoff +
            @sizeOf(elf.Elf64_Shdr) * elf_header.shstrndx;
        var string_table_shdr: elf.Elf64_Shdr = undefined;
        std.mem.copy(
            u8,
            std.mem.asBytes(&string_table_shdr),
            self.source_buffer[string_table_shdr_offset..][0..@sizeOf(elf.Elf64_Shdr)],
        );

        if (string_table_shdr.sh_type != elf.SHT_STRTAB) return error.InvalidStringTable;
        if (string_table_shdr.sh_size <= 0) return error.InvalidStringTable;
        if (string_table_shdr.sh_size >= self.source_buffer.len) return error.InvalidStringTable;

        const sh_offset = string_table_shdr.sh_offset; // Line length limits.
        const string_table = self.source_buffer[sh_offset..][0..string_table_shdr.sh_size];

        // Next, go through each ELF section to find the ones we're looking for;
        var pack_offset: ?usize = null;
        var metadata_offset: ?u32 = null;
        for (0..elf_header.shnum) |i| {
            var shdr: elf.Elf64_Shdr = undefined;
            const offset = elf_header.shoff + @sizeOf(elf.Elf64_Shdr) * i;
            std.mem.copy(
                u8,
                std.mem.asBytes(&shdr),
                self.source_buffer[offset .. offset + @sizeOf(elf.Elf64_Shdr)],
            );
            const name = std.mem.sliceTo(
                @as([*:0]const u8, @ptrCast((&string_table).ptr + shdr.sh_name)),
                0,
            );

            if (std.mem.eql(u8, name, ".tbmvp")) {
                // Our pack must be the second-last section in the file.
                if (pack_offset != null) return error.MultipleMultiversionPack;
                if (i != elf_header.shnum - 2) return error.InvalidMultiversionPackLocation;

                pack_offset = @intCast(shdr.sh_offset);
            } else if (std.mem.eql(u8, name, ".tbmvm")) {
                // Our metadata must be the last section in the file.
                if (metadata_offset != null) return error.MultipleMultiversionMetadata;
                if (shdr.sh_size != @sizeOf(MultiversionMetadata))
                    return error.InvalidMultiversionMetadataSize;
                if (i != elf_header.shnum - 1) return error.InvalidMultiversionMetadataLocation;

                metadata_offset = @intCast(shdr.sh_offset);
            }
        }

        if (pack_offset == null or metadata_offset == null)
            return error.MultiversionMetadataOrPackNotFound;

        return .{
            .pack = pack_offset.?,
            .metadata = metadata_offset.?,
        };
    }

    fn handle_error(self: *Multiversion, result: anyerror) void {
        log_multiversioning.err("binary does not contain a valid multiversion pack: {}", .{result});

        self.stage = .{ .err = result };

        if (self.callback) |callback| {
            self.callback = null;
            callback(self, result);
        }
    }

    pub fn tick_until_ready(self: *Multiversion) !void {
        // Either set a callback, or use this psuedo-sync interface.
        assert(self.callback == null);

        while (self.stage != .ready and self.stage != .err) {
            try self.io.tick();
        }

        if (self.stage == .err) {
            return self.stage.err;
        }
    }

    pub fn exec_latest(self: *Multiversion) !noreturn {
        log_multiversioning.info("re-executing {s}...\n", .{self.exe_path});

        if (execveat(
            self.target_fd,
            "",
            self.args_envp.args,
            self.args_envp.envp,
            os.AT.EMPTY_PATH,
        ) == 0) {
            unreachable;
        } else {
            return error.ExecveatFailed;
        }
    }

    /// exec_release is called before a replica is fully open, and before it has transitioned to not
    /// allocating. Therefore, standard `os.read` blocking syscalls are available.
    /// (in any case, using blocking IO on a memfd on Linux is safe.)
    pub fn exec_release(self: *Multiversion, release: Release) !noreturn {
        const metadata = &self.target_metadata.?;

        const index = blk: {
            for (metadata.past.versions[0..metadata.past.count], 0..) |version, index| {
                if (release.value == version) {
                    break :blk index;
                }
            } else {
                // This should never happen: it means that somehow ...
                return error.VersionNotFound;
            }
        };

        const binary_offset = metadata.past.offsets[index];
        const binary_size = metadata.past.sizes[index];
        const binary_checksum = metadata.past.checksums[index];

        // Our target release is physically embedded in the binary. Shuffle the bytes
        // around, so that it's at the start, then truncate the descriptor so there's nothing
        // trailing.
        try os.lseek_SET(self.target_fd, self.target_pack_offset.? + binary_offset);
        const bytes_read = try os.read(self.target_fd, self.source_buffer[0..binary_size]);
        assert(bytes_read == binary_size);

        try os.lseek_SET(self.target_fd, 0);
        const bytes_written = try os.write(self.target_fd, self.source_buffer[0..bytes_read]);
        assert(bytes_read == bytes_written);

        // Zero the remaining bytes in the file.
        try os.ftruncate(self.target_fd, binary_size);

        // Ensure the checksum matches the metadata. This could have been done above, but
        // do it in a separate step to make sure.
        const checksum_written = blk: {
            try os.lseek_SET(self.target_fd, 0);
            const bytes_read_checksum = try os.read(
                self.target_fd,
                self.source_buffer[0..binary_size],
            );
            assert(bytes_read_checksum == binary_size);
            break :blk checksum.checksum(self.source_buffer[0..binary_size]);
        };
        assert(checksum_written == binary_checksum);

        switch (builtin.target.os.tag) {
            .linux => {
                log_multiversioning.info("executing release {}...\n", .{release});

                if (execveat(
                    self.target_fd,
                    "",
                    self.args_envp.args,
                    self.args_envp.envp,
                    os.AT.EMPTY_PATH,
                ) == 0) {
                    unreachable;
                } else {
                    return error.ExecveatFailed;
                }
            },
            else => @panic("exec_release unimplemented"),
        }

        unreachable;
    }
};

pub fn self_exe_path(allocator: std.mem.Allocator) ![:0]const u8 {
    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const native_self_exe_path = try std.fs.selfExePath(&buf);

    if (builtin.target.os.tag == .linux and std.mem.eql(
        u8,
        native_self_exe_path,
        "/memfd:" ++ memfd_path ++ " (deleted)",
    )) {
        // Technically, "/memfd:tigerbeetle-multiversion (deleted)" is a valid path at which you
        // could place your binary - please don't!
        assert(std.fs.cwd().statFile(native_self_exe_path) catch null == null);

        // Running from a memfd already; the real path is argv[0].
        const path = std.mem.span(os.argv[0]);
        assert(std.fs.path.isAbsolute(path));

        return path;
    } else {
        // Not running from a memfd. `native_self_exe_path` is the real path.
        return try allocator.dupeZ(u8, native_self_exe_path);
    }
}
