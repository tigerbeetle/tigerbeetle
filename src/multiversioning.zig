const builtin = @import("builtin");
const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;
const os = std.os;
const posix = std.posix;
const native_endian = @import("builtin").target.cpu.arch.endian();
const constants = @import("constants.zig");
const IO = @import("io.zig").IO;

const elf = std.elf;

// Re-export to make release code easier.
pub const checksum = @import("vsr/checksum.zig");
pub const multiversion_binary_size_max = constants.multiversion_binary_size_max;

const log = std.log.scoped(.multiversioning);

/// Creates a virtual file backed by memory.
fn open_memory_file(name: [*:0]const u8) posix.fd_t {
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

/// A ReleaseList is ordered from lowest-to-highest.
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

    pub fn less_than(_: void, a: Release, b: Release) bool {
        return switch (std.math.order(a.value, b.value)) {
            .lt => true,
            .eq => false,
            .gt => false,
        };
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

pub const MultiversionHeader = extern struct {
    const Flags = packed struct {
        /// Normally release upgrades are allowed to skip to the latest. If a corresponding release
        /// is set to true here, it must be visited on the way to the newest release.
        visit: bool,

        /// If this binary has debug info attached.
        debug: bool,

        padding: u6 = 0,

        comptime {
            assert(@sizeOf(Flags) == 1);
            assert(@bitSizeOf(Flags) == @sizeOf(Flags) * 8);
        }
    };

    // When slicing into the binary:
    // checksum(section[past_offset..past_offset+past_size]) == past_checksum.
    // This is then validated when the binary is written to a memfd or similar.
    // TODO: Might be nicer as an AoS? It's control plane state.
    pub const PastReleases = extern struct {
        /// The maximum number of past releases is one less, because the current release is
        /// stored outside PastReleases.
        const past_releases_max = constants.vsr_releases_max - 1;

        count: u32 = 0,

        releases: [past_releases_max]u32 = std.mem.zeroes([past_releases_max]u32),
        checksums: [past_releases_max]u128 = std.mem.zeroes([past_releases_max]u128),

        /// Offsets are relative to the start of the body (`.tb_mvb`) offset.
        offsets: [past_releases_max]u32 = std.mem.zeroes([past_releases_max]u32),

        sizes: [past_releases_max]u32 = std.mem.zeroes([past_releases_max]u32),

        flags: [past_releases_max]Flags = std.mem.zeroes([past_releases_max]Flags),
        flags_padding: [1]u8 = std.mem.zeroes([1]u8),

        // Extra metadata. Not used by any current upgrade processes directly, but useful to know:
        git_commits: [past_releases_max][20]u8 = std.mem.zeroes([past_releases_max][20]u8),
        release_client_mins: [past_releases_max]u32 = std.mem.zeroes([past_releases_max]u32),

        pub fn init(count: u32, past_init: struct {
            releases: []const u32,
            checksums: []const u128,
            offsets: []const u32,
            sizes: []const u32,
            flags: []const Flags,
            git_commits: []const [20]u8,
            release_client_mins: []const u32,
        }) PastReleases {
            assert(past_init.releases.len == count);
            assert(past_init.checksums.len == count);
            assert(past_init.offsets.len == count);
            assert(past_init.sizes.len == count);
            assert(past_init.flags.len == count);
            assert(past_init.git_commits.len == count);
            assert(past_init.release_client_mins.len == count);

            var past_releases = PastReleases{};
            past_releases.count = count;

            stdx.copy_disjoint(.inexact, u32, &past_releases.releases, past_init.releases);
            stdx.copy_disjoint(.inexact, u128, &past_releases.checksums, past_init.checksums);
            stdx.copy_disjoint(.inexact, u32, &past_releases.offsets, past_init.offsets);
            stdx.copy_disjoint(.inexact, u32, &past_releases.sizes, past_init.sizes);
            stdx.copy_disjoint(.inexact, Flags, &past_releases.flags, past_init.flags);
            stdx.copy_disjoint(.inexact, [20]u8, &past_releases.git_commits, past_init.git_commits);
            stdx.copy_disjoint(
                .inexact,
                u32,
                &past_releases.release_client_mins,
                past_init.release_client_mins,
            );

            @memset(std.mem.sliceAsBytes(past_releases.releases[count..]), 0);
            @memset(std.mem.sliceAsBytes(past_releases.checksums[count..]), 0);
            @memset(std.mem.sliceAsBytes(past_releases.offsets[count..]), 0);
            @memset(std.mem.sliceAsBytes(past_releases.sizes[count..]), 0);
            @memset(std.mem.sliceAsBytes(past_releases.flags[count..]), 0);
            @memset(std.mem.sliceAsBytes(past_releases.git_commits[count..]), 0);
            @memset(std.mem.sliceAsBytes(past_releases.release_client_mins[count..]), 0);

            past_releases.verify() catch @panic("invalid past_release");

            return past_releases;
        }

        pub fn verify(self: *const PastReleases) !void {
            if (self.count > past_releases_max) return error.InvalidPastReleases;
            if (self.count == 0) return error.InvalidPastReleases;

            if (!stdx.zeroed(std.mem.sliceAsBytes(self.releases[self.count..])) or
                !stdx.zeroed(std.mem.sliceAsBytes(self.checksums[self.count..])) or
                !stdx.zeroed(std.mem.sliceAsBytes(self.offsets[self.count..])) or
                !stdx.zeroed(std.mem.sliceAsBytes(self.sizes[self.count..])) or
                !stdx.zeroed(std.mem.sliceAsBytes(self.flags[self.count..])) or
                !stdx.zeroed(&self.flags_padding) or
                !stdx.zeroed(std.mem.sliceAsBytes(self.git_commits[self.count..])) or
                !stdx.zeroed(std.mem.sliceAsBytes(self.release_client_mins[self.count..])))
            {
                return error.InvalidPastReleases;
            }

            const releases = self.releases[0..self.count];
            const offsets = self.offsets[0..self.count];
            const sizes = self.sizes[0..self.count];
            const flags = self.flags[0..self.count];
            const git_commits = self.git_commits[0..self.count];
            const release_client_mins = self.release_client_mins[0..self.count];

            for (releases) |v| if (v == 0) return error.InvalidPastReleases;
            if (!std.sort.isSorted(u32, releases, {}, std.sort.asc(u32))) {
                return error.InvalidPastReleases;
            }

            if (offsets[0] != 0) return error.InvalidPastReleases;
            for (offsets[1..], 1..) |offset, i| {
                const calculated_offset = blk: {
                    var calculated_offset: u32 = 0;
                    for (sizes[0..i]) |size| {
                        calculated_offset += size;
                    }
                    break :blk calculated_offset;
                };
                if (offset == 0) return error.InvalidPastReleases;
                if (offset != calculated_offset) return error.InvalidPastReleases;
            }

            for (sizes) |s| if (s == 0) return error.InvalidPastReleases;
            for (flags) |f| if (f.padding != 0) return error.InvalidPastReleases;
            for (git_commits) |g| if (stdx.zeroed(&g)) return error.InvalidPastReleases;

            for (release_client_mins) |v| if (v == 0) return error.InvalidPastReleases;
            if (!std.sort.isSorted(u32, release_client_mins, {}, std.sort.asc(u32))) {
                return error.InvalidPastReleases;
            }
        }
    };

    /// Covers MultiversionHeader[@sizeOf(u128)..].
    checksum_header: u128 = undefined,

    /// The AEGIS128L checksum of the binary, if the header (`.tb_mvh`) section were zeroed out.
    /// Used to validate that the binary itself is not corrupt. Putting this in requires a bit
    /// of trickery:
    /// * inject a zero `.tb_mvh` section of the correct size,
    /// * compute the hash,
    /// * update the section with the correct data.
    /// Used to ensure we don't try and exec into a corrupt binary.
    checksum_binary_without_header: u128 = 0,

    /// The AEGIS128L checksum of the direct output of `zig build`, for the current_release, before
    /// any objcopy or build magic has been performed.
    /// Used when extracting the latest binary from a now-past release during the build process.
    /// Instead of having to rebuild from source, objcopy is used to remove the multiversion
    /// sections, which is then compared to this checksum to ensure the output is identical.
    current_checksum: u128,

    /// Track the schema of the header. It's possible to completely change the schema - past this
    /// point - while maintaining an upgrade path by having a transitional release:
    /// * 0.15.4 uses schema version 1,
    /// * 0.15.5 uses schema version 1/2,
    /// * 0.15.6 uses schema version 2.
    ///
    /// Then, it's possible to have 2 multiversion releases, one with {0.15.4, 0.15.5} and one with
    /// {0.15.5, 0.15.6}, that allow an upgrade path with 2 steps.
    schema_version: u32 = 1,
    vsr_releases_max: u32 = constants.vsr_releases_max,

    /// The current release is executed differently to past releases embedded in the body, so store
    /// it separately. See exec_latest vs exec_release.
    current_release: u32,

    current_flags: Flags,
    current_flags_padding: [3]u8 = std.mem.zeroes([3]u8),

    past: PastReleases = .{},
    past_padding: [16]u8 = std.mem.zeroes([16]u8),

    /// Reserved space for future use. This is special: unlike the rest of the *_padding fields,
    /// which are required to be zeroed, this is not. This allows adding whole new fields in a
    /// backwards compatible way, while preventing the temptation of changing the meaning of
    /// existing fields without bumping the schema version entirely.
    reserved: [4768]u8 = std.mem.zeroes([4768]u8),

    /// Parses an instance from a slice of bytes and validates its checksum. Returns a copy.
    pub fn init_from_bytes(bytes: *const [@sizeOf(MultiversionHeader)]u8) !MultiversionHeader {
        const self = std.mem.bytesAsValue(MultiversionHeader, bytes).*;
        try self.verify();

        return self;
    }

    fn verify(self: *const MultiversionHeader) !void {
        const checksum_calculated = self.calculate_header_checksum();

        if (checksum_calculated != self.checksum_header) return error.ChecksumMismatch;
        if (self.schema_version != 1) return error.InvalidSchemaVersion;
        if (self.vsr_releases_max != constants.vsr_releases_max) return error.InvalidVSRReleaseMax;
        if (self.current_flags.padding != 0) return error.InvalidCurrentFlags;
        if (!stdx.zeroed(&self.current_flags_padding)) return error.InvalidCurrentFlags;
        if (!self.current_flags.visit) return error.InvalidCurrentFlags;
        if (self.current_release == 0) return error.InvalidCurrentRelease;

        stdx.maybe(stdx.zeroed(&self.reserved));

        try self.past.verify();
        if (!stdx.zeroed(&self.past_padding)) return error.InvalidPastPadding;

        const past_release_newest = self.past.releases[self.past.count - 1];
        if (past_release_newest >= self.current_release) return error.PastReleaseNewerThanCurrent;
    }

    pub fn calculate_header_checksum(self: *const MultiversionHeader) u128 {
        // The checksum for the rest of the file must have been set by this point.
        assert(self.checksum_binary_without_header != 0);

        comptime assert(std.meta.fieldIndex(MultiversionHeader, "checksum_header") == 0);

        const checksum_size = @sizeOf(@TypeOf(self.checksum_header));
        comptime assert(checksum_size == @sizeOf(u128));

        return checksum.checksum(std.mem.asBytes(self)[@sizeOf(u128)..]);
    }

    /// Given a release, return all the releases:
    /// * Older than the specified from_release,
    /// * Newer than the current from_release, up to and including a newer one with the `visits`
    ///   flag set.
    pub fn advertisable(self: *const MultiversionHeader, from_release: Release) ReleaseList {
        var release_list: ReleaseList = .{};

        for (0..self.past.count) |i| {
            release_list.append_assume_capacity(Release{ .value = self.past.releases[i] });
            if (from_release.value < self.past.releases[i]) {
                if (self.past.flags[i].visit) {
                    break;
                }
            }
        } else {
            release_list.append_assume_capacity(Release{ .value = self.current_release });
        }

        // These asserts should be impossible to reach barring a bug; they're checked in verify()
        // so there shouldn't be a way for a corrupt / malformed binary to get this far.
        assert(release_list.count() > 0);
        assert(std.sort.isSorted(
            Release,
            release_list.const_slice(),
            {},
            Release.less_than,
        ));
        for (
            release_list.const_slice()[0 .. release_list.count() - 1],
            release_list.const_slice()[1..],
        ) |release, release_next| assert(release.value != release_next.value);

        return release_list;
    }

    comptime {
        // Changing these will affect the structure stored on disk, which has implications for past
        // clients trying to read!
        assert(constants.vsr_releases_max == 64);
        assert(PastReleases.past_releases_max == 63);
        assert(@sizeOf(MultiversionHeader) == 8192);
        assert(@offsetOf(MultiversionHeader, "checksum_header") == 0);
        assert(@offsetOf(MultiversionHeader, "schema_version") == 48);
        assert(stdx.no_padding(PastReleases));
        assert(stdx.no_padding(MultiversionHeader));
    }
};

test "MultiversionHeader.advertisable" {
    const ListU32 = stdx.BoundedArray(u32, constants.vsr_releases_max);
    const ListU128 = stdx.BoundedArray(u128, constants.vsr_releases_max);
    const ListGitCommit = stdx.BoundedArray([20]u8, constants.vsr_releases_max);

    const tests = [_]struct {
        releases: []const u32,
        flags: []const MultiversionHeader.Flags,
        current: u32,
        from: u32,
        expected: []const u32,
    }{
        .{ .releases = &.{ 1, 2, 3 }, .flags = &.{
            .{ .visit = false, .debug = false },
            .{ .visit = false, .debug = false },
            .{ .visit = false, .debug = false },
        }, .current = 4, .from = 2, .expected = &.{ 1, 2, 3, 4 } },
        .{ .releases = &.{ 1, 2, 3 }, .flags = &.{
            .{ .visit = false, .debug = false },
            .{ .visit = false, .debug = false },
            .{ .visit = true, .debug = false },
        }, .current = 4, .from = 2, .expected = &.{ 1, 2, 3 } },
        .{ .releases = &.{ 1, 2, 3, 4 }, .flags = &.{
            .{ .visit = false, .debug = false },
            .{ .visit = false, .debug = false },
            .{ .visit = true, .debug = false },
            .{ .visit = false, .debug = false },
        }, .current = 5, .from = 2, .expected = &.{ 1, 2, 3 } },
        .{ .releases = &.{ 1, 2, 3, 4 }, .flags = &.{
            .{ .visit = true, .debug = false },
            .{ .visit = false, .debug = false },
            .{ .visit = true, .debug = false },
            .{ .visit = true, .debug = false },
        }, .current = 5, .from = 5, .expected = &.{ 1, 2, 3, 4, 5 } },
    };
    for (tests) |t| {
        var checksums: ListU128 = .{};
        var offsets: ListU32 = .{};
        var sizes: ListU32 = .{};
        var git_commits: ListGitCommit = .{};

        for (t.releases) |_| {
            checksums.append_assume_capacity(0);
            offsets.append_assume_capacity(@intCast(offsets.count()));
            sizes.append_assume_capacity(1);
            git_commits.append_assume_capacity("00000000000000000000".*);
        }

        const past_releases = MultiversionHeader.PastReleases.init(@intCast(t.releases.len), .{
            .releases = t.releases,
            .checksums = checksums.const_slice(),
            .offsets = offsets.const_slice(),
            .sizes = sizes.const_slice(),
            .flags = t.flags,
            .release_client_mins = t.releases,
            .git_commits = git_commits.const_slice(),
        });

        var header = MultiversionHeader{
            .past = past_releases,
            .current_release = t.current,
            .current_checksum = 0,
            .current_flags = .{ .visit = true, .debug = false },
            .checksum_binary_without_header = 1,
        };
        header.checksum_header = header.calculate_header_checksum();

        try header.verify();

        const advertisable = header.advertisable(Release{ .value = t.from });
        var expected: ReleaseList = .{};
        for (t.expected) |release| {
            expected.append_assume_capacity(Release{ .value = release });
        }
        try std.testing.expectEqualSlices(
            Release,
            expected.const_slice(),
            advertisable.const_slice(),
        );
    }
}

const memfd_path = "tigerbeetle-multiversion";

pub const Multiversion = struct {
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
    source_fd: ?posix.fd_t = null,

    target_fd: posix.fd_t,
    target_body_offset: ?u32 = null,
    target_header: ?MultiversionHeader = null,
    /// This list is referenced by `Replica.releases_bundled`.
    releases_bundled: ReleaseList = .{},

    completion: IO.Completion = undefined,

    timeout_completion: IO.Completion = undefined,
    timeout_statx: os.linux.Statx = undefined,
    timeout_statx_previous: union(enum) { none, previous: os.linux.Statx, err } = .none,
    timeout_start_enabled: bool = false,

    stage: union(enum) {
        init,

        source_stat,
        source_open,
        source_read,

        target_update,

        ready,
        err: anyerror,
    } = .init,

    pub fn init(allocator: std.mem.Allocator, io: *IO, exe_path: [:0]const u8) !Multiversion {
        assert(builtin.target.os.tag == .linux);
        assert(std.fs.path.isAbsolute(exe_path));

        // To keep the invariant that whatever has been advertised can be executed, while allowing
        // new binaries to be put in place, double buffering is used:
        // * source_buffer is where the in-progress data lives,
        // * target_fd is where the advertised data lives.
        // This does impact memory usage.
        const source_buffer = try allocator.alloc(u8, constants.multiversion_binary_size_max);
        errdefer allocator.free(source_buffer);

        // Only Linux has a nice API for executing from an in-memory file. For macOS and Windows,
        // a standard named temporary file will be used instead.
        const target_fd: posix.fd_t = switch (builtin.target.os.tag) {
            .linux => blk: {
                const fd = open_memory_file(memfd_path);
                errdefer posix.close(fd);

                try posix.ftruncate(fd, constants.multiversion_binary_size_max);

                break :blk fd;
            },

            // TODO: macOS / Windows support.
            else => IO.INVALID_FILE,
        };
        errdefer posix.close(target_fd);

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

    pub fn deinit(self: *Multiversion, allocator: std.mem.Allocator) void {
        posix.close(self.target_fd);
        self.target_fd = IO.INVALID_FILE;

        allocator.free(self.source_buffer);
        allocator.free(std.mem.span(self.args_envp.args[0].?));
        allocator.free(self.args_envp.args);

        self.timeout_start_enabled = false;
    }

    pub fn open_sync(self: *Multiversion) !void {
        assert(!self.timeout_start_enabled);
        assert(self.stage == .init);

        self.binary_open();

        assert(self.stage != .init);

        while (self.stage != .ready and self.stage != .err) {
            self.io.tick() catch |e| {
                assert(self.stage != .ready);
                self.stage = .{ .err = e };
            };
        }

        if (self.stage == .err) {
            // If there's been an error starting up multiversioning, don't disable it, but
            // advertise only the current version in memory.
            self.releases_bundled.clear();
            self.releases_bundled.append_assume_capacity(constants.config.process.release);

            return self.stage.err;
        }
    }

    pub fn timeout_enable(self: *Multiversion) void {
        assert(!self.timeout_start_enabled);
        assert(self.stage == .ready or self.stage == .err);

        self.timeout_start_enabled = true;
        self.timeout_start();

        log.debug("enabled automatic on-disk version detection.", .{});
    }

    fn timeout_start(self: *Multiversion) void {
        if (!self.timeout_start_enabled) return;

        self.io.timeout(
            *Multiversion,
            self,
            timeout_callback,
            &self.timeout_completion,
            @as(u63, @intCast(constants.multiversion_poll_interval_ms * std.time.ns_per_ms)),
        );
    }

    fn timeout_callback(
        self: *Multiversion,
        _: *IO.Completion,
        result: IO.TimeoutError!void,
    ) void {
        assert(self.stage == .init or self.stage == .ready or self.stage == .err);

        _ = result catch unreachable;
        if (!self.timeout_start_enabled) return;

        self.stage = .source_stat;
        self.io.statx(
            *Multiversion,
            self,
            binary_statx_callback,
            &self.completion,
            posix.AT.FDCWD,
            self.exe_path,
            0,
            os.linux.STATX_BASIC_STATS,
            &self.timeout_statx,
        );
    }

    fn binary_statx_callback(self: *Multiversion, _: *IO.Completion, result: anyerror!void) void {
        _ = result catch |e| {
            self.timeout_statx_previous = .err;
            self.timeout_start();

            return self.handle_error(e);
        };

        if (self.timeout_statx.mode & os.linux.S.IXUSR == 0) {
            return self.handle_error(error.BinaryNotMarkedExecutable);
        }

        // Zero the atime, so we can compare the rest of the struct directly.
        self.timeout_statx.atime = std.mem.zeroes(os.linux.statx_timestamp);

        if (self.timeout_statx_previous == .err or
            (self.timeout_statx_previous == .previous and !stdx.equal_bytes(
            os.linux.Statx,
            &self.timeout_statx_previous.previous,
            &self.timeout_statx,
        ))) {
            log.info("binary change detected: {s}", .{self.exe_path});

            self.stage = .init;
            self.binary_open();
        } else {
            self.stage = .init;
            self.timeout_start();
        }

        self.timeout_statx_previous = .{ .previous = self.timeout_statx };
    }

    fn binary_open(self: *Multiversion) void {
        assert(self.stage == .init or self.stage == .ready or self.stage == .err);
        self.stage = .source_open;

        self.io.openat(
            *Multiversion,
            self,
            binary_open_callback,
            &self.completion,
            IO.INVALID_FILE,
            self.exe_path,
            .{ .ACCMODE = .RDONLY },
            0,
        );
    }

    fn binary_open_callback(
        self: *Multiversion,
        _: *IO.Completion,
        result: IO.OpenatError!posix.fd_t,
    ) void {
        assert(self.stage == .source_open);
        assert(self.source_fd == null);
        const fd = result catch |e| return self.handle_error(e);

        self.stage = .source_read;
        self.source_fd = fd;
        self.io.read(
            *Multiversion,
            self,
            binary_read_callback,
            &self.completion,
            self.source_fd.?,
            self.source_buffer,
            0,
        );
    }

    fn binary_read_callback(
        self: *Multiversion,
        _: *IO.Completion,
        result: IO.ReadError!usize,
    ) void {
        assert(self.stage == .source_read);

        posix.close(self.source_fd.?);
        self.source_fd = null;

        const bytes_read = result catch |e| return self.handle_error(e);
        const source_buffer = self.source_buffer[0..bytes_read];

        self.stage = .target_update;
        self.target_update(source_buffer) catch |e| return self.handle_error(e);
        assert(self.stage == .ready);
    }

    fn target_update(self: *Multiversion, source_buffer: []u8) !void {
        assert(self.stage == .target_update);

        const offsets = switch (builtin.target.os.tag) {
            .linux => try parse_elf(source_buffer),
            else => @panic("multiversion unimplemented"),
        };

        if (offsets.header + @sizeOf(MultiversionHeader) > source_buffer.len) {
            return error.FileTooSmall;
        }

        // `from_bytes` validates the header checksum internally.
        const source_buffer_header = source_buffer[offsets.header..][0..@sizeOf(
            MultiversionHeader,
        )];
        const header = try MultiversionHeader.init_from_bytes(source_buffer_header);

        // Zero the header section in memory, to compute the hash, before copying it back.
        @memset(source_buffer_header, 0);
        const source_buffer_checksum = checksum.checksum(source_buffer);
        stdx.copy_disjoint(
            .exact,
            u8,
            source_buffer_header,
            std.mem.asBytes(&header),
        );

        if (source_buffer_checksum != header.checksum_binary_without_header) {
            return error.ChecksumMismatch;
        }

        // Potentially update the releases_bundled list, if all our checks pass:
        // 1. The release on disk includes the release we're running.
        // 2. The existing releases_bundled, of any versions newer than current, is a subset
        //    of the new advertisable releases.
        const advertisable = header.advertisable(constants.config.process.release);
        const advertisable_includes_running = blk: {
            for (advertisable.const_slice()) |release| {
                if (release.value == constants.config.process.release.value) {
                    break :blk true;
                }
            }
            break :blk false;
        };
        const advertisable_is_forward_superset = blk: {
            for (self.releases_bundled.const_slice()) |existing_release| {
                // It doesn't matter if older releases don't overlap.
                if (existing_release.value < constants.config.process.release.value) continue;

                for (advertisable.const_slice()) |release| {
                    if (existing_release.value == release.value) {
                        break;
                    }
                } else {
                    break :blk false;
                }
            }
            break :blk true;
        };

        if (!advertisable_includes_running) return error.RunningVersionNotIncluded;
        if (!advertisable_is_forward_superset) return error.NotSuperset;

        // Log out the releases bundled; both old and new. Only if this was a change detection run
        // and not from startup.
        if (self.timeout_statx_previous != .none)
            log.info("releases_bundled old: {any}", .{
                self.releases_bundled.const_slice(),
            });
        defer if (self.timeout_statx_previous != .none)
            log.info("releases_bundled new: {any}", .{
                self.releases_bundled.const_slice(),
            });

        // The below flip needs to happen atomically:
        // * update the releases_bundled to be what's in the source,
        // * update the target_fd to have the same contents as the source.
        //
        // Since target_fd points to a memfd on Linux, this is functionally a memcpy. On other
        // platforms, it's blocking IO - which is acceptable for development.
        self.releases_bundled.clear();
        self.releases_bundled.append_slice_assume_capacity(advertisable.const_slice());

        // While these look like blocking IO operations, on a memfd they're memory manipulation.
        // TODO: Would panic'ing be a better option? On Linux, these should never fail. On other
        // platforms where target_fd might be backed by a file, they could...
        errdefer log.err("target binary update failed - " ++
            "this replica might fail to automatically restart!", .{});

        try posix.lseek_SET(self.target_fd, 0);
        const bytes_written = try posix.write(self.target_fd, source_buffer);
        if (source_buffer.len != bytes_written) return error.ShortWrite;

        self.target_header = header;
        self.target_body_offset = offsets.body;

        self.stage = .ready;

        self.timeout_start();
    }

    fn handle_error(self: *Multiversion, result: anyerror) void {
        assert(self.stage != .init);

        log.err("binary does not contain valid multiversion data: {}", .{result});

        self.stage = .{ .err = result };

        self.timeout_start();
    }

    pub fn exec_latest(self: *Multiversion) !noreturn {
        assert(self.stage == .ready);

        log.info("re-executing {s}...\n", .{self.exe_path});

        if (execveat(
            self.target_fd,
            "",
            self.args_envp.args,
            self.args_envp.envp,
            posix.AT.EMPTY_PATH,
        ) == 0) {
            unreachable;
        } else {
            return error.ExecveatFailed;
        }
    }

    /// exec_release is called before a replica is fully open, but just after it has transitioned to
    /// static. Therefore, standard `os.read` blocking syscalls are available.
    /// (in any case, using blocking IO on a memfd on Linux is safe.)
    pub fn exec_release(self: *Multiversion, release_target: Release) !noreturn {
        // exec_release uses self.source_buffer, but this may be the target of an async read by
        // the kernel (from binary_open_callback). Assert that timeouts are not running, and
        // multiversioning is ready to ensure this can't be the case.
        assert(!self.timeout_start_enabled);
        assert(self.stage == .ready);

        const header = &self.target_header.?;

        // It should never happen that index is null: the caller must (and does, in the case of
        // replica_release_execute) ensure that exec_release is only called if the release
        // is available.
        const index = std.mem.indexOfScalar(
            u32,
            header.past.releases[0..header.past.count],
            release_target.value,
        ).?;

        const binary_offset = header.past.offsets[index];
        const binary_size = header.past.sizes[index];
        const binary_checksum = header.past.checksums[index];

        // Our target release is physically embedded in the binary. Shuffle the bytes
        // around, so that it's at the start, then truncate the descriptor so there's nothing
        // trailing.
        try posix.lseek_SET(self.target_fd, self.target_body_offset.? + binary_offset);
        const bytes_read = try posix.read(self.target_fd, self.source_buffer[0..binary_size]);
        assert(bytes_read == binary_size);

        try posix.lseek_SET(self.target_fd, 0);
        const bytes_written = try posix.write(self.target_fd, self.source_buffer[0..binary_size]);
        assert(bytes_written == binary_size);

        // Zero the remaining bytes in the file.
        try posix.ftruncate(self.target_fd, binary_size);

        // Ensure the checksum matches the header. This could have been done above, but
        // do it in a separate step to make sure.
        const written_checksum = blk: {
            try posix.lseek_SET(self.target_fd, 0);
            const bytes_read_for_checksum = try posix.read(
                self.target_fd,
                self.source_buffer[0..binary_size],
            );
            assert(bytes_read_for_checksum == binary_size);
            break :blk checksum.checksum(self.source_buffer[0..binary_size]);
        };
        assert(written_checksum == binary_checksum);

        switch (builtin.target.os.tag) {
            .linux => {
                log.info("executing release {}...\n", .{release_target});

                if (execveat(
                    self.target_fd,
                    "",
                    self.args_envp.args,
                    self.args_envp.envp,
                    posix.AT.EMPTY_PATH,
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
        const path = try allocator.dupeZ(u8, std.mem.span(os.argv[0]));
        assert(std.fs.path.isAbsolute(path));

        return path;
    } else {
        // Not running from a memfd. `native_self_exe_path` is the real path.
        return try allocator.dupeZ(u8, native_self_exe_path);
    }
}

/// Parse an untrusted, unverified, and potentially corrupt ELF file. This parsing happens before
/// any checksums are verified, and so needs to deal with any ELF metadata being corrupt, while
/// not panicing and returning errors.
///
/// Anything that would normally assert should return an error instead - especially implicit things
/// like bounds checking on slices.
fn parse_elf(buffer: []const u8) !struct { header: u32, body: u32 } {
    if (@sizeOf(elf.Elf64_Ehdr) > buffer.len) return error.InvalidELF;
    const elf_header = try elf.Header.parse(@alignCast(buffer[0..@sizeOf(elf.Elf64_Ehdr)]));

    // TigerBeetle only supports little endian on 64 bit platforms.
    if (elf_header.endian != .little) return error.WrongEndian;
    if (!elf_header.is_64) return error.Not64bit;

    // Map to some non-abbreviated names to make understanding ELF a little bit easier. Later on,
    // when sh_* names are used, they refer to `section header ...`.
    const elf_section_headers_offset = elf_header.shoff;
    const elf_section_headers_count = elf_header.shnum;
    const string_table_section_header_index = elf_header.shstrndx;

    // Only support "simple" ELF string tables.
    if (string_table_section_header_index >= elf.SHN_LORESERVE) return error.LongStringTable;
    if (string_table_section_header_index == elf.SHN_UNDEF) return error.LongStringTable;

    // We iterate over elf_section_headers_count, so add a sanity check on the number of sections
    // in the file. It is a u16, so it is bounded relatively low already, but we expect on the
    // order of maybe ~30 with debug symbols.
    if (elf_section_headers_count > 128) return error.TooManySections;
    if (elf_section_headers_count < 2) return error.TooFewSections;

    // First, read the string table section.
    const string_table_elf_section_header_offset: u64 = elf_section_headers_offset +
        @as(u64, @sizeOf(elf.Elf64_Shdr)) * string_table_section_header_index;
    if (string_table_elf_section_header_offset + @sizeOf(elf.Elf64_Shdr) > buffer.len) {
        return error.InvalidELF;
    }
    const string_table_elf_section_header = std.mem.bytesAsValue(
        elf.Elf64_Shdr,
        buffer[string_table_elf_section_header_offset..][0..@sizeOf(elf.Elf64_Shdr)],
    );

    if (string_table_elf_section_header.sh_type != elf.SHT_STRTAB) return error.InvalidStringTable;
    if (string_table_elf_section_header.sh_size <= 0) return error.InvalidStringTable;
    if (string_table_elf_section_header.sh_size >= buffer.len) return error.InvalidStringTable;

    const string_table_offset = string_table_elf_section_header.sh_offset;

    if (@as(u65, string_table_offset) + string_table_elf_section_header.sh_size >
        std.math.maxInt(usize))
    {
        return error.InvalidStringTable;
    }

    if (string_table_offset + string_table_elf_section_header.sh_size > buffer.len) {
        return error.InvalidStringTable;
    }

    if (buffer[string_table_offset + string_table_elf_section_header.sh_size - 1] != 0) {
        return error.InvalidStringTable;
    }
    const string_table =
        buffer[string_table_offset..][0 .. string_table_elf_section_header.sh_size - 1 :0];

    // Next, go through each ELF section to find the ones we're looking for:
    var header_offset: ?u32 = null;
    var body_offset: ?u32 = null;
    var body_size: ?u32 = null;
    for (0..elf_section_headers_count) |i| {
        const offset: u64 = elf_section_headers_offset + @as(u64, @sizeOf(elf.Elf64_Shdr)) * i;
        if (offset + @sizeOf(elf.Elf64_Shdr) > buffer.len) return error.InvalidSectionOffset;

        const elf_section_header = std.mem.bytesAsValue(
            elf.Elf64_Shdr,
            buffer[offset..][0..@sizeOf(elf.Elf64_Shdr)],
        );

        if (elf_section_header.sh_name > string_table.len) return error.InvalidStringTableOffset;

        // This will always match _something_, since above we check that the last item in the
        // string table is a null terminator.
        const name = std.mem.sliceTo(
            @as([*:0]const u8, string_table[elf_section_header.sh_name.. :0]),
            0,
        );

        if (std.mem.eql(u8, name, ".tb_mvb")) {
            // The body must be the second-last section in the file.
            if (body_offset != null) return error.MultipleMultiversionBody;
            if (i != elf_section_headers_count - 2) return error.InvalidMultiversionBodyLocation;
            if (elf_section_header.sh_offset > std.math.maxInt(@TypeOf(body_offset.?))) {
                return error.InvalidMultiversionBodyOffset;
            }
            if (elf_section_header.sh_size > std.math.maxInt(@TypeOf(body_size.?))) {
                return error.InvalidMultiversionBodySize;
            }

            assert(body_size == null);

            body_offset = @intCast(elf_section_header.sh_offset);
            body_size = @intCast(elf_section_header.sh_size);
        } else if (std.mem.eql(u8, name, ".tb_mvh")) {
            // The header must be the last section in the file. (It's _logically_ a header.)
            if (header_offset != null) return error.MultipleMultiversionHeader;
            if (elf_section_header.sh_size != @sizeOf(MultiversionHeader)) {
                return error.InvalidMultiversionHeaderSize;
            }
            if (i != elf_section_headers_count - 1) return error.InvalidMultiversionHeaderLocation;
            if (elf_section_header.sh_offset > std.math.maxInt(@TypeOf(header_offset.?))) {
                return error.InvalidMultiversionHeaderOffset;
            }

            header_offset = @intCast(elf_section_header.sh_offset);
        }
    }

    if (header_offset == null or body_offset == null) {
        return error.MultiversionHeaderOrBodyNotFound;
    }

    if (body_offset.? + body_size.? > header_offset.?) {
        return error.MultiversionBodyOverlapsHeader;
    }

    return .{
        .header = header_offset.?,
        .body = body_offset.?,
    };
}

fn expect_any_error(actual_error_union: anytype) !void {
    if (actual_error_union) |_| return error.TestUnexpectedError else |_| {}
}

const test_elf_name_length_max = 10;

fn test_elf_build_header(buffer: []align(8) u8) !*elf.Elf64_Ehdr {
    try expect_any_error(parse_elf(buffer));

    const elf_header: *elf.Elf64_Ehdr = std.mem.bytesAsValue(
        elf.Elf64_Ehdr,
        buffer[0..@sizeOf(elf.Elf64_Ehdr)],
    );

    stdx.copy_disjoint(.exact, u8, elf_header.e_ident[0..4], elf.MAGIC);
    try expect_any_error(parse_elf(buffer));

    elf_header.e_ident[elf.EI_VERSION] = 1;
    try expect_any_error(parse_elf(buffer));
    elf_header.e_ident[elf.EI_DATA] = elf.ELFDATA2LSB;
    try expect_any_error(parse_elf(buffer));
    elf_header.e_ident[elf.EI_CLASS] = elf.ELFCLASS64;
    try expect_any_error(parse_elf(buffer));

    elf_header.e_shnum = 4;
    try expect_any_error(parse_elf(buffer));
    elf_header.e_shoff = 8192;
    try expect_any_error(parse_elf(buffer));
    elf_header.e_shstrndx = 1;
    try expect_any_error(parse_elf(buffer));

    return elf_header;
}

fn test_elf_build_string_table(buffer: []align(8) u8, elf_header: *elf.Elf64_Ehdr) ![]u8 {
    const string_table_elf_section_header_offset: u64 = elf_header.e_shoff +
        @as(u64, @sizeOf(elf.Elf64_Shdr)) * elf_header.e_shstrndx;

    const string_table_elf_section_header = std.mem.bytesAsValue(
        elf.Elf64_Shdr,
        buffer[string_table_elf_section_header_offset..][0..@sizeOf(elf.Elf64_Shdr)],
    );

    string_table_elf_section_header.sh_type = elf.SHT_STRTAB;
    try expect_any_error(parse_elf(buffer));

    string_table_elf_section_header.sh_size = test_elf_name_length_max * elf_header.e_shnum;
    try expect_any_error(parse_elf(buffer));

    string_table_elf_section_header.sh_offset = 300;
    try expect_any_error(parse_elf(buffer));

    string_table_elf_section_header.sh_name = @intCast(string_table_elf_section_header.sh_size - 1);

    const string_table_size = string_table_elf_section_header.sh_size;
    const string_table = buffer[string_table_elf_section_header.sh_offset..][0..string_table_size];
    string_table[string_table_elf_section_header.sh_size - 1] = 0;
    try expect_any_error(parse_elf(buffer));

    return string_table;
}

fn test_elf_build_section(
    buffer: []align(8) u8,
    string_table: []u8,
    elf_header: *elf.Elf64_Ehdr,
    index: u32,
    name: []const u8,
) !*align(1) elf.Elf64_Shdr {
    assert(name.len < test_elf_name_length_max);
    assert(index < elf_header.e_shnum);

    const offset: u64 = elf_header.e_shoff + @as(u64, @sizeOf(elf.Elf64_Shdr)) * index;
    const elf_section_header = std.mem.bytesAsValue(
        elf.Elf64_Shdr,
        buffer[offset..][0..@sizeOf(elf.Elf64_Shdr)],
    );
    elf_section_header.sh_name = test_elf_name_length_max * index;
    try expect_any_error(parse_elf(buffer));

    stdx.copy_disjoint(.inexact, u8, string_table[elf_section_header.sh_name..], name);
    try expect_any_error(parse_elf(buffer));
    string_table[elf_section_header.sh_name..][name.len] = 0;
    try expect_any_error(parse_elf(buffer));
    elf_section_header.sh_offset = 8192 * index;
    try expect_any_error(parse_elf(buffer));

    return elf_section_header;
}

// Not quite a fuzzer, but build up an ELF, checking that there's an error after each step, with a
// full range of values is the undefined intermediate bits.
test "parse_elf" {
    var buffer: [32768]u8 align(8) = undefined;
    for (0..256) |i| {
        @memset(&buffer, @as(u8, @intCast(i)));

        const elf_header = try test_elf_build_header(&buffer);
        const string_table = try test_elf_build_string_table(&buffer, elf_header);

        // The string table can't be 0, and the .tb_mvb and .tb_mvh sections need to be the second
        // last and last sections in the file respectively. Pad the 0 index with a no-op section.
        _ = try test_elf_build_section(&buffer, string_table, elf_header, 0, ".tb_nop");

        const section_mvb = try test_elf_build_section(
            &buffer,
            string_table,
            elf_header,
            2,
            ".tb_mvb",
        );
        // So it overlaps on purpose, to check the MultiversionBodyOverlapsHeader assert.
        section_mvb.sh_size = 16384;

        const section_mvh = try test_elf_build_section(
            &buffer,
            string_table,
            elf_header,
            3,
            ".tb_mvh",
        );
        section_mvh.sh_size = 8192; // @sizeOf(MultiversionHeader), but hardcoded.

        try std.testing.expectError(error.MultiversionBodyOverlapsHeader, parse_elf(&buffer));

        section_mvb.sh_size = 8192;
        const parsed = try parse_elf(&buffer);

        assert(parsed.body == 16384);
        assert(parsed.header == 24576);
    }
}

pub fn validate(allocator: std.mem.Allocator, exe_path: [:0]const u8) !void {
    if (builtin.target.os.tag != .linux) @panic("only linux is supported for validate()");

    var io = try IO.init(32, 0);
    defer io.deinit();

    const absolute_exe_path = try std.fs.cwd().realpathAlloc(allocator, exe_path);
    defer allocator.free(absolute_exe_path);

    const absolute_exe_path_z = try allocator.dupeZ(u8, absolute_exe_path);
    defer allocator.free(absolute_exe_path_z);

    var multiversion = try Multiversion.init(
        allocator,
        &io,
        absolute_exe_path_z,
    );
    defer multiversion.deinit(allocator);

    try multiversion.open_sync();
    assert(multiversion.stage == .ready);
}

pub fn print_information(allocator: std.mem.Allocator, stdout: anytype) !void {
    if (builtin.target.os.tag != .linux) {
        try std.fmt.format(stdout, "multiversioning not enabled: {s} unsupported\n", .{
            @tagName(builtin.target.os.tag),
        });

        return;
    }

    var io = try IO.init(32, 0);
    defer io.deinit();

    const absolute_exe_path = try self_exe_path(allocator);
    defer allocator.free(absolute_exe_path);

    var multiversion = try Multiversion.init(
        allocator,
        &io,
        absolute_exe_path,
    );
    defer multiversion.deinit(allocator);

    try multiversion.open_sync();
    assert(multiversion.stage == .ready);
    multiversion.timeout_start_enabled = false;

    if (multiversion.stage == .err) {
        try std.fmt.format(stdout, "multiversioning not enabled: {}\n", .{multiversion.stage.err});

        return;
    }

    const header = multiversion.target_header.?;
    try std.fmt.format(
        stdout,
        "multiversioning.releases_bundled={any}\n",
        .{multiversion.releases_bundled.const_slice()},
    );

    inline for (comptime std.meta.fieldNames(MultiversionHeader)) |field| {
        if (!std.mem.eql(u8, field, "past") and
            !std.mem.eql(u8, field, "current_flags_padding") and
            !std.mem.eql(u8, field, "past_padding") and
            !std.mem.eql(u8, field, "reserved"))
        {
            try std.fmt.format(stdout, "multiversioning.{s}={any}\n", .{
                field,
                if (comptime std.mem.eql(u8, field, "current_release"))
                    Release{ .value = @field(header, field) }
                else
                    @field(header, field),
            });
        }
    }

    try std.fmt.format(
        stdout,
        "multiversioning.past.count={}\n",
        .{header.past.count},
    );
    inline for (comptime std.meta.fieldNames(MultiversionHeader.PastReleases)) |field| {
        if (comptime std.mem.eql(u8, field, "releases")) {
            var release_list: ReleaseList = .{};
            for (@field(header.past, field)[0..header.past.count]) |release| {
                release_list.append_assume_capacity(Release{ .value = release });
            }

            try std.fmt.format(stdout, "multiversioning.past.{s}={any}\n", .{
                field,
                release_list.const_slice(),
            });
        } else if (comptime std.mem.eql(u8, field, "git_commits")) {
            try std.fmt.format(stdout, "multiversioning.past.{s}={{ ", .{field});

            for (@field(header.past, field)[0..header.past.count]) |*git_commit| {
                try std.fmt.format(stdout, "{s} ", .{
                    std.fmt.fmtSliceHexLower(git_commit),
                });
            }

            try std.fmt.format(stdout, "}}\n", .{});
        } else if (comptime (!std.mem.eql(u8, field, "count") and
            !std.mem.eql(u8, field, "flags_padding")))
        {
            try std.fmt.format(stdout, "multiversioning.past.{s}={any}\n", .{
                field,
                @field(header.past, field)[0..header.past.count],
            });
        }
    }
}
