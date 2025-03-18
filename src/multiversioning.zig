const builtin = @import("builtin");
const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const os = std.os;
const posix = std.posix;
const constants = @import("constants.zig");
const IO = @import("io.zig").IO;
const Timeout = @import("./vsr.zig").Timeout;

const elf = std.elf;

// Re-export to make release code easier.
pub const checksum = @import("vsr/checksum.zig");
pub const multiversion_binary_size_max = constants.multiversion_binary_size_max;
pub const multiversion_binary_platform_size_max = constants.multiversion_binary_platform_size_max;

// Useful for test code, or constructing releases in release.zig.
pub const ListU32 = stdx.BoundedArrayType(u32, constants.vsr_releases_max);
pub const ListU128 = stdx.BoundedArrayType(u128, constants.vsr_releases_max);
pub const ListGitCommit = stdx.BoundedArrayType([20]u8, constants.vsr_releases_max);
pub const ListFlag = stdx.BoundedArrayType(MultiversionHeader.Flags, constants.vsr_releases_max);

/// In order to embed multiversion headers and bodies inside a universal binary, we repurpose some
/// old CPU Type IDs.
/// These are valid (in the MachO spec) but ancient (macOS has never run on anything other than
/// x86_64 / arm64) platforms. They were chosen so that it wouldn't be a random value, but also
/// wouldn't be something that could be realistically encountered.
pub const section_to_macho_cpu = enum(c_int) {
    tb_mvb_aarch64 = 0x00000001, // VAX
    tb_mvh_aarch64 = 0x00000002, // ROMP
    tb_mvb_x86_64 = 0x00000004, // NS32032
    tb_mvh_x86_64 = 0x00000005, // NS32332
};

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
pub const ReleaseList = stdx.BoundedArrayType(Release, constants.vsr_releases_max);

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
    pub const Flags = packed struct {
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

        /// Used by the build process to verify that the inner checksums are correct. Skipped during
        /// runtime, as the outer checksum includes them all. This same method can't be implemented
        /// for current_release, as that would require `objcopy` at runtime to split the pieces out.
        pub fn verify_checksums(self: *const PastReleases, body: []const u8) !void {
            for (
                self.checksums[0..self.count],
                self.offsets[0..self.count],
                self.sizes[0..self.count],
            ) |checksum_expected, offset, size| {
                const checksum_calculated = checksum.checksum(body[offset..][0..size]);
                if (checksum_calculated != checksum_expected) {
                    return error.PastReleaseChecksumMismatch;
                }
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
    /// it separately. See exec_current vs exec_release.
    current_release: u32,

    current_flags: Flags,
    current_flags_padding: [3]u8 = std.mem.zeroes([3]u8),

    past: PastReleases = .{},
    past_padding: [16]u8 = std.mem.zeroes([16]u8),

    current_git_commit: [20]u8,
    current_release_client_min: u32,

    /// Reserved space for future use. This is special: unlike the rest of the *_padding fields,
    /// which are required to be zeroed, this is not. This allows adding whole new fields in a
    /// backwards compatible way, while preventing the temptation of changing the meaning of
    /// existing fields without bumping the schema version entirely.
    reserved: [4744]u8 = std.mem.zeroes([4744]u8),

    /// Parses an instance from a slice of bytes and validates its checksum. Returns a copy.
    pub fn init_from_bytes(bytes: *const [@sizeOf(MultiversionHeader)]u8) !MultiversionHeader {
        const self = std.mem.bytesAsValue(MultiversionHeader, bytes).*;
        try self.verify();

        return self;
    }

    pub fn verify(self: *const MultiversionHeader) !void {
        const checksum_calculated = self.calculate_header_checksum();

        if (checksum_calculated != self.checksum_header) return error.ChecksumMismatch;
        if (self.schema_version != 1) return error.InvalidSchemaVersion;
        if (self.vsr_releases_max != constants.vsr_releases_max) return error.InvalidVSRReleaseMax;
        if (self.current_flags.padding != 0) return error.InvalidCurrentFlags;
        if (!stdx.zeroed(&self.current_flags_padding)) return error.InvalidCurrentFlags;
        if (!self.current_flags.visit) return error.InvalidCurrentFlags;
        if (self.current_release == 0) return error.InvalidCurrentRelease;

        // current_git_commit and current_release_client_min were added after 0.15.4.
        if (self.current_release > (try Release.parse("0.15.4")).value) {
            if (stdx.zeroed(&self.current_git_commit)) return error.InvalidCurrentRelease;
            if (self.current_release_client_min == 0) return error.InvalidCurrentRelease;
        } else {
            if (!stdx.zeroed(&self.current_git_commit)) return error.InvalidCurrentRelease;
            if (self.current_release_client_min != 0) return error.InvalidCurrentRelease;
        }

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
            .current_git_commit = std.mem.zeroes([20]u8),
            .current_release_client_min = 0,
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

const multiversion_uuid = "tigerbeetle-multiversion-1768a738-ef69-4605-8b5c-c6e63580e345";

pub const Multiversion = struct {
    const ArgsEnvp = if (builtin.target.os.tag == .windows) void else struct {
        // Coerces to [*:null]const ?[*:0]const u8 but lets us keep information to free the memory
        // later.
        args: [:null]?[*:0]const u8,
        envp: [*:null]const ?[*:0]const u8,
    };

    const ExePathFormat = enum { elf, pe, macho, detect };

    io: *IO,

    exe_path: [:0]const u8,
    exe_path_format: ExePathFormat,
    args_envp: ArgsEnvp,

    source_buffer: []align(8) u8,
    source_fd: ?posix.fd_t = null,
    source_offset: ?u64 = null,

    target_fd: posix.fd_t,
    target_path: [:0]const u8,
    target_body_offset: ?u32 = null,
    target_body_size: ?u32 = null,
    target_header: ?MultiversionHeader = null,
    /// This list is referenced by `Replica.releases_bundled`.
    /// Note that this only contains the advertisable releases, which are a subset of the actual
    /// releases included in the multiversion binary. See MultiversionHeader.advertisable().
    releases_bundled: ReleaseList = .{},

    completion: IO.Completion = undefined,

    timeout: Timeout,
    timeout_statx: os.linux.Statx = undefined,
    timeout_statx_previous: union(enum) { none, previous: os.linux.Statx, err } = .none,

    stage: union(enum) {
        init,

        source_stat,
        source_open,
        source_read,

        target_update,

        ready,
        err: anyerror,
    } = .init,

    pub fn init(
        allocator: std.mem.Allocator,
        io: *IO,
        exe_path: [:0]const u8,
        exe_path_format: enum { detect, native },
    ) !Multiversion {
        assert(std.fs.path.isAbsolute(exe_path));

        const multiversion_binary_size_max_by_format = switch (exe_path_format) {
            .detect => constants.multiversion_binary_size_max,
            .native => constants.multiversion_binary_platform_size_max(.{
                .macos = builtin.target.os.tag == .macos,
                .debug = builtin.mode != .ReleaseSafe,
            }),
        };

        // To keep the invariant that whatever has been advertised can be executed, while allowing
        // new binaries to be put in place, double buffering is used:
        // * source_buffer is where the in-progress data lives,
        // * target_fd is where the advertised data lives.
        // This does impact memory usage.
        const source_buffer = try allocator.alignedAlloc(
            u8,
            8,
            multiversion_binary_size_max_by_format,
        );
        errdefer allocator.free(source_buffer);

        const nonce = stdx.unique_u128();

        const target_path: [:0]const u8 = switch (builtin.target.os.tag) {
            .linux => try allocator.dupeZ(u8, multiversion_uuid),
            .macos, .windows => blk: {
                const suffix = if (builtin.target.os.tag == .windows) ".exe" else "";
                const temporary_directory = try system_temporary_directory(allocator);
                defer allocator.free(temporary_directory);
                const filename = try std.fmt.allocPrint(allocator, "{s}-{}" ++ suffix, .{
                    multiversion_uuid,
                    nonce,
                });
                defer allocator.free(filename);
                break :blk try std.fs.path.joinZ(allocator, &.{ temporary_directory, filename });
            },
            else => @panic("unsupported platform"),
        };
        errdefer allocator.free(target_path);

        // Only Linux has a nice API for executing from an in-memory file. For macOS and Windows,
        // a standard named temporary file will be used instead.
        const target_fd: posix.fd_t = switch (builtin.target.os.tag) {
            .linux => blk: {
                const fd = open_memory_file(target_path);
                errdefer posix.close(fd);

                try posix.ftruncate(fd, multiversion_binary_size_max_by_format);

                break :blk fd;
            },

            .macos, .windows => blk: {
                const mode = if (builtin.target.os.tag == .macos) 0o777 else 0;
                const file = std.fs.createFileAbsolute(
                    target_path,
                    .{ .read = true, .truncate = true, .mode = mode },
                ) catch |e| std.debug.panic(
                    "error in target_fd open: {}",
                    .{e},
                );
                try file.setEndPos(multiversion_binary_size_max);

                break :blk file.handle;
            },

            else => @panic("unsupported platform"),
        };
        errdefer posix.close(target_fd);

        const args_envp: ArgsEnvp = switch (builtin.target.os.tag) {
            .linux, .macos => blk: {
                // We can pass through our env as-is to exec. We have to manipulate the types
                // here somewhat: they're cast in start.zig and we can't access `argc_argv_ptr`
                // directly. process.zig does the same trick in execve().
                //
                // For args, modify them so that argv[0] is exe_path. This allows our memfd executed
                // binary to find its way back to the real file on disk.
                const args = try allocator.allocSentinel(?[*:0]const u8, os.argv.len, null);
                errdefer allocator.free(args);

                args[0] = try allocator.dupeZ(u8, exe_path);
                errdefer allocator.free(args[0]);

                for (1..os.argv.len) |i| args[i] = os.argv[i];

                break :blk .{
                    .args = args,
                    .envp = @as([*:null]const ?[*:0]const u8, @ptrCast(os.environ.ptr)),
                };
            },

            // ArgsEnvp is void on Windows, and command line passing is handled directly by
            // exec_target_fd().
            .windows => {},

            else => @panic("unsupported platform"),
        };

        return .{
            .io = io,

            .exe_path = exe_path,
            .exe_path_format = switch (exe_path_format) {
                .native => switch (builtin.target.os.tag) {
                    .linux => .elf,
                    .windows => .pe,
                    .macos => .macho,
                    else => @panic("unsupported platform"),
                },
                .detect => .detect,
            },

            .args_envp = args_envp,

            .source_buffer = source_buffer,

            .target_fd = target_fd,
            .target_path = target_path,

            .timeout = Timeout{
                .name = "multiversioning_timeout",
                .id = 0, // id for logging is set by timeout_enable after opening the superblock.
                .after = constants.multiversion_poll_interval_ms / constants.tick_ms,
            },
        };
    }

    pub fn deinit(self: *Multiversion, allocator: std.mem.Allocator) void {
        posix.close(self.target_fd);
        self.target_fd = IO.INVALID_FILE;
        allocator.free(self.target_path);

        allocator.free(self.source_buffer);

        if (builtin.target.os.tag != .windows) {
            allocator.free(std.mem.span(self.args_envp.args[0].?));
            allocator.free(self.args_envp.args);
        }
        self.* = undefined;
    }

    pub fn open_sync(self: *Multiversion) !void {
        assert(self.stage == .init);
        assert(!self.timeout.ticking);

        if (comptime builtin.target.os.tag == .linux) {
            self.binary_statx();
        } else {
            self.binary_open();
        }
        assert(self.stage != .init);

        while (self.stage != .ready and self.stage != .err) {
            self.io.run() catch |e| {
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

        assert(self.stage == .ready);
        assert(self.target_header != null);
        assert(self.releases_bundled.count() >= 1);

        if (comptime builtin.target.os.tag == .linux) {
            assert(self.timeout_statx_previous != .none);
        }
    }

    pub fn tick(self: *Multiversion) void {
        self.timeout.tick();
        if (self.timeout.fired()) self.on_timeout();
    }

    pub fn timeout_start(self: *Multiversion, replica_index: u8) void {
        assert(!self.timeout.ticking);
        if (builtin.target.os.tag != .linux) {
            // Checking for new binaries on disk after the replica has been opened is only
            // supported on Linux.
            return;
        }
        assert(self.timeout.id == 0);
        self.timeout.id = replica_index;
        self.timeout.start();
        log.debug("enabled automatic on-disk version detection.", .{});
    }

    fn on_timeout(self: *Multiversion) void {
        self.timeout.reset();

        assert(builtin.target.os.tag == .linux);
        if (comptime builtin.target.os.tag != .linux) return; // Prevent codegen.

        switch (self.stage) {
            .source_stat,
            .source_open,
            .source_read,
            .target_update,
            => return, // Previous check still in progress

            .init, .ready, .err => {},
        }

        self.stage = .init;
        self.binary_statx();
    }

    fn binary_statx(self: *Multiversion) void {
        assert(self.stage == .init);

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
        assert(self.stage == .source_stat);

        _ = result catch |e| {
            self.timeout_statx_previous = .err;

            return self.handle_error(e);
        };

        if (self.timeout_statx.mode & os.linux.S.IXUSR == 0) {
            return self.handle_error(error.BinaryNotMarkedExecutable);
        }

        // Zero the atime, so we can compare the rest of the struct directly.
        self.timeout_statx.atime = std.mem.zeroes(os.linux.statx_timestamp);

        if (self.timeout_statx_previous == .previous and
            stdx.equal_bytes(
            os.linux.Statx,
            &self.timeout_statx_previous.previous,
            &self.timeout_statx,
        )) {
            self.stage = .init;
        } else {
            if (self.timeout_statx_previous != .none) {
                log.info("binary change detected: {s}", .{self.exe_path});
            }

            self.stage = .init;
            self.binary_open();
        }

        self.timeout_statx_previous = .{ .previous = self.timeout_statx };
    }

    fn binary_open(self: *Multiversion) void {
        assert(self.stage == .init);
        assert(self.source_offset == null);

        self.stage = .source_open;
        self.source_offset = 0;

        switch (builtin.os.tag) {
            .linux => self.io.openat(
                *Multiversion,
                self,
                binary_open_callback,
                &self.completion,
                IO.INVALID_FILE,
                self.exe_path,
                .{ .ACCMODE = .RDONLY },
                0,
            ),
            .macos, .windows => {
                const file = std.fs.openFileAbsolute(self.exe_path, .{}) catch |e|
                    std.debug.panic("error in binary_open: {}", .{e});
                self.binary_open_callback(&self.completion, file.handle);
            },
            else => @panic("unsupported platform"),
        }
    }

    fn binary_open_callback(
        self: *Multiversion,
        _: *IO.Completion,
        result: IO.OpenatError!posix.fd_t,
    ) void {
        assert(self.stage == .source_open);
        assert(self.source_fd == null);
        assert(self.source_offset.? == 0);

        const fd = result catch |e| {
            self.source_offset = null;
            return self.handle_error(e);
        };

        self.stage = .source_read;
        self.source_fd = fd;
        self.binary_read();
    }

    fn binary_read(self: *Multiversion) void {
        assert(self.stage == .source_read);
        assert(self.source_fd != null);
        assert(self.source_offset != null);
        assert(self.source_offset.? < self.source_buffer.len);

        self.io.read(
            *Multiversion,
            self,
            binary_read_callback,
            &self.completion,
            self.source_fd.?,
            self.source_buffer[self.source_offset.?..],
            self.source_offset.?,
        );
    }

    fn binary_read_callback(
        self: *Multiversion,
        _: *IO.Completion,
        result: IO.ReadError!usize,
    ) void {
        assert(self.stage == .source_read);
        assert(self.source_fd != null);
        assert(self.source_offset != null);
        assert(self.source_offset.? < self.source_buffer.len);

        defer {
            if (self.stage != .source_read) {
                assert(self.stage == .err or self.stage == .ready);
                assert(self.source_fd != null);
                assert(self.source_offset != null);

                posix.close(self.source_fd.?);
                self.source_offset = null;
                self.source_fd = null;
            }
        }

        const bytes_read = result catch |e| return self.handle_error(e);
        self.source_offset.? += bytes_read;
        assert(self.source_offset.? <= self.source_buffer.len);
        // This could be a truncated file, but it'll get rejected when we verify the checksum.
        maybe(self.source_offset.? == self.source_buffer.len);

        if (bytes_read == 0) {
            const source_buffer = self.source_buffer[0..self.source_offset.?];

            self.stage = .target_update;
            self.target_update(source_buffer) catch |e| return self.handle_error(e);
            assert(self.stage == .ready);
        } else {
            self.binary_read();
        }
    }

    fn target_update(self: *Multiversion, source_buffer: []align(8) u8) !void {
        assert(self.stage == .target_update);
        const offsets = switch (self.exe_path_format) {
            .elf => try parse_elf(source_buffer),
            .pe => try parse_pe(source_buffer),
            .macho => try parse_macho(source_buffer),
            .detect => parse_elf(source_buffer) catch parse_pe(source_buffer) catch
                parse_macho(source_buffer) catch return error.NoValidPlatformDetected,
        };

        const active = offsets.active() orelse return error.NoValidPlatformDetected;

        if (active.header_offset + @sizeOf(MultiversionHeader) > source_buffer.len) {
            return error.FileTooSmall;
        }

        // `init_from_bytes` validates the header checksum internally.
        const source_buffer_header =
            source_buffer[active.header_offset..][0..@sizeOf(MultiversionHeader)];
        const header = try MultiversionHeader.init_from_bytes(source_buffer_header);
        var header_inactive_platform: ?MultiversionHeader = null;

        // MachO's checksum_binary_without_header works slightly differently since there are
        // actually two headers, once for x86_64 and one for aarch64. It zeros them both.
        if (offsets.inactive()) |inactive| {
            assert(offsets.format == .macho);

            const source_buffer_header_inactive_platform =
                source_buffer[inactive.header_offset..][0..@sizeOf(MultiversionHeader)];
            header_inactive_platform = try MultiversionHeader.init_from_bytes(
                source_buffer_header_inactive_platform,
            );
            @memset(source_buffer_header_inactive_platform, 0);
            if (header.checksum_binary_without_header !=
                header_inactive_platform.?.checksum_binary_without_header)
            {
                return error.HeadersDiffer;
            }
        }

        // Zero the header section in memory, to compute the hash, before copying it back.
        @memset(source_buffer_header, 0);
        const source_buffer_checksum = checksum.checksum(source_buffer);
        if (source_buffer_checksum != header.checksum_binary_without_header) {
            return error.ChecksumMismatch;
        }

        // Restore the header(s).
        stdx.copy_disjoint(
            .exact,
            u8,
            source_buffer_header,
            std.mem.asBytes(&header),
        );

        if (offsets.inactive()) |inactive| {
            assert(offsets.format == .macho);
            const source_buffer_header_inactive_platform =
                source_buffer[inactive.header_offset..][0..@sizeOf(MultiversionHeader)];

            stdx.copy_disjoint(
                .exact,
                u8,
                source_buffer_header_inactive_platform,
                std.mem.asBytes(&header_inactive_platform.?),
            );
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
        errdefer log.warn("target binary update failed - " ++
            "this replica might fail to automatically restart!", .{});

        const target_file = std.fs.File{ .handle = self.target_fd };
        try target_file.pwriteAll(source_buffer, 0);

        self.target_header = header;
        self.target_body_offset = active.body_offset;
        self.target_body_size = active.body_size;

        self.stage = .ready;
    }

    fn handle_error(self: *Multiversion, result: anyerror) void {
        assert(self.stage != .init);

        log.err("binary does not contain valid multiversion data: {}", .{result});

        self.stage = .{ .err = result };
    }

    pub fn exec_current(self: *Multiversion, release_target: Release) !noreturn {
        // target_fd is only modified in target_update() which happens synchronously.
        assert(self.stage != .target_update);

        // Ensure that target_update() has been called at least once, and thus target_fd is
        // populated by checking that target_header has been set.
        assert(self.target_header != null);

        // `release_target` is only used as a sanity check, and doesn't control the exec path here.
        // There are two possible cases:
        // * release_target == target_header.current_release:
        //   The latest release will be executed, and it won't do any more re-execs from there
        //   onwards (that we know about). Happens when jumping to the latest release.
        // * release_target in target_header.past.releases:
        //   The latest release will be executed, but after starting up it will use exec_release()
        //   to execute a past version. Happens when stopping at an intermediate release with
        //   visit == true.
        const release_target_current = release_target.value == self.target_header.?.current_release;
        const release_target_past = std.mem.indexOfScalar(
            u32,
            self.target_header.?.past.releases[0..self.target_header.?.past.count],
            release_target.value,
        ) != null;

        assert(release_target_current != release_target_past);

        // The trailing newline is intentional - it provides visual separation in the logs when
        // exec'ing new versions.
        if (release_target_current) {
            log.info("executing current release {} via {s}...\n", .{
                release_target,
                self.exe_path,
            });
        } else if (release_target_past) {
            log.info("executing current release {} (target: {}) via {s}...\n", .{
                Release{ .value = self.target_header.?.current_release },
                release_target,
                self.exe_path,
            });
        }
        try self.exec_target_fd();
    }

    /// exec_release is called before a replica is fully open, but just after it has transitioned to
    /// static. Therefore, standard `os.read` blocking syscalls are available.
    /// (in any case, using blocking IO on a memfd on Linux is safe.)
    pub fn exec_release(self: *Multiversion, release_target: Release) !noreturn {
        // exec_release uses self.source_buffer, but this may be the target of an async read by
        // the kernel (from binary_open_callback). Assert that timeouts are not running, and
        // multiversioning is ready to ensure this can't be the case.
        assert(!self.timeout.ticking);
        assert(self.stage == .ready);

        const header = &self.target_header.?;

        if (header.current_release == constants.config.process.release.value) {
            // Normally if we are downgrading, it means that we are running the newest release
            // in the list of bundled releases.
            assert(constants.config.process.release.value ==
                self.releases_bundled.get(self.releases_bundled.count() - 1).value);
        } else {
            // Scenario:
            // 1. Replica starts on release A.
            // 2. Replica detects that its binary has been replaced by B.
            //    It reads the binary of B into a memfd.
            // 3. Replica decides to upgrade to B, so it exec()'s the memfd.
            // 4. (Swap B's binary on disk with C.)
            // 5. Replica starts up, running B's binary.
            // 6. During open, replica reads the binary's header from disk.
            // But that's C's binary/header, so B is unexpectedly not the latest release in it.
            log.warn("binary changed unexpectedly (expected={} found={})", .{
                constants.config.process.release,
                Release{ .value = header.current_release },
            });

            assert(constants.config.process.release.value !=
                self.releases_bundled.get(self.releases_bundled.count() - 1).value);
        }

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

        const target_file = std.fs.File{ .handle = self.target_fd };

        // Our target release is physically embedded in the binary. Shuffle the bytes
        // around, so that it's at the start, then truncate the descriptor so there's nothing
        // trailing.
        const bytes_read = try target_file.preadAll(
            self.source_buffer[0..binary_size],
            self.target_body_offset.? + binary_offset,
        );
        assert(bytes_read == binary_size);

        try target_file.pwriteAll(self.source_buffer[0..binary_size], 0);

        // Zero the remaining bytes in the file.
        try posix.ftruncate(self.target_fd, binary_size);

        // Ensure the checksum matches the header. This could have been done above, but
        // do it in a separate step to make sure.
        const written_checksum = blk: {
            const bytes_read_for_checksum = try target_file.preadAll(
                self.source_buffer[0..binary_size],
                0,
            );

            assert(bytes_read_for_checksum == binary_size);
            break :blk checksum.checksum(self.source_buffer[0..binary_size]);
        };
        assert(written_checksum == binary_checksum);

        // The trailing newline is intentional - it provides visual separation in the logs when
        // exec'ing new versions.
        log.info("executing internal release {} via {s}...\n", .{
            release_target,
            self.exe_path,
        });
        try self.exec_target_fd();
    }

    fn exec_target_fd(self: *Multiversion) !noreturn {
        switch (builtin.os.tag) {
            .linux => {
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
            .macos => {
                std.posix.execveZ(self.target_path, self.args_envp.args, self.args_envp.envp) catch
                    return error.ExecveZFailed;

                unreachable;
            },
            .windows => {
                // Includes the null byte, that utf8ToUtf16LeAllocZ needs.
                var buffer: [std.fs.max_path_bytes]u8 = undefined;
                var fixed_allocator = std.heap.FixedBufferAllocator.init(&buffer);
                const allocator = fixed_allocator.allocator();

                const target_path_w = std.unicode.utf8ToUtf16LeAllocZ(
                    allocator,
                    self.target_path,
                ) catch unreachable;
                defer allocator.free(target_path_w);

                // "The Unicode version of this function, CreateProcessW, can modify the contents of
                // this string. Therefore, this parameter cannot be a pointer to read-only memory
                // (such as a const variable or a literal string). If this parameter is a constant
                // string, the function may cause an access violation."
                //
                // That said, with how CreateProcessW is called, this should _never_ happen, since
                // its both provided a full lpApplicationName, and because GetCommandLineW actually
                // points to a copy of memory from the PEB.
                const get_command_line_w = @extern(
                    *const fn () callconv(.C) std.os.windows.LPWSTR,
                    .{
                        .library_name = "kernel32",
                        .name = "GetCommandLineW",
                    },
                );
                const cmd_line_w = get_command_line_w();

                var lp_startup_info = std.mem.zeroes(std.os.windows.STARTUPINFOW);
                lp_startup_info.cb = @sizeOf(std.os.windows.STARTUPINFOW);

                var lp_process_information: std.os.windows.PROCESS_INFORMATION = undefined;

                // Close the handle before trying to execute.
                posix.close(self.target_fd);

                // If bInheritHandles is FALSE, and dwFlags inside STARTUPINFOW doesn't have
                // STARTF_USESTDHANDLES set, the stdin/stdout/stderr handles of the parent will
                // be passed through to the child.
                std.os.windows.CreateProcessW(
                    target_path_w,
                    cmd_line_w,
                    null,
                    null,
                    std.os.windows.FALSE,
                    std.os.windows.CREATE_UNICODE_ENVIRONMENT,
                    null,
                    null,
                    &lp_startup_info,
                    &lp_process_information,
                ) catch return error.CreateProcessWFailed;
                std.process.exit(0);
            },
            else => @panic("unsupported platform"),
        }
    }
};

pub fn self_exe_path(allocator: std.mem.Allocator) ![:0]const u8 {
    var buf: [std.fs.max_path_bytes]u8 = undefined;

    if (builtin.target.os.tag == .windows) {
        // Special case: Wine doesn't support selfExePath.
        const ntdll = os.windows.kernel32.GetModuleHandleW(
            std.unicode.utf8ToUtf16LeStringLiteral("ntdll.dll"),
        ).?;
        const wine_get_version = os.windows.kernel32.GetProcAddress(ntdll, "wine_get_version");

        if (wine_get_version != null) {
            log.warn("wine doesn't support std.fs.selfExePath", .{});
            return allocator.dupeZ(u8, "");
        }
    }

    const native_self_exe_path = try std.fs.selfExePath(&buf);

    if (builtin.target.os.tag == .linux and std.mem.eql(
        u8,
        native_self_exe_path,
        "/memfd:" ++ multiversion_uuid ++ " (deleted)",
    )) {
        // Technically, "/memfd:tigerbeetle-multiversion-... (deleted)" is a valid path at which you
        // could place your binary - please don't!
        assert(std.fs.cwd().statFile(native_self_exe_path) catch null == null);

        // Running from a memfd already; the real path is argv[0].
        const path = try allocator.dupeZ(u8, std.mem.span(os.argv[0]));
        assert(std.fs.path.isAbsolute(path));

        return path;
    } else if (std.mem.indexOf(u8, native_self_exe_path, multiversion_uuid) != null) {
        assert(builtin.target.os.tag == .windows or builtin.target.os.tag == .macos);
        // Similar to above, you _could_ call your binary "tigerbeetle-multiversion-...". This can't
        // be checked with an assert unfortunately.

        // Running from a temp path already; the real path is argv[0].
        var arg_iterator = try std.process.argsWithAllocator(allocator);
        defer arg_iterator.deinit();

        const path = arg_iterator.next().?;
        assert(std.fs.path.isAbsolute(path));

        return try allocator.dupeZ(u8, path);
    } else {
        // Not running from a memfd or temp path. `native_self_exe_path` is the real path.
        return try allocator.dupeZ(u8, native_self_exe_path);
    }
}

const HeaderBodyOffsets = struct {
    const Offsets = struct {
        header_offset: u32,
        body_offset: u32,
        body_size: u32,
    };

    format: enum { elf, pe, macho },
    aarch64: ?Offsets,
    x86_64: ?Offsets,

    fn active(header_body_offsets: HeaderBodyOffsets) ?Offsets {
        return switch (builtin.target.cpu.arch) {
            .x86_64 => header_body_offsets.x86_64,
            .aarch64 => header_body_offsets.aarch64,
            else => comptime unreachable,
        };
    }

    fn inactive(header_body_offsets: HeaderBodyOffsets) ?Offsets {
        return switch (builtin.target.cpu.arch) {
            .x86_64 => header_body_offsets.aarch64,
            .aarch64 => header_body_offsets.x86_64,
            else => comptime unreachable,
        };
    }
};

/// Parse an untrusted, unverified, and potentially corrupt ELF file. This parsing happens before
/// any checksums are verified, and so needs to deal with any ELF metadata being corrupt, while
/// not panicking and returning errors.
///
/// Anything that would normally assert should return an error instead - especially implicit things
/// like bounds checking on slices.
pub fn parse_elf(buffer: []align(@alignOf(elf.Elf64_Ehdr)) const u8) !HeaderBodyOffsets {
    if (@sizeOf(elf.Elf64_Ehdr) > buffer.len) return error.InvalidELF;
    const elf_header = try elf.Header.parse(buffer[0..@sizeOf(elf.Elf64_Ehdr)]);

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

    const offsets: HeaderBodyOffsets.Offsets = .{
        .header_offset = header_offset.?,
        .body_offset = body_offset.?,
        .body_size = body_size.?,
    };
    const arch = elf_header.machine.toTargetCpuArch() orelse
        return error.UnknownArchitecture;
    return switch (arch) {
        .aarch64 => .{ .format = .elf, .aarch64 = offsets, .x86_64 = null },
        .x86_64 => .{ .format = .elf, .aarch64 = null, .x86_64 = offsets },
        else => return error.UnknownArchitecture,
    };
}

pub fn parse_macho(buffer: []const u8) !HeaderBodyOffsets {
    if (@sizeOf(std.macho.fat_header) > buffer.len) return error.InvalidMacho;
    const fat_header = std.mem.bytesAsValue(
        std.macho.fat_header,
        buffer[0..@sizeOf(std.macho.fat_header)],
    );
    if (fat_header.magic != std.macho.FAT_CIGAM) return error.InvalidMachoMagic;
    if (@byteSwap(fat_header.nfat_arch) != 6) return error.InvalidMachoArches;

    var header_offset_aarch64: ?u32 = null;
    var header_offset_x86_64: ?u32 = null;
    var body_offset_aarch64: ?u32 = null;
    var body_offset_x86_64: ?u32 = null;
    var body_size_aarch64: ?u32 = null;
    var body_size_x86_64: ?u32 = null;
    for (0..6) |i| {
        const offset = @sizeOf(std.macho.fat_header) + @sizeOf(std.macho.fat_arch) * i;
        if (offset + @sizeOf(std.macho.fat_arch) > buffer.len) return error.InvalidMacho;
        const fat_arch = std.mem.bytesAsValue(
            std.macho.fat_arch,
            buffer[offset..][0..@sizeOf(std.macho.fat_arch)],
        );
        const fat_arch_cpu_type = @byteSwap(fat_arch.cputype);

        switch (fat_arch_cpu_type) {
            @intFromEnum(section_to_macho_cpu.tb_mvb_aarch64) => {
                assert(body_offset_aarch64 == null and body_size_aarch64 == null);
                body_offset_aarch64 = @byteSwap(fat_arch.offset);
                body_size_aarch64 = @byteSwap(fat_arch.size);
            },
            @intFromEnum(section_to_macho_cpu.tb_mvh_aarch64) => {
                assert(header_offset_aarch64 == null);
                header_offset_aarch64 = @byteSwap(fat_arch.offset);
            },
            @intFromEnum(section_to_macho_cpu.tb_mvb_x86_64) => {
                assert(body_offset_x86_64 == null and body_size_x86_64 == null);
                body_offset_x86_64 = @byteSwap(fat_arch.offset);
                body_size_x86_64 = @byteSwap(fat_arch.size);
            },
            @intFromEnum(section_to_macho_cpu.tb_mvh_x86_64) => {
                assert(header_offset_x86_64 == null);
                header_offset_x86_64 = @byteSwap(fat_arch.offset);
            },
            else => {},
        }
    }

    if (header_offset_aarch64 == null or body_offset_aarch64 == null) {
        return error.MultiversionHeaderOrBodyNotFound;
    }

    if (header_offset_x86_64 == null or body_offset_x86_64 == null) {
        return error.MultiversionHeaderOrBodyNotFound;
    }

    if (body_offset_aarch64.? + body_size_aarch64.? > header_offset_aarch64.?) {
        return error.MultiversionBodyOverlapsHeader;
    }

    if (body_offset_x86_64.? + body_size_x86_64.? > header_offset_x86_64.?) {
        return error.MultiversionBodyOverlapsHeader;
    }

    return .{
        .format = .macho,
        .aarch64 = .{
            .header_offset = header_offset_aarch64.?,
            .body_offset = body_offset_aarch64.?,
            .body_size = body_size_aarch64.?,
        },
        .x86_64 = .{
            .header_offset = header_offset_x86_64.?,
            .body_offset = body_offset_x86_64.?,
            .body_size = body_size_x86_64.?,
        },
    };
}

pub fn parse_pe(buffer: []const u8) !HeaderBodyOffsets {
    const coff = try std.coff.Coff.init(buffer, false);

    if (!coff.is_image) return error.InvalidPE;

    const header_section = coff.getSectionByName(".tb_mvh");
    const body_section = coff.getSectionByName(".tb_mvb");

    if (header_section == null) return error.MultiversionHeaderOrBodyNotFound;
    if (body_section == null) return error.MultiversionHeaderOrBodyNotFound;

    const header_offset = header_section.?.pointer_to_raw_data;
    const body_offset = body_section.?.pointer_to_raw_data;
    const body_size = body_section.?.size_of_raw_data;

    if (body_offset + body_size > header_offset) {
        return error.MultiversionBodyOverlapsHeader;
    }

    const offsets: HeaderBodyOffsets.Offsets = .{
        .header_offset = header_offset,
        .body_offset = body_offset,
        .body_size = body_size,
    };

    const arch = coff.getCoffHeader().machine.toTargetCpuArch() orelse
        return error.UnknownArchitecture;
    return switch (arch) {
        .aarch64 => .{ .format = .pe, .aarch64 = offsets, .x86_64 = null },
        .x86_64 => .{ .format = .pe, .aarch64 = null, .x86_64 = offsets },
        else => return error.UnknownArchitecture,
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

    elf_header.e_machine = elf.EM.X86_64;
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
test parse_elf {
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

        assert(parsed.x86_64.?.body_offset == 16384);
        assert(parsed.x86_64.?.header_offset == 24576);
    }
}

pub fn print_information(
    allocator: std.mem.Allocator,
    exe_path: [:0]const u8,
    output: std.io.AnyWriter,
) !void {
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
        .detect,
    );
    defer multiversion.deinit(allocator);

    multiversion.open_sync() catch |err| {
        try output.print("multiversioning not enabled: {}\n", .{err});
        return err;
    };

    assert(multiversion.stage == .ready);

    try output.print("multiversioning.exe_path={s}\n", .{exe_path});
    try output.print("multiversioning.absolute_exe_path={s}\n", .{absolute_exe_path});

    const header = multiversion.target_header.?;

    // `source_buffer` contains the same data as `target_file` - this code doesn't update anything
    // after the initial open_sync().
    const target_body_size = multiversion.target_body_size.?; // Line length limits.
    try header.past.verify_checksums(
        multiversion.source_buffer[multiversion.target_body_offset.?..][0..target_body_size],
    );

    try output.print(
        "multiversioning.releases_bundled={any}\n",
        .{multiversion.releases_bundled.const_slice()},
    );

    inline for (comptime std.meta.fieldNames(MultiversionHeader)) |field| {
        if (std.mem.eql(u8, field, "current_git_commit")) {
            try output.print("multiversioning.header.{s}={s}\n", .{
                field,
                std.fmt.fmtSliceHexLower(&header.current_git_commit),
            });
        } else if (!std.mem.eql(u8, field, "past") and
            !std.mem.eql(u8, field, "current_flags_padding") and
            !std.mem.eql(u8, field, "past_padding") and
            !std.mem.eql(u8, field, "reserved"))
        {
            try output.print("multiversioning.header.{s}={any}\n", .{
                field,
                if (comptime std.mem.eql(u8, field, "current_release"))
                    Release{ .value = @field(header, field) }
                else
                    @field(header, field),
            });
        }
    }

    try std.fmt.format(
        output,
        "multiversioning.header.past.count={}\n",
        .{header.past.count},
    );
    inline for (comptime std.meta.fieldNames(MultiversionHeader.PastReleases)) |field| {
        if ((comptime std.mem.eql(u8, field, "releases")) or
            (comptime std.mem.eql(u8, field, "release_client_mins")))
        {
            var release_list: ReleaseList = .{};
            for (@field(header.past, field)[0..header.past.count]) |release| {
                release_list.append_assume_capacity(Release{ .value = release });
            }

            try output.print("multiversioning.header.past.{s}={any}\n", .{
                field,
                release_list.const_slice(),
            });
        } else if (comptime std.mem.eql(u8, field, "git_commits")) {
            for (@field(header.past, field)[0..header.past.count], 0..) |*git_commit, i| {
                try output.print("multiversioning.header.past.{s}.{}={}\n", .{
                    field,
                    Release{ .value = header.past.releases[i] },
                    std.fmt.fmtSliceHexLower(git_commit),
                });
            }
        } else if ((comptime std.mem.eql(u8, field, "checksums")) or
            (comptime std.mem.eql(u8, field, "flags")))
        {
            for (@field(header.past, field)[0..header.past.count], 0..) |value, i| {
                try output.print("multiversioning.header.past.{s}.{}={}\n", .{
                    field,
                    Release{ .value = header.past.releases[i] },
                    value,
                });
            }
        } else if (comptime (!std.mem.eql(u8, field, "count") and
            !std.mem.eql(u8, field, "flags_padding")))
        {
            try output.print("multiversioning.header.past.{s}={any}\n", .{
                field,
                @field(header.past, field)[0..header.past.count],
            });
        }
    }
}

/// This is not exhaustive, but should be good enough for 99.95% of the modern systems we support.
/// Caller owns returned memory.
fn system_temporary_directory(allocator: std.mem.Allocator) ![]const u8 {
    switch (builtin.os.tag) {
        .linux, .macos => {
            return std.process.getEnvVarOwned(allocator, "TMPDIR") catch allocator.dupe(u8, "/tmp");
        },
        .windows => {
            return std.process.getEnvVarOwned(allocator, "TMP") catch
                std.process.getEnvVarOwned(allocator, "TEMP") catch
                allocator.dupe(u8, "C:\\Windows\\Temp");
        },
        else => @panic("unsupported platform"),
    }
}
