const std = @import("std");
const assert = std.debug.assert;

entropy: []const u8,

pub const Error = error{OutOfEntropy};

const FRNG = @This();

pub fn init(entropy: []const u8) FRNG {
    return .{ .entropy = entropy };
}

pub fn bytes(frng: *FRNG, size: usize) Error![]const u8 {
    if (frng.entropy.len < size) return error.OutOfEntropy;
    defer frng.entropy = frng.entropy[size..];

    return frng.entropy[0..size];
}

pub fn array(frng: *FRNG, comptime size: usize) Error![size]u8 {
    return (try frng.bytes(size))[0..size].*;
}

pub fn int(frng: *FRNG, Int: type) Error!Int {
    comptime assert(@typeInfo(Int).int.signedness == .unsigned);
    comptime assert(@import("builtin").target.cpu.arch.endian() == .little);
    return @bitCast(try frng.array(@sizeOf(Int)));
}

pub fn boolean(frng: *FRNG) Error!bool {
    return (try frng.int(u8)) & 1 == 1;
}

pub fn int_inclusive(frng: *FRNG, Int: type, max: Int) Error!Int {
    comptime assert(@typeInfo(Int).int.signedness == .unsigned);
    if (max == std.math.maxInt(Int)) return try frng.int(Int);

    const bits = @typeInfo(Int).int.bits;
    const less_than = max + 1;

    var x = try frng.int(Int);
    var m = std.math.mulWide(Int, x, less_than);
    var l: Int = @truncate(m);
    if (l < less_than) {
        var t = -%less_than;

        if (t >= less_than) {
            t -= less_than;
            if (t >= less_than) t %= less_than;
        }
        while (l < t) {
            x = try frng.int(Int);
            m = std.math.mulWide(Int, x, less_than);
            l = @truncate(m);
        }
    }
    return @intCast(m >> bits);
}

pub fn range_inclusive(frng: *FRNG, Int: type, min: Int, max: Int) Error!Int {
    comptime assert(@typeInfo(Int).int.signedness == .unsigned);
    assert(min <= max);
    return min + try frng.int_inclusive(Int, max - min);
}

pub fn index(frng: *FRNG, slice: anytype) Error!usize {
    assert(slice.len > 0);
    return try frng.range_inclusive(usize, 0, slice.len - 1);
}

pub fn swarm_weights(frng: *FRNG, Weights: type) Error!Weights {
    var result: Weights = undefined;
    inline for (comptime std.meta.fieldNames(Weights)) |field| {
        @field(result, field) = try frng.range_inclusive(u32, 1, 100);
    }
    return result;
}

pub fn weighted(frng: *FRNG, weights: anytype) Error!std.meta.FieldEnum(@TypeOf(weights)) {
    var weight: u32 = 0;
    inline for (comptime std.meta.fieldNames(@TypeOf(weights))) |field| {
        weight += @field(weights, field);
    }
    assert(weight > 0);
    weight = try frng.int_inclusive(u32, weight - 1);
    inline for (comptime std.meta.fieldNames(@TypeOf(weights))) |field| {
        weight -|= @field(weights, field);
        if (weight == 0) {
            return @field(
                std.meta.FieldEnum(@TypeOf(weights)),
                field,
            );
        }
    }
    unreachable;
}

pub const Driver = struct {
    io: std.Io,
    sut: []const u8,
    buffer: []u8,

    const log = std.log;

    pub fn main(
        gpa: std.mem.Allocator,
        io: std.Io,
        sut: []const u8,
        operation: union(enum) {
            replay: struct { size: u32, seed: u64 },
            search: struct {
                attempts: u32 = 100,
                size_max: u32 = 4 * 1024 * 1024,
            },
        },
    ) !void {
        const size_max = switch (operation) {
            .replay => |options| options.size,
            .search => |options| options.size_max,
        };

        const buffer = try gpa.alloc(u8, size_max);
        defer gpa.free(buffer);

        var driver: Driver = .{
            .io = io,
            .buffer = buffer,
            .sut = sut,
        };

        switch (operation) {
            .replay => |options| {
                const outcome = try driver.run_once(.{
                    .size = options.size,
                    .seed = options.seed,
                    .quiet = false,
                });
                log.info("{t}", .{outcome});
            },
            .search => |options| {
                const outcome = try driver.search(.{ .attempts = options.attempts });
                switch (outcome) {
                    .pass => log.info("ok", .{}),
                    .fail => |fail| {
                        log.err("minimized size={} seed={}", .{ fail.size, fail.seed });
                    },
                }
            },
        }
    }

    fn search(driver: Driver, options: struct {
        attempts: u32 = 100,
    }) !union(enum) { pass, fail: struct { size: u32, seed: u64 } } {
        var found_size: ?u32 = null;
        var found_seed: ?u64 = null;

        var pass: bool = true;
        var size: u32 = 16;
        var step: u32 = 16;
        for (0..1024) |_| {
            if (step == 0) break;

            const size_next = if (pass) size + step else size - step;
            if (size_next > driver.buffer.len) break;
            const outcome = try driver.run_multiple(.{
                .size = size_next,
                .attempts = options.attempts,
            });
            switch (outcome) {
                .pass => log.info("pass: size={}", .{size_next}),
                .fail => |seed| {
                    found_size = size_next;
                    found_seed = seed;
                    log.err("fail: size={} seed={}", .{ size_next, seed });
                },
            }
            const pass_next = (outcome == .pass);

            if (pass and pass_next) {
                step *= 2;
            } else if (!pass and !pass_next) {
                // Keep the step.
            } else {
                step /= 2;
            }

            if (pass or !pass_next) {
                size = size_next;
                pass = pass_next;
            }
        } else @panic("safety counter");

        if (found_size == null) return .pass;
        return .{ .fail = .{
            .size = found_size.?,
            .seed = found_seed.?,
        } };
    }

    fn run_multiple(driver: Driver, options: struct {
        size: u32,
        attempts: u32,
    }) !union(enum) { pass, fail: u64 } {
        assert(options.size <= driver.buffer.len);

        for (0..options.attempts) |_| {
            var seed: u64 = undefined;
            driver.io.random(@ptrCast(&seed));

            const outcome = try driver.run_once(.{
                .seed = seed,
                .size = options.size,
                .quiet = true,
            });
            switch (outcome) {
                .fail => return .{ .fail = seed },
                .pass => {},
            }
        }
        return .pass;
    }

    fn run_once(driver: Driver, options: struct {
        size: u32,
        seed: u64,
        quiet: bool,
    }) !enum { pass, fail } {
        assert(options.size <= driver.buffer.len);
        const entropy = driver.buffer[0..options.size];

        var child = try std.process.spawn(driver.io, .{
            .argv = &.{driver.sut},
            .stdin = .pipe,
            .stderr = if (options.quiet) .ignore else .inherit,
        });

        var rng_instance: std.Random.DefaultPrng = .init(options.seed);
        var rng = rng_instance.random();

        rng.bytes(entropy);

        try child.stdin.?.writeStreamingAll(driver.io, entropy);
        child.stdin.?.close(driver.io);
        child.stdin = null;

        const term = try child.wait(driver.io);
        return if (success(term)) .pass else .fail;
    }

    fn success(term: std.process.Child.Term) bool {
        return term == .exited and term.exited == 0;
    }
};
