const std = @import("std");
const FRNG = @import("./FRNG.zig");
const assert = std.debug.assert;

const MiB = 1 << 20;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    const buffer = try gpa.alloc(u8, 4 * MiB);
    defer gpa.free(buffer);

    var stdin_reader = std.Io.File.stdin().reader(io, &.{});
    const entropy = try stdin_reader.interface.allocRemaining(gpa, .unlimited);
    defer gpa.free(entropy);

    var frng = FRNG.init(entropy);

    var world: World = try .init(gpa, &frng, .{ .log_count_max = 16 });
    defer world.deinit(gpa);

    world.run();
}

const World = struct {
    frng: *FRNG,
    replicas: [replica_count]Replica,
    log: []Entry,

    fn init(gpa: std.mem.Allocator, frng: *FRNG, options: struct {
        log_count_max: u64,
    }) !World {
        const log = try gpa.alloc(Entry, options.log_count_max);
        errdefer gpa.free(log);

        @memset(log, .reserved);

        return .{
            .frng = frng,
            .replicas = @splat(.{}),
            .log = log,
        };
    }

    fn deinit(world: *World, gpa: std.mem.Allocator) void {
        gpa.free(world.log);
        world.* = undefined;
    }

    fn run(world: *World) void {
        while (true) {
            world.step() catch |e| switch (e) {
                error.OutOfEntropy => break,
            };
            world.check();
        }
    }

    fn check(world: *World) void {
        for (world.replicas) |replica| {
            if (replica.committed_last.op >= world.log.len) continue;

            if (world.log[replica.committed_last.op].is_reserved()) {
                world.log[replica.committed_last.op] = replica.committed_last;
                world.check_hash_link(replica.committed_last.op);
                world.check_hash_link(replica.committed_last.op + 1);
            } else {
                assert(std.meta.eql(
                    world.log[replica.committed_last.op],
                    replica.committed_last,
                ));
            }
        }
    }

    fn check_hash_link(world: *World, index: u64) void {
        if (index == 0) return;
        if (index >= world.log.len) return;
        if (world.log[index - 1].is_reserved()) return;
        if (world.log[index].is_reserved()) return;
        assert(world.log[index - 1].checksum == world.log[index].parent);
    }

    pub fn step(world: *World) !void {
        switch (try world.frng.weighted(.{ .request = 1, .message = 2 })) {
            .request => {
                try world.request();
            },
            .message => {},
        }
    }

    fn request(world: *World) !void {
        const replica_index = try world.frng.index(world.replicas);
        const payload = try world.frng.int(u128);

        world.replicas[replica_index].on_request(payload);
    }
};

const pipeline_max: u64 = 2;

const Replica = struct {
    pipeline: [pipeline_max]Entry = @splat(.reserved),
    committed_last: Entry = .root,
    view_min: u64 = 0,
    view_max: u64 = 1,

    fn on_request(replica: *Replica, payload: u128) void {
        const op = replica.committed_last.op + 1;
        const parent = replica.committed_last.checksum;
        replica.committed_last = .{
            .op = op,
            .view = replica.view_max,
            .checksum = 0,
            .parent = parent,
            .payload = payload,
        };
        replica.committed_last.set_checksum();
    }
    fn on_message(replica: *Replica, message: Message) void {
        _ = replica; // autofix
        _ = message; // autofix
    }
};

const Entry = extern struct {
    op: u64,
    view: u64,
    checksum: u128,
    parent: u128,
    payload: u128, // checksum

    pub const root: Entry = b: {
        var entry: Entry = .{
            .op = 1,
            .view = 0,
            .checksum = 0,
            .parent = 0,
            .payload = 0,
        };
        entry.set_checksum();
        break :b entry;
    };
    pub const reserved: Entry = .{
        .op = 0,
        .view = 0,
        .checksum = 0,
        .parent = 0,
        .payload = 0,
    };

    fn is_reserved(entry: *const Entry) bool {
        if (entry.op == 0) {
            assert(std.meta.eql(
                entry.*,
                .reserved,
            ));
            return true;
        } else return false;
    }

    fn set_checksum(entry: *Entry) void {
        assert(entry.checksum == 0);
        const checksum = std.hash.CityHash64.hash(std.mem.asBytes(entry));
        entry.checksum = checksum;
    }
};

const replica_count: u8 = 6;

const Message = struct {
    pipeline: [replica_count][pipeline_max]Entry,
    replica: u8,
};
