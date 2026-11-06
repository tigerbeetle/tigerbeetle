const std = @import("std");
const FRNG = @import("./FRNG.zig");
const assert = std.debug.assert;

const MiB = 1 << 20;

const log = std.log;

pub const std_options: std.Options = .{
    .log_level = .debug,
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    const buffer = try gpa.alloc(u8, 4 * MiB);
    defer gpa.free(buffer);

    var stdin_reader = std.Io.File.stdin().reader(io, &.{});
    const entropy = try stdin_reader.interface.allocRemaining(gpa, .unlimited);
    defer gpa.free(entropy);

    var frng = FRNG.init(entropy);

    var world: World = try .init(gpa, &frng, .{ .journal_count_max = 16 });
    defer world.deinit(gpa);

    world.run();
}

const World = struct {
    frng: *FRNG,
    replicas: [replica_count]Replica,
    replica_dead: [replica_count]bool = @splat(false),
    journal: []Entry,

    fn init(gpa: std.mem.Allocator, frng: *FRNG, options: struct {
        journal_count_max: u64,
    }) !World {
        const journal = try gpa.alloc(Entry, options.journal_count_max);
        errdefer gpa.free(journal);

        @memset(journal, .reserved);

        var world: World = .{
            .frng = frng,
            .replicas = undefined,
            .journal = journal,
        };

        for (&world.replicas, 0..) |*replica, replica_index| {
            replica.* = Replica.init(@intCast(replica_index));
        }
        return world;
    }

    fn deinit(world: *World, gpa: std.mem.Allocator) void {
        gpa.free(world.journal);
        world.* = undefined;
    }

    fn run(world: *World) void {
        world.run_safety() catch |e| switch (e) {
            error.OutOfEntropy => {},
        };
        world.run_liveness();
    }

    fn run_safety(world: *World) !void {
        while (true) {
            try world.step();
            world.check();
        }
    }

    fn run_liveness(world: *World) void {
        var op_max: u64 = 1;
        for (0..1_000_000) |index| {
            world.replica_request(index % replica_count, index);
            world.check();
            while (op_max + 1 < world.journal.len and
                !world.journal[op_max + 1].is_reserved())
            {
                op_max += 1;
            }
            if (op_max + 1 == world.journal.len) {
                break;
            }
        } else @panic("dead X_X");
    }

    fn check(world: *World) void {
        for (&world.replicas) |*replica| {
            const committed_last = replica.local().committed_last;
            assert(!committed_last.is_reserved());

            if (committed_last.op >= world.journal.len) continue;

            if (world.journal[committed_last.op].is_reserved()) {
                world.journal[committed_last.op] = committed_last;
                world.check_hash_link(committed_last.op);
                world.check_hash_link(committed_last.op + 1);
            } else {
                assert(std.meta.eql(
                    world.journal[committed_last.op],
                    committed_last,
                ));
            }
        }
    }

    fn check_hash_link(world: *World, index: u64) void {
        if (index == 0) return;
        if (index >= world.journal.len) return;
        if (world.journal[index - 1].is_reserved()) return;
        if (world.journal[index].is_reserved()) return;
        assert(world.journal[index - 1].checksum == world.journal[index].parent);
    }

    pub fn step(world: *World) !void {
        const action = try world.frng.weighted(.{
            .request = 1,
            .kill = 1,
            .message = 1,
            .view_change = 1,
            .persist = 1,
        });
        switch (action) {
            .request => {
                try world.request();
            },
            .message => {},
            .kill => try world.kill(),
            .view_change => try world.view_change(),
            .persist => try world.persist(),
        }
    }

    fn persist(world: *World) !void {
        const replica = try world.frng.index(world.replicas);
        try world.replicas[replica].local().pipeline.persist(world.frng);
        world.replicas[replica].commit();
    }

    fn request(world: *World) !void {
        const replica = try world.frng.index(world.replicas);
        const payload = try world.frng.int(u128);
        world.replica_request(replica, payload);
    }

    fn replica_request(world: *World, replica: usize, payload: u128) void {
        if (world.replica_dead[replica]) {
            return;
        }
        log.debug("request: replica={} payload={}", .{ replica, payload });
        world.replicas[replica].on_request(payload);
    }

    fn kill(world: *World) !void {
        const replica = try world.frng.index(world.replicas);
        world.replica_dead[replica] = !world.replica_dead[replica];
        const dead_or_alive = if (world.replica_dead[replica]) "dead" else "alive";
        log.debug("kill: {} is {s}", .{ replica, dead_or_alive });
    }

    fn view_change(world: *World) !void {
        const replica = try world.frng.index(world.replicas);
        world.replicas[replica].view_change();
        log.debug("view_change: replica={} view_max={}", .{
            replica,
            world.replicas[replica].local().view_max,
        });
    }
};

const pipeline_max: u64 = 2;

const Pipeline = struct {
    entry: [pipeline_max]Entry = @splat(.reserved),
    ok: [pipeline_max]bool = @splat(false),
    op_base: u64 = 2,

    fn get(pipeline: *Pipeline, op: u64) ?*Entry {
        const index = pipeline.op_to_index(op) orelse return null;
        if (pipeline.entry[index].is_reserved()) {
            assert(!pipeline.ok[index]);
            return null;
        }
        return &pipeline.entry[index];
    }

    pub fn op_head(pipeline: *const Pipeline) u64 {
        var op: ?u64 = null;
        for (&pipeline.entry, 0..) |*entry, entry_index| {
            if (!entry.is_reserved()) {
                assert(entry.op == pipeline.op_base + entry_index);
                assert(op == null or op.? < entry.op);
                op = entry.op;
            }
        }
        return op orelse pipeline.op_base;
    }

    pub fn full(pipeline: *const Pipeline) bool {
        return !pipeline.entry[pipeline.entry.len - 1].is_reserved();
    }

    pub fn push(pipeline: *Pipeline, entry: Entry) void {
        assert(!pipeline.full());
        assert(entry.op == pipeline.op_head() + 1);
        assert(pipeline.get(entry.op) == null);
        const index = pipeline.op_to_index(entry.op).?;
        pipeline.entry[index] = entry;
        assert(pipeline.get(entry.op).?.checksum == entry.checksum);
    }

    fn op_to_index(pipeline: *Pipeline, op: u64) ?u64 {
        if (op < pipeline.op_base) return null;
        const index = op - pipeline.op_base;
        return if (index < pipeline.entry.len) index else null;
    }

    fn persist(pipeline: *Pipeline, prng: *FRNG) !void {
        const index = try prng.index(pipeline.entry);
        if (!pipeline.entry[index].is_reserved()) {
            pipeline.ok[index] = true;
        }
    }

    fn is_ok(pipeline: *Pipeline, op: u64) bool {
        const index = pipeline.op_to_index(op) orelse return false;
        return pipeline.ok[index];
    }

    fn pop(pipeline: *Pipeline) Entry {
        const entry = pipeline.entry[0];
        assert(!entry.is_reserved());
        assert(pipeline.op_base == entry.op);

        pipeline.entry[0] = .reserved;
        pipeline.ok[0] = false;
        std.mem.rotate(Entry, &pipeline.entry, 1);
        std.mem.rotate(bool, &pipeline.ok, 1);

        pipeline.op_base += 1;
        assert(pipeline.op_base == entry.op + 1);
        return entry;
    }

    pub fn format(
        pipeline: @This(),
        writer: *std.Io.Writer,
    ) std.Io.Writer.Error!void {
        try writer.print("base={}", .{pipeline.op_base});
        for (pipeline.entry) |entry| {
            try writer.print(" {f}", .{entry});
        }
        try writer.print("\n", .{});
    }
};

const Replica = struct {
    state: GlobalSate,
    replica: u8,

    fn init(replica_index: u8) Replica {
        return .{
            .replica = replica_index,
            .state = @splat(.{}),
        };
    }

    fn local(replica: *Replica) *LocalState {
        return &replica.state[replica.replica];
    }

    fn on_request(replica: *Replica, payload: u128) void {
        if (!replica.primary()) {
            return;
        }
        if (replica.local().pipeline.full()) {
            return;
        }

        const op_head = replica.local().pipeline.op_head();

        const parent_checksum = (replica.local().pipeline.get(op_head) orelse
            &replica.local().committed_last).checksum;

        var entry: Entry = .{
            .op = op_head + 1,
            .view = replica.local().view_max,
            .checksum = 0,
            .parent = parent_checksum,
            .payload = payload,
        };
        entry.set_checksum();
        replica.local().pipeline.push(entry);
        assert(replica.local().pipeline.op_head() == op_head + 1);
    }

    fn commit(replica: *Replica) void {
        // commit only in order
        var ok_count: u8 = 0;
        const op = replica.local().committed_last.op + 1;
        for (&replica.state) |*peer| {
            ok_count += @intFromBool(peer.pipeline.is_ok(op));
        }
        if (ok_count >= quorum_replication) {
            const entry = replica.local().pipeline.pop();
            log.debug("{}: commit: {f}", .{ replica, entry });
            assert(!entry.is_reserved());
            replica.local().committed_last = entry;
        }
    }

    fn on_message(replica: *Replica, message: Message) void {
        _ = replica; // autofix
        _ = message; // autofix
    }

    fn view_change(replica: *Replica) void {
        replica.local().view_max += 1;
    }

    fn primary(replica: *Replica) bool {
        return replica.replica == (replica.local().view_max % replica_count);
    }
};

// State of the single replica
const LocalState = struct {
    committed_last: Entry = .root,
    pipeline: Pipeline = .{},
    view_min: u64 = 0,
    view_max: u64 = 1,
};

const GlobalSate = [replica_count]LocalState;

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

    pub fn format(entry: *const Entry, writer: *std.Io.Writer) !void {
        if (entry.is_reserved()) {
            try writer.print("_", .{});
        } else {
            try writer.print("{}/{}={}", .{ entry.op, entry.view, entry.checksum });
        }
    }

    fn set_checksum(entry: *Entry) void {
        assert(entry.checksum == 0);
        const checksum = std.hash.CityHash64.hash(std.mem.asBytes(entry));
        entry.checksum = checksum;
    }
};

const replica_count: u8 = 6;
const quorum_replication = 3;

const Message = struct {
    state: GlobalSate,
};
