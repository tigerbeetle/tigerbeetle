const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.test_replica);
const expectEqual = std.testing.expectEqual;
const allocator = std.testing.allocator;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Process = @import("../testing/cluster/message_bus.zig").Process;
const Message = @import("../message_pool.zig").MessagePool.Message;
const parse_table = @import("../testing/table.zig").parse;
const StateMachineType = @import("../testing/state_machine.zig").StateMachineType;
const Cluster = @import("../testing/cluster.zig").ClusterType(StateMachineType);
const LinkFilter = @import("../testing/cluster/network.zig").LinkFilter;
const Network = @import("../testing/cluster/network.zig").Network;
const Storage = @import("../testing/storage.zig").Storage;

const slot_count = constants.journal_slot_count;
const checkpoint_1 = vsr.Checkpoint.checkpoint_after(0);
const checkpoint_2 = vsr.Checkpoint.checkpoint_after(checkpoint_1);
const checkpoint_3 = vsr.Checkpoint.checkpoint_after(checkpoint_2);
const checkpoint_1_trigger = vsr.Checkpoint.trigger_for_checkpoint(checkpoint_1).?;
const checkpoint_2_trigger = vsr.Checkpoint.trigger_for_checkpoint(checkpoint_2).?;
const checkpoint_3_trigger = vsr.Checkpoint.trigger_for_checkpoint(checkpoint_3).?;
const log_level = std.log.Level.err;

// TODO Test client eviction once it no longer triggers a client panic.
// TODO Detect when cluster has stabilized and stop run() early, rather than just running for a
//      fixed number of ticks.
// TODO (Maybe:) Lazy-enable (begin ticks) for clients, so that clients don't register via ping,
//      causing unexpected/unaccounted-for commits. Maybe also don't tick clients at all during
//      run(), so that new requests cannot be added "unexpectedly". (This will remove the need for
//      the boilerplate c.request(20) == 20 at the beginning of most tests).
// TODO Many of these would benefit from explicit code coverage marks.

comptime {
    // The tests are written for these configuration values in particular.
    assert(constants.journal_slot_count == 32);
    assert(constants.lsm_batch_multiple == 4);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt right of head)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.R_).stop();
    t.replica(.R0).corrupt(.{ .wal_prepare = 22 });

    // 2/3 can't commit when 1/2 is status=recovering_head.
    try t.replica(.R0).open();
    try expectEqual(t.replica(.R0).status(), .recovering_head);
    try t.replica(.R1).open();
    try c.request(24, 20);
    // With the aid of the last replica, the cluster can recover.
    try t.replica(.R2).open();
    try c.request(24, 24);
    try expectEqual(t.replica(.R_).commit(), 24);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt left of head, 3/3 corrupt)" {
    // The replicas recognize that the corrupt entry is outside of the pipeline and
    // must be committed.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.R_).stop();
    t.replica(.R_).corrupt(.{ .wal_prepare = 10 });
    try t.replica(.R_).open();
    t.run();

    // The same prepare is lost by all WALs, so the cluster can never recover.
    // Each replica stalls trying to repair the header break.
    try expectEqual(t.replica(.R_).status(), .view_change);
    try expectEqual(t.replica(.R_).commit(), 0);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt root)" {
    // A replica can recover from a corrupt root prepare.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_prepare = 0 });
    try t.replica(.R0).open();

    try c.request(21, 21);
    try expectEqual(t.replica(.R_).commit(), 21);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt checkpoint…head)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    // Trigger the first checkpoint.
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    t.replica(.R0).stop();

    // Corrupt op_checkpoint (27) and all ops that follow.
    var slot: usize = slot_count - constants.lsm_batch_multiple - 1;
    while (slot < slot_count) : (slot += 1) {
        t.replica(.R0).corrupt(.{ .wal_prepare = slot });
    }
    try t.replica(.R0).open();
    try expectEqual(t.replica(.R0).status(), .recovering_head);

    try c.request(slot_count, slot_count);
    try expectEqual(t.replica(.R0).status(), .normal);
    t.replica(.R1).stop();
    try c.request(slot_count + 1, slot_count + 1);
}

test "Cluster: recovery: WAL prepare corruption (R=1, corrupt between checkpoint and head)" {
    // R=1 can never recover if a WAL-prepare is corrupt.
    const t = try TestContext.init(.{ .replica_count = 1 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_prepare = 15 });
    if (t.replica(.R0).open()) {
        unreachable;
    } else |err| switch (err) {
        error.WALCorrupt => {},
        else => unreachable,
    }
}

test "Cluster: recovery: WAL header corruption (R=1)" {
    // R=1 locally repairs WAL-header corruption.
    const t = try TestContext.init(.{ .replica_count = 1 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_header = 15 });
    try t.replica(.R0).open();
    try c.request(30, 30);
}

test "Cluster: recovery: WAL torn prepare, standby with intact prepare (R=1 S=1)" {
    // R=1 recovers to find that its last prepare was a torn write, so it is truncated.
    // The standby received the prepare, though.
    //
    // R=1 handles this by incrementing its view during recovery, so that the standby can truncate
    // discard the truncated prepare.
    const t = try TestContext.init(.{
        .replica_count = 1,
        .standby_count = 1,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_header = 20 });
    try t.replica(.R0).open();
    try c.request(30, 30);
    try expectEqual(t.replica(.R0).commit(), 30);
    try expectEqual(t.replica(.S0).commit(), 30);
}

test "Cluster: recovery: grid corruption (disjoint)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);

    // Checkpoint to ensure that the replicas will actually use the grid to recover.
    // All replicas must be at the same commit to ensure grid repair won't fail and
    // fall back to state sync.
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_1);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger);

    t.replica(.R_).stop();

    // Corrupt the whole grid.
    // Manifest blocks will be repaired as each replica opens its forest.
    // Table index/filter/data blocks will be repaired as the replica commits/compacts.
    for ([_]TestReplicas{
        t.replica(.R0),
        t.replica(.R1),
        t.replica(.R2),
    }, 0..) |replica, i| {
        var address: u64 = 1 + i; // Addresses start at 1.
        while (address <= Storage.grid_blocks_max) : (address += 3) {
            // Leave every third address un-corrupt.
            // Each block exists intact on exactly one replica.
            replica.corrupt(.{ .grid_block = address + 1 });
            replica.corrupt(.{ .grid_block = address + 2 });
        }
    }

    try t.replica(.R_).open();
    t.run();

    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_1);

    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_2);
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger);
}

test "Cluster: recovery: recovering_head, outdated start view" {
    // 1. Wait for B1 to ok op=21.
    // 2. Restart B1 while corrupting op=21, so that it gets into a .recovering_head with op=20.
    // 3. Try make B1 forget about op=21 by delivering it an outdated .start_view with op=20.
    const t = try TestContext.init(.{
        .replica_count = 3,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    var a = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    try c.request(20, 20);

    b1.stop();
    b1.corrupt(.{ .wal_prepare = 20 });

    try b1.open();
    try expectEqual(b1.status(), .recovering_head);
    try expectEqual(b1.op_head(), 19);

    b1.record(.A0, .incoming, .start_view);
    t.run();
    try expectEqual(b1.status(), .normal);
    try expectEqual(b1.op_head(), 20);

    b2.drop_all(.R_, .bidirectional);

    try c.request(21, 21);

    b1.stop();
    b1.corrupt(.{ .wal_prepare = 21 });

    try b1.open();
    try expectEqual(b1.status(), .recovering_head);
    try expectEqual(b1.op_head(), 20);

    // TODO Explicit code coverage marks: This should hit the
    // "on_start_view: ignoring (recovering_head, nonce mismatch)"
    a.stop();
    b1.replay_recorded();
    t.run();

    try expectEqual(b1.status(), .recovering_head);
    try expectEqual(b1.op_head(), 20);

    // Should B1 erroneously accept op=20 as head, unpartitioning B2 here would lead to a data loss.
    b2.pass_all(.R_, .bidirectional);
    t.run();
    try a.open();
    try c.request(22, 22);
}

test "Cluster: recovery: recovering head: idle cluster" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    var b = t.replica(.B1);

    try c.request(20, 20);

    b.stop();
    b.corrupt(.{ .wal_prepare = 21 });
    b.corrupt(.{ .wal_header = 21 });

    try b.open();
    try expectEqual(b.status(), .recovering_head);
    try expectEqual(b.op_head(), 20);

    t.run();

    try expectEqual(b.status(), .normal);
    try expectEqual(b.op_head(), 20);
}

test "Cluster: network: partition 2-1 (isolate backup, symmetric)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.B2).drop_all(.__, .bidirectional);
    try c.request(30, 30);
    try expectEqual(t.replica(.A0).commit(), 30);
    try expectEqual(t.replica(.B1).commit(), 30);
    try expectEqual(t.replica(.B2).commit(), 20);
}

test "Cluster: network: partition 2-1 (isolate backup, asymmetric, send-only)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.B2).drop_all(.__, .incoming);
    try c.request(30, 30);
    try expectEqual(t.replica(.A0).commit(), 30);
    try expectEqual(t.replica(.B1).commit(), 30);
    try expectEqual(t.replica(.B2).commit(), 20);
}

test "Cluster: network: partition 2-1 (isolate backup, asymmetric, receive-only)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.B2).drop_all(.__, .outgoing);
    try c.request(30, 30);
    try expectEqual(t.replica(.A0).commit(), 30);
    try expectEqual(t.replica(.B1).commit(), 30);
    // B2 may commit some ops, but at some point is will likely fall behind.
    // Prepares may be reordered by the network, and if B1 receives X+1 then X,
    // it will not forward X on, as it is a "repair".
    // And B2 is partitioned, so it cannot repair its hash chain.
    try std.testing.expect(t.replica(.B2).commit() >= 20);
}

test "Cluster: network: partition 1-2 (isolate primary, symmetric)" {
    // The primary cannot communicate with either backup, but the backups can communicate with one
    // another. The backups will perform a view-change since they don't receive heartbeats.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);

    const p = t.replica(.A0);
    p.drop_all(.B1, .bidirectional);
    p.drop_all(.B2, .bidirectional);
    try c.request(30, 30);
    try expectEqual(p.commit(), 20);
}

test "Cluster: network: partition 1-2 (isolate primary, asymmetric, send-only)" {
    // The primary can send to the backups, but not receive.
    // After a short interval of not receiving messages (specifically prepare_ok's) it will abdicate
    // by pausing heartbeats, allowing the next replica to take over as primary.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.A0).drop_all(.B1, .incoming);
    t.replica(.A0).drop_all(.B2, .incoming);
    // TODO: Explicit coverage marks: This should hit the "primary abdicating" log line.
    try c.request(30, 30);
}

test "Cluster: network: partition 1-2 (isolate primary, asymmetric, receive-only)" {
    // The primary can receive from the backups, but not send to them.
    // The backups will perform a view-change since they don't receive heartbeats.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.A0).drop_all(.B1, .outgoing);
    t.replica(.A0).drop_all(.B2, .outgoing);
    try c.request(30, 30);
}

test "Cluster: network: partition client-primary (symmetric)" {
    // Clients cannot communicate with the primary, but they still request/reply via a backup.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);

    t.replica(.A0).drop_all(.C_, .bidirectional);
    // TODO: https://github.com/tigerbeetle/tigerbeetle/issues/444
    // try c.request(30, 30);
    try c.request(30, 20);
}

test "Cluster: network: partition client-primary (asymmetric, drop requests)" {
    // Primary cannot receive messages from the clients.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);

    t.replica(.A0).drop_all(.C_, .incoming);
    // TODO: https://github.com/tigerbeetle/tigerbeetle/issues/444
    // try c.request(30, 40);
    try c.request(30, 20);
}

test "Cluster: network: partition client-primary (asymmetric, drop replies)" {
    // Clients cannot receive replies from the primary, but they receive replies from a backup.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);

    t.replica(.A0).drop_all(.C_, .outgoing);
    // TODO: https://github.com/tigerbeetle/tigerbeetle/issues/444
    // try c.request(30, 30);
    try c.request(30, 20);
}

test "Cluster: repair: partition 2-1, then backup fast-forward 1 checkpoint" {
    // A backup that has fallen behind by two checkpoints can catch up, without using state sync.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    var r_lag = t.replica(.B2);
    r_lag.drop_all(.__, .bidirectional);

    // Commit enough ops to checkpoint once, and then nearly wrap around, leaving enough slack
    // that the lagging backup can repair (without state sync).
    const commit = 20 + slot_count - constants.pipeline_prepare_queue_max;
    try c.request(commit, commit);
    try expectEqual(t.replica(.A0).op_checkpoint(), checkpoint_1);
    try expectEqual(t.replica(.B1).op_checkpoint(), checkpoint_1);

    try expectEqual(r_lag.status(), .normal);
    try expectEqual(r_lag.op_checkpoint(), 0);

    // Allow repair, but ensure that state sync doesn't run.
    r_lag.pass_all(.__, .bidirectional);
    r_lag.drop(.__, .bidirectional, .sync_checkpoint);
    t.run();

    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_1);
    try expectEqual(t.replica(.R_).commit(), commit);
}

test "Cluster: repair: view-change, new-primary lagging behind checkpoint, forfeit" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b1.drop_all(.__, .bidirectional);

    try c.request(checkpoint_1_trigger - 1, checkpoint_1_trigger - 1);
    try expectEqual(a0.commit(), checkpoint_1_trigger - 1);
    try expectEqual(b1.commit(), 20);
    try expectEqual(b2.commit(), checkpoint_1_trigger - 1);

    b2.drop(.__, .incoming, .commit);
    try c.request(checkpoint_1_trigger + 1, checkpoint_1_trigger + 1);
    try expectEqual(a0.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), 0);
    try expectEqual(b2.op_checkpoint(), checkpoint_1);

    // Partition the primary, but restore B1. B1 will attempt to become the primary next,
    // but it is too far behind, so B2 becomes the new primary instead.
    b2.pass_all(.__, .bidirectional);
    b1.pass_all(.__, .bidirectional);
    a0.drop_all(.__, .bidirectional);
    // TODO: Explicit coverage marks: This should hit the
    // "on_do_view_change: lagging primary; forfeiting" log line.
    t.run();

    try expectEqual(b2.role(), .primary);
    try expectEqual(b2.index(), t.replica(.A0).index());
    try expectEqual(b2.view(), b1.view());
    try expectEqual(b2.log_view(), b1.log_view());

    // Thanks to the new primary, the lagging backup is able to catch up to the latest
    // checkpoint/commit.
    try expectEqual(b1.role(), .backup);
    try expectEqual(b1.commit(), checkpoint_1_trigger);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);
}

// TODO: Re-enable when the op_checkpoint hack is removed from ignore_request_message().
// (This test relies on preparing on the primary before the backups commit, which is presently
// impossible.)
// test "Cluster: repair: view-change, new-primary lagging behind checkpoint, truncate all post-checkpoint ops" {
//     const t = try TestContext.init(.{
//         .replica_count = 3,
//         .client_count = constants.pipeline_prepare_queue_max,
//     });
//     defer t.deinit();
//
//     var c = t.clients(0, t.cluster.clients.len);
//     try c.request(checkpoint_1_trigger - 1, checkpoint_1_trigger - 1);
//
//     var a0 = t.replica(.A0);
//     var b1 = t.replica(.B1);
//     var b2 = t.replica(.B2);
//
//     // Drop SVCs to ensure A0 cannot abdicate (yet).
//     t.replica(.R_).drop(.__, .bidirectional, .start_view_change);
//     // B1 can see and ack the checkpoint-trigger prepare, but not its commit, so it cannot checkpoint.
//     b1.drop(.__, .incoming, .commit);
//     // B2 can see that the checkpoint-trigger prepare commits, but cannot receive the message itself.
//     // It will try RSV to advances its head, but don't let it just yet.
//     b2.drop(.__, .incoming, .prepare);
//     b2.drop(.__, .incoming, .start_view);
//
//     try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
//     try expectEqual(a0.op_checkpoint(), checkpoint_1);
//     try expectEqual(b1.op_checkpoint(), 0);
//     try expectEqual(b2.op_checkpoint(), 0);
//
//     // B1 wouldn't prepare these anyway, but we prevent it from learning that the checkpoint
//     // trigger is committed.
//     b1.drop(.__, .incoming, .prepare);
//
//     try c.request(checkpoint_1_trigger + constants.pipeline_prepare_queue_max, checkpoint_1_trigger);
//     try expectEqual(a0.op_head(), checkpoint_1_trigger + constants.pipeline_prepare_queue_max);
//     try expectEqual(b1.op_head(), checkpoint_1_trigger);
//     try expectEqual(b2.op_head(), checkpoint_1_trigger - 1);
//
//     {
//         // Now that A0 has a full pipeline past the checkpoint, allow B2 to advance its head via the
//         // hook. But first kick it into recovering_head to force it to load the SV's headers into
//         // its view_headers. Those SV headers have an op-max past the next checkpoint, so our
//         // op_head does not increase beyond the checkpoint trigger.
//         b2.pass(.__, .incoming, .start_view);
//         b2.corrupt(.{ .wal_prepare = checkpoint_1_trigger - 1 });
//
//         // We must receive a prepare to trigger a view_jump, which is what will trigger the RSV.
//         // But don't send an ack, since that would allow A0 to send us the next prepare,
//         // which would cause us to learn that the trigger is committed,
//         // which would cause us to checkpoint.
//         b2.pass(.__, .incoming, .prepare);
//         b2.drop(.__, .outgoing, .prepare_ok);
//
//         // Corrupt headers & prevent header repair so that we can't commit.
//         b2.corrupt(.{ .wal_header = 5 });
//         b2.drop(.__, .incoming, .headers);
//
//         b2.stop();
//         try b2.open();
//         try expectEqual(b2.status(), .recovering_head);
//         t.run();
//
//         try expectEqual(b2.status(), .normal);
//         try expectEqual(b2.op_checkpoint(), 0);
//         try expectEqual(b2.op_head(), checkpoint_1_trigger);
//         try expectEqual(b2.view_headers()[0].op, checkpoint_1_trigger + constants.pipeline_prepare_queue_max);
//     }
//
//     // Take down A0 and allow a view-change.
//     // B2 has not repaired yet, so it reuses the view_headers that it got from the SV (with headers
//     // from the next wrap).
//     // TODO Explicit code coverage marks: This hits the "discarded uncommitted ops after trigger"
//     // log line in on_do_view_change().
//     a0.stop();
//     b1.pass_all(.__, .incoming);
//     b2.pass_all(.__, .incoming);
//     t.run();
//
//     try expectEqual(b1.status(), .normal);
//     try expectEqual(b2.status(), .normal);
//     try expectEqual(b1.op_checkpoint(), checkpoint_1);
//     try expectEqual(b2.op_checkpoint(), checkpoint_1);
// }

test "Cluster: repair: crash, corrupt committed pipeline op, repair it, view-change; dont nack" {
    // This scenario is also applicable when any op within the pipeline suffix is corrupted.
    // But we test by corrupting the last op to take advantage of recovering_head to learn the last
    // op's header without its prepare.
    //
    // Also, a corrupt last op maximizes uncertainty — there are no higher ops which
    // can definitively show that the last op is committed (via `header.commit`).
    const t = try TestContext.init(.{
        .replica_count = 3,
        .client_count = constants.pipeline_prepare_queue_max,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b2.drop_all(.R_, .bidirectional);

    try c.request(30, 30);

    b1.stop();
    b1.corrupt(.{ .wal_prepare = 30 });

    // We can't learn op=30's prepare, only its header (via start_view).
    b1.drop(.R_, .bidirectional, .prepare);
    try b1.open();
    try expectEqual(b1.status(), .recovering_head);
    t.run();

    b1.pass_all(.R_, .bidirectional);
    b2.pass_all(.R_, .bidirectional);
    a0.stop();
    a0.drop_all(.R_, .outgoing);
    t.run();

    // The cluster is stuck trying to repair op=30 (requesting the prepare).
    // B2 can nack op=30, but B1 *must not*.
    try expectEqual(b1.status(), .view_change);
    try expectEqual(b1.commit(), 29);
    try expectEqual(b1.op_head(), 30);

    // A0 provides prepare=30.
    a0.pass_all(.R_, .outgoing);
    try a0.open();
    t.run();
    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).commit(), 30);
    try expectEqual(t.replica(.R_).op_head(), 30);
}

test "Cluster: repair: corrupt reply" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    // Prevent any view changes, to ensure A0 repairs its corrupt prepare.
    t.replica(.R_).drop(.R_, .bidirectional, .do_view_change);

    // Block the client from seeing the reply from the cluster.
    t.replica(.R_).drop(.C_, .outgoing, .reply);
    try c.request(21, 20);

    // Corrupt all of the primary's saved replies.
    // (This is easier than figuring out the reply's actual slot.)
    var slot: usize = 0;
    while (slot < constants.clients_max) : (slot += 1) {
        t.replica(.A0).corrupt(.{ .client_reply = slot });
    }

    // The client will keep retrying request 21 until it receives a reply.
    // The primary requests the reply from one of its backups.
    // (Pass A0 only to ensure that no other client forwards the reply.)
    t.replica(.A0).pass(.C_, .outgoing, .reply);
    t.run();

    try expectEqual(c.replies(), 21);
}

test "Cluster: repair: ack committed prepare" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    const p = t.replica(.A0);
    const b1 = t.replica(.B1);
    const b2 = t.replica(.B2);

    // A0 commits 21.
    // B1 prepares 21, but does not commit.
    t.replica(.R_).drop(.R_, .bidirectional, .start_view_change);
    t.replica(.R_).drop(.R_, .bidirectional, .do_view_change);
    p.drop(.__, .outgoing, .commit);
    b2.drop(.__, .incoming, .prepare);
    try c.request(21, 21);
    try expectEqual(p.commit(), 21);
    try expectEqual(b1.commit(), 20);
    try expectEqual(b2.commit(), 20);

    try expectEqual(p.op_head(), 21);
    try expectEqual(b1.op_head(), 21);
    try expectEqual(b2.op_head(), 20);

    try expectEqual(p.status(), .normal);
    try expectEqual(b1.status(), .normal);
    try expectEqual(b2.status(), .normal);

    // Change views. B1/B2 participate. Don't allow B2 to repair op=21.
    t.replica(.R_).pass(.R_, .bidirectional, .start_view_change);
    t.replica(.R_).pass(.R_, .bidirectional, .do_view_change);
    p.drop(.__, .bidirectional, .prepare);
    p.drop(.__, .bidirectional, .do_view_change);
    p.drop(.__, .bidirectional, .start_view_change);
    t.run();
    try expectEqual(b1.commit(), 20);
    try expectEqual(b2.commit(), 20);

    try expectEqual(p.status(), .normal);
    try expectEqual(b1.status(), .normal);
    try expectEqual(b2.status(), .normal);

    // But other than that, heal A0/B1, but partition B2 completely.
    // (Prevent another view change.)
    p.pass_all(.__, .bidirectional);
    b1.pass_all(.__, .bidirectional);
    b2.drop_all(.__, .bidirectional);
    t.replica(.R_).drop(.R_, .bidirectional, .start_view_change);
    t.replica(.R_).drop(.R_, .bidirectional, .do_view_change);
    t.run();

    try expectEqual(p.status(), .normal);
    try expectEqual(b1.status(), .normal);
    try expectEqual(b2.status(), .normal);

    // A0 acks op=21 even though it already committed it.
    try expectEqual(p.commit(), 21);
    try expectEqual(b1.commit(), 21);
    try expectEqual(b2.commit(), 20);
}

test "Cluster: repair: primary checkpoint, backup crash before checkpoint, primary prepare" {
    // 1. Given 3 replica: A0, B1, B2.
    // 2. B2 is partitioned (for the entire scenario).
    // 3. A0 and B1 prepare and commit many messages...
    // 4. A0 commits a checkpoint trigger and checkpoints.
    // 5. B1 crashes before it can commit the trigger or checkpoint.
    // 6. A0 prepares a message.
    // 7. B1 restarts. The very first entry in its WAL is corrupt.
    // A0 has *not* already overwritten the corresponding entry in its own WAL, thanks to the
    // pipeline component of the vsr_checkpoint_interval.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    var p = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    // B2 does not participate in this scenario.
    b2.stop();
    try c.request(checkpoint_1_trigger - 1, checkpoint_1_trigger - 1);

    b1.drop(.R_, .incoming, .commit);
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    try expectEqual(p.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), 0);
    try expectEqual(p.commit(), checkpoint_1_trigger);
    try expectEqual(b1.commit(), checkpoint_1_trigger - 1);

    b1.pass(.R_, .incoming, .commit);
    b1.stop();
    b1.corrupt(.{ .wal_prepare = 1 });
    try c.request(checkpoint_1_trigger + constants.pipeline_prepare_queue_max, checkpoint_1_trigger);
    try b1.open();
    t.run();

    try expectEqual(p.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);
    try expectEqual(p.commit(), checkpoint_1_trigger + constants.pipeline_prepare_queue_max);
    try expectEqual(b1.commit(), checkpoint_1_trigger + constants.pipeline_prepare_queue_max);
}

test "Cluster: view-change: DVC, 1+1/2 faulty header stall, 2+1/3 faulty header succeed" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    t.replica(.R0).stop();
    try c.request(24, 24);
    t.replica(.R1).stop();
    t.replica(.R2).stop();

    t.replica(.R1).corrupt(.{ .wal_prepare = 22 });

    // The nack quorum size is 2.
    // The new view must determine whether op=22 is possibly committed.
    // - R0 never received op=22 (it had already crashed), so it nacks.
    // - R1 did receive op=22, but upon recovering its WAL, it was corrupt, so it cannot nack.
    // The cluster must wait form R2 before recovering.
    try t.replica(.R0).open();
    try t.replica(.R1).open();
    // TODO Explicit code coverage marks: This should hit the "quorum received, awaiting repair"
    // log line in on_do_view_change().
    t.run();
    try expectEqual(t.replica(.R0).status(), .view_change);
    try expectEqual(t.replica(.R1).status(), .view_change);

    // R2 provides the missing header, allowing the view-change to succeed.
    try t.replica(.R2).open();
    t.run();
    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).commit(), 24);
}

test "Cluster: view-change: DVC, 2/3 faulty header stall" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    t.replica(.R0).stop();
    try c.request(24, 24);
    t.replica(.R1).stop();
    t.replica(.R2).stop();

    t.replica(.R1).corrupt(.{ .wal_prepare = 22 });
    t.replica(.R2).corrupt(.{ .wal_prepare = 22 });

    try t.replica(.R_).open();
    // TODO Explicit code coverage marks: This should hit the "quorum received, deadlocked"
    // log line in on_do_view_change().
    t.run();
    try expectEqual(t.replica(.R_).status(), .view_change);
}

test "Cluster: view-change: duel of the primaries" {
    // In a cluster of 3, one replica gets partitioned away, and the remaining two _both_ become
    // primaries (for different views). Additionally, the primary from the  higher view is
    // abdicating. The primaries should figure out that they need to view-change to a higher view.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    try expectEqual(t.replica(.R_).view(), 1);
    try expectEqual(t.replica(.R1).role(), .primary);

    t.replica(.R2).drop_all(.R_, .bidirectional);
    t.replica(.R1).drop(.R_, .outgoing, .commit);
    try c.request(21, 21);

    try expectEqual(t.replica(.R0).commit_max(), 20);
    try expectEqual(t.replica(.R1).commit_max(), 21);
    try expectEqual(t.replica(.R2).commit_max(), 20);

    t.replica(.R0).pass_all(.R_, .bidirectional);
    t.replica(.R2).pass_all(.R_, .bidirectional);
    t.replica(.R1).drop_all(.R_, .bidirectional);
    t.replica(.R2).drop(.R0, .bidirectional, .prepare_ok);
    t.replica(.R2).drop(.R0, .outgoing, .do_view_change);
    t.run();

    // The stage is set: we have two primaries in different views, R2 is about to abdicate.
    try expectEqual(t.replica(.R1).view(), 1);
    try expectEqual(t.replica(.R1).status(), .normal);
    try expectEqual(t.replica(.R1).role(), .primary);
    try expectEqual(t.replica(.R1).commit(), 21);
    try expectEqual(t.replica(.R2).op_head(), 21);

    try expectEqual(t.replica(.R2).view(), 2);
    try expectEqual(t.replica(.R2).status(), .normal);
    try expectEqual(t.replica(.R2).role(), .primary);
    try expectEqual(t.replica(.R2).commit(), 20);
    try expectEqual(t.replica(.R2).op_head(), 21);

    t.replica(.R1).pass_all(.R_, .bidirectional);
    t.replica(.R2).pass_all(.R_, .bidirectional);
    t.replica(.R0).drop_all(.R_, .bidirectional);
    t.run();

    try expectEqual(t.replica(.R1).commit(), 21);
    try expectEqual(t.replica(.R2).commit(), 21);
}

test "Cluster: sync: partition, lag, sync (transition from idle)" {
    for ([_]u64{
        // Normal case: the cluster has committed atop the checkpoint trigger.
        // The lagging replica can learn the canonical checkpoint from a commit message.
        checkpoint_2_trigger + 1,
        // Idle case: the idle cluster has not committed atop the checkpoint trigger.
        // The lagging replica uses the sync target candidate quorum (populated by ping messages)
        // to identify the canonical checkpoint.
        // TODO Explicit code coverage: "candidate checkpoint is canonical (quorum)"
        checkpoint_2_trigger,
    }) |cluster_commit_max| {
        log.info("test cluster_commit_max={}", .{cluster_commit_max});

        const t = try TestContext.init(.{ .replica_count = 3 });
        defer t.deinit();

        var c = t.clients(0, t.cluster.clients.len);
        try c.request(20, 20);
        try expectEqual(t.replica(.R_).commit(), 20);

        t.replica(.R2).drop_all(.R_, .bidirectional);
        try c.request(cluster_commit_max, cluster_commit_max);

        t.replica(.R2).pass_all(.R_, .bidirectional);
        t.run();

        // R2 catches up via state sync.
        try expectEqual(t.replica(.R_).status(), .normal);
        try expectEqual(t.replica(.R_).commit(), cluster_commit_max);
        try expectEqual(t.replica(.R_).sync_status(), .idle);

        // The entire cluster is healthy and able to commit more.
        try c.request(checkpoint_3_trigger, checkpoint_3_trigger);
        try expectEqual(t.replica(.R_).status(), .normal);
        try expectEqual(t.replica(.R_).commit(), checkpoint_3_trigger);

        t.run(); // (Wait for grid sync to finish.)
        try TestReplicas.expect_sync_done(t.replica(.R_));
    }
}

test "Cluster: sync: sync, bump target, sync" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(16, 16);
    try expectEqual(t.replica(.R_).commit(), 16);

    t.replica(.R2).drop_all(.R_, .bidirectional);
    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);

    // Allow R2 to complete SyncStage.requesting_target, but get stuck
    // during SyncStage.requesting_checkpoint.
    t.replica(.R2).pass_all(.R_, .bidirectional);
    t.replica(.R2).drop(.R_, .outgoing, .request_sync_checkpoint);
    t.run();
    try expectEqual(t.replica(.R2).sync_status(), .requesting_checkpoint);
    try expectEqual(t.replica(.R2).sync_target_checkpoint_op(), checkpoint_2);

    // R2 discovers the newer sync target and restarts sync.
    try c.request(checkpoint_3_trigger, checkpoint_3_trigger);
    try expectEqual(t.replica(.R2).sync_status(), .requesting_checkpoint);
    try expectEqual(t.replica(.R2).sync_target_checkpoint_op(), checkpoint_3);

    t.replica(.R2).pass(.R_, .bidirectional, .request_sync_checkpoint);
    t.run();

    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).commit(), checkpoint_3_trigger);
    try expectEqual(t.replica(.R_).sync_status(), .idle);

    t.run(); // (Wait for grid sync to finish.)
    try TestReplicas.expect_sync_done(t.replica(.R_));
}

test "Cluster: sync: R=2" {
    // TODO explicit code coverage marks: "candidate is canonical (R=2)"
    const t = try TestContext.init(.{ .replica_count = 2 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_1_trigger - 1, checkpoint_1_trigger - 1);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);

    // A0 prepares the trigger op, commits it, and checkpoints.
    // B1 prepares the trigger op, but does not commit/checkpoint.
    b1.drop(.R_, .incoming, .commit); // Prevent last commit.
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    try expectEqual(a0.commit(), checkpoint_1_trigger);
    try expectEqual(b1.commit(), checkpoint_1_trigger - 1);
    try expectEqual(a0.op_head(), checkpoint_1_trigger);
    try expectEqual(b1.op_head(), checkpoint_1_trigger);
    try expectEqual(a0.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), 0);

    // On B1, corrupt the same slot that A0 is about to overwrite with a new prepare.
    b1.stop();
    b1.pass(.R_, .incoming, .commit);
    b1.corrupt(.{ .wal_prepare = (checkpoint_1_trigger + 2) % slot_count });

    // Prepare 2 more ops, to overwrite A0's slots 0,1 in the WAL.
    try c.request(checkpoint_1_trigger + 2, checkpoint_1_trigger);

    try b1.open();
    t.run();

    // B1 needs a prepare that no longer exists anywhere in the cluster.
    // So it state syncs. This is a special case, since normally A0's latest checkpoint
    // would not be recognized as canonical until it was committed.

    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger + 2);
    try expectEqual(c.replies(), checkpoint_1_trigger + 2);

    try TestReplicas.expect_sync_done(t.replica(.R_));
}

test "Cluster: sync: checkpoint diverges, sync (primary diverges)" {
    // Buggify a divergent replica (i.e. a storage determinism bug).
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    // Pass the first checkpoint to ensure that manifest blocks are required.
    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    a0.drop(.R_, .bidirectional, .request_sync_checkpoint); // (Prevent sync for now.)
    a0.diverge();

    // Prior to the checkpoint, the cluster has not realized that A0 diverged.
    try c.request(checkpoint_2_trigger - 1, checkpoint_2_trigger - 1);
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger - 1);
    try expectEqual(a0.role(), .primary);

    // After the checkpoint, A0 must discard all acks from B1/B2, since they are from
    // a different (i.e. correct) checkpoint.
    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_2);
    // A0 was forced to ignore B1/B2's acks since the checkpoint id didn't match its own.
    // Unable to commit, it stepped down as primary.
    try expectEqual(a0.role(), .backup);
    try expectEqual(b1.commit(), checkpoint_2_trigger);
    try expectEqual(b2.commit(), checkpoint_2_trigger);
    // A0 may have committed trigger+1, but its commit_min will backtrack due to sync.

    // A0 has learned about B1/B2's canonical checkpoint — a checkpoint with the same op,
    // but a different identifier.
    try expectEqual(a0.sync_status(), .requesting_checkpoint);
    try expectEqual(a0.sync_target_checkpoint_op(), checkpoint_2);
    try expectEqual(a0.sync_target_checkpoint_op(), t.replica(.R_).op_checkpoint());
    try expectEqual(a0.sync_target_checkpoint_id(), b1.op_checkpoint_id());
    try expectEqual(a0.sync_target_checkpoint_id(), b2.op_checkpoint_id());

    a0.pass(.R_, .bidirectional, .request_sync_checkpoint); // Allow sync again.
    t.run();

    // After syncing, A0 is back to the correct checkpoint.
    try expectEqual(t.replica(.R_).sync_status(), .idle);
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_2);
    try expectEqual(t.replica(.R_).op_checkpoint_id(), a0.op_checkpoint_id());

    try TestReplicas.expect_sync_done(t.replica(.R_));
}

test "Cluster: sync: R=4, 2/4 ahead + idle, 2/4 lagging, sync" {
    const t = try TestContext.init(.{ .replica_count = 4 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).commit(), 20);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);
    var b3 = t.replica(.B3);

    b2.stop();
    b3.stop();

    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);
    try expectEqual(a0.status(), .normal);
    try expectEqual(b1.status(), .normal);

    try b2.open();
    try b3.open();
    t.run();
    t.run();

    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).sync_status(), .idle);
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_2);

    try TestReplicas.expect_sync_done(t.replica(.R_));
}

const ProcessSelector = enum {
    __, // all replicas, standbys, and clients
    R_, // all (non-standby) replicas
    R0,
    R1,
    R2,
    R3,
    R4,
    R5,
    S_, // all standbys
    S0,
    S1,
    S2,
    S3,
    S4,
    S5,
    A0, // current primary
    B1, // backup immediately following current primary
    B2,
    B3,
    B4,
    B5,
    C_, // all clients
};

const TestContext = struct {
    cluster: *Cluster,
    log_level: std.log.Level,
    client_requests: [constants.clients_max]usize = [_]usize{0} ** constants.clients_max,
    client_replies: [constants.clients_max]usize = [_]usize{0} ** constants.clients_max,

    pub fn init(options: struct {
        replica_count: u8,
        standby_count: u8 = 0,
        client_count: u8 = constants.clients_max,
    }) !*TestContext {
        var log_level_original = std.testing.log_level;
        std.testing.log_level = log_level;

        var prng = std.rand.DefaultPrng.init(123);
        const random = prng.random();

        const cluster = try Cluster.init(allocator, TestContext.on_client_reply, .{
            .cluster_id = 0,
            .replica_count = options.replica_count,
            .standby_count = options.standby_count,
            .client_count = options.client_count,
            .storage_size_limit = vsr.sector_floor(constants.storage_size_limit_max),
            .seed = random.int(u64),
            .network = .{
                .node_count = options.replica_count + options.standby_count,
                .client_count = options.client_count,
                .seed = random.int(u64),
                .one_way_delay_mean = 3 + random.uintLessThan(u16, 10),
                .one_way_delay_min = random.uintLessThan(u16, 3),

                .path_maximum_capacity = 128,
                .path_clog_duration_mean = 0,
                .path_clog_probability = 0,
                .recorded_count_max = 16,
            },
            .storage = .{
                .read_latency_min = 1,
                .read_latency_mean = 5,
                .write_latency_min = 1,
                .write_latency_mean = 5,
            },
            .storage_fault_atlas = .{
                .faulty_superblock = false,
                .faulty_wal_headers = false,
                .faulty_wal_prepares = false,
                .faulty_client_replies = false,
                .faulty_grid = false,
            },
            .state_machine = .{ .lsm_forest_node_count = 4096 },
        });
        errdefer cluster.deinit();

        for (cluster.storages) |*storage| storage.faulty = true;

        var context = try allocator.create(TestContext);
        errdefer allocator.destroy(context);

        context.* = .{
            .cluster = cluster,
            .log_level = log_level_original,
        };
        cluster.context = context;

        return context;
    }

    pub fn deinit(t: *TestContext) void {
        std.testing.log_level = t.log_level;
        t.cluster.deinit();
        allocator.destroy(t);
    }

    pub fn replica(t: *TestContext, selector: ProcessSelector) TestReplicas {
        const replica_processes = t.processes(selector);
        var replica_indexes = stdx.BoundedArray(u8, constants.members_max){};
        for (replica_processes.const_slice()) |p| replica_indexes.append_assume_capacity(p.replica);
        return TestReplicas{
            .context = t,
            .cluster = t.cluster,
            .replicas = replica_indexes,
        };
    }

    pub fn clients(t: *TestContext, index: usize, count: usize) TestClients {
        var client_indexes = stdx.BoundedArray(usize, constants.clients_max){};
        for (index..index + count) |i| client_indexes.append_assume_capacity(i);
        return TestClients{
            .context = t,
            .cluster = t.cluster,
            .clients = client_indexes,
        };
    }

    pub fn run(t: *TestContext) void {
        const tick_max = 4_100;
        var tick_count: usize = 0;
        while (tick_count < tick_max) : (tick_count += 1) {
            if (t.tick()) tick_count = 0;
        }
    }

    /// Returns whether the cluster state advanced.
    fn tick(t: *TestContext) bool {
        const commits_before = t.cluster.state_checker.commits.items.len;
        t.cluster.tick();
        return commits_before != t.cluster.state_checker.commits.items.len;
    }

    fn on_client_reply(
        cluster: *Cluster,
        client: usize,
        request: *Message.Request,
        reply: *Message.Reply,
    ) void {
        _ = request;
        _ = reply;
        const t: *TestContext = @ptrCast(@alignCast(cluster.context.?));
        t.client_replies[client] += 1;
    }

    const ProcessList = stdx.BoundedArray(Process, constants.members_max + constants.clients_max);

    fn processes(t: *const TestContext, selector: ProcessSelector) ProcessList {
        const replica_count = t.cluster.options.replica_count;

        var view: u32 = 0;
        for (t.cluster.replicas) |*r| view = @max(view, r.view);

        var array = ProcessList{};
        switch (selector) {
            .R0 => array.append_assume_capacity(.{ .replica = 0 }),
            .R1 => array.append_assume_capacity(.{ .replica = 1 }),
            .R2 => array.append_assume_capacity(.{ .replica = 2 }),
            .R3 => array.append_assume_capacity(.{ .replica = 3 }),
            .R4 => array.append_assume_capacity(.{ .replica = 4 }),
            .R5 => array.append_assume_capacity(.{ .replica = 5 }),
            .S0 => array.append_assume_capacity(.{ .replica = replica_count + 0 }),
            .S1 => array.append_assume_capacity(.{ .replica = replica_count + 1 }),
            .S2 => array.append_assume_capacity(.{ .replica = replica_count + 2 }),
            .S3 => array.append_assume_capacity(.{ .replica = replica_count + 3 }),
            .S4 => array.append_assume_capacity(.{ .replica = replica_count + 4 }),
            .S5 => array.append_assume_capacity(.{ .replica = replica_count + 5 }),
            .A0 => array.append_assume_capacity(.{ .replica = @as(u8, @intCast((view + 0) % replica_count)) }),
            .B1 => array.append_assume_capacity(.{ .replica = @as(u8, @intCast((view + 1) % replica_count)) }),
            .B2 => array.append_assume_capacity(.{ .replica = @as(u8, @intCast((view + 2) % replica_count)) }),
            .B3 => array.append_assume_capacity(.{ .replica = @as(u8, @intCast((view + 3) % replica_count)) }),
            .B4 => array.append_assume_capacity(.{ .replica = @as(u8, @intCast((view + 4) % replica_count)) }),
            .B5 => array.append_assume_capacity(.{ .replica = @as(u8, @intCast((view + 5) % replica_count)) }),
            .__, .R_, .S_, .C_ => {
                if (selector == .__ or selector == .R_) {
                    for (t.cluster.replicas[0..replica_count], 0..) |_, i| {
                        array.append_assume_capacity(.{ .replica = @as(u8, @intCast(i)) });
                    }
                }
                if (selector == .__ or selector == .S_) {
                    for (t.cluster.replicas[replica_count..], 0..) |_, i| {
                        array.append_assume_capacity(.{ .replica = @as(u8, @intCast(replica_count + i)) });
                    }
                }
                if (selector == .__ or selector == .C_) {
                    for (t.cluster.clients) |*client| {
                        array.append_assume_capacity(.{ .client = client.id });
                    }
                }
            },
        }
        assert(array.count() > 0);
        return array;
    }
};

const TestReplicas = struct {
    context: *TestContext,
    cluster: *Cluster,
    replicas: stdx.BoundedArray(u8, constants.members_max),

    pub fn stop(t: *const TestReplicas) void {
        for (t.replicas.const_slice()) |r| {
            log.info("{}: crash replica", .{r});
            t.cluster.crash_replica(r);
        }
    }

    pub fn open(t: *const TestReplicas) !void {
        for (t.replicas.const_slice()) |r| {
            log.info("{}: restart replica", .{r});
            t.cluster.restart_replica(r) catch |err| {
                assert(t.replicas.count() == 1);
                return switch (err) {
                    error.WALCorrupt => return error.WALCorrupt,
                    error.WALInvalid => return error.WALInvalid,
                    else => @panic("unexpected error"),
                };
            };
        }
    }

    pub fn index(t: *const TestReplicas) u8 {
        assert(t.replicas.count() == 1);
        return t.replicas.get(0);
    }

    fn get(
        t: *const TestReplicas,
        comptime field: std.meta.FieldEnum(Cluster.Replica),
    ) std.meta.fieldInfo(Cluster.Replica, field).type {
        var value_all: ?std.meta.fieldInfo(Cluster.Replica, field).type = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            const value = @field(replica, @tagName(field));
            if (value_all) |all| {
                if (all != value) {
                    for (t.replicas.const_slice()) |replica_index| {
                        log.err("replica={} field={s} value={}", .{
                            replica_index,
                            @tagName(field),
                            @field(&t.cluster.replicas[replica_index], @tagName(field)),
                        });
                    }
                    @panic("test failed: value mismatch");
                }
            } else {
                value_all = value;
            }
        }
        return value_all.?;
    }

    pub fn status(t: *const TestReplicas) vsr.Status {
        return t.get(.status);
    }

    pub fn view(t: *const TestReplicas) u32 {
        return t.get(.view);
    }

    pub fn log_view(t: *const TestReplicas) u32 {
        return t.get(.log_view);
    }

    pub fn op_head(t: *const TestReplicas) u64 {
        return t.get(.op);
    }

    pub fn commit(t: *const TestReplicas) u64 {
        return t.get(.commit_min);
    }

    pub fn commit_max(t: *const TestReplicas) u64 {
        return t.get(.commit_max);
    }

    pub fn state_machine_opened(t: *const TestReplicas) bool {
        return t.get(.state_machine_opened);
    }

    fn sync_stage(t: *const TestReplicas) vsr.SyncStage {
        assert(t.replicas.count() > 0);

        var sync_stage_all: ?vsr.SyncStage = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            if (sync_stage_all) |all| {
                assert(std.meta.eql(all, replica.syncing));
            } else {
                sync_stage_all = replica.syncing;
            }
        }
        return sync_stage_all.?;
    }

    pub fn sync_status(t: *const TestReplicas) std.meta.Tag(vsr.SyncStage) {
        return @as(std.meta.Tag(vsr.SyncStage), t.sync_stage());
    }

    fn sync_target(t: *const TestReplicas) ?vsr.SyncTarget {
        return t.sync_stage().target();
    }

    pub fn sync_target_checkpoint_op(t: *const TestReplicas) ?u64 {
        if (t.sync_target()) |target| {
            return target.checkpoint_op;
        } else {
            return null;
        }
    }

    pub fn sync_target_checkpoint_id(t: *const TestReplicas) ?u128 {
        if (t.sync_target()) |target| {
            return target.checkpoint_id;
        } else {
            return null;
        }
    }

    const Role = enum { primary, backup, standby };

    pub fn role(t: *const TestReplicas) Role {
        var role_all: ?Role = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            const replica_role: Role = role: {
                if (replica.standby()) {
                    break :role .standby;
                } else if (replica.replica == replica.primary_index(replica.view)) {
                    break :role .primary;
                } else {
                    break :role .backup;
                }
            };
            assert(role_all == null or role_all.? == replica_role);
            role_all = replica_role;
        }
        return role_all.?;
    }

    pub fn op_checkpoint_id(t: *const TestReplicas) u128 {
        var checkpoint_id_all: ?u128 = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            const replica_checkpoint_id = replica.superblock.working.checkpoint_id();
            assert(checkpoint_id_all == null or checkpoint_id_all.? == replica_checkpoint_id);
            checkpoint_id_all = replica_checkpoint_id;
        }
        return checkpoint_id_all.?;
    }

    pub fn op_checkpoint(t: *const TestReplicas) u64 {
        var checkpoint_all: ?u64 = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            assert(checkpoint_all == null or checkpoint_all.? == replica.op_checkpoint());
            checkpoint_all = replica.op_checkpoint();
        }
        return checkpoint_all.?;
    }

    pub fn view_headers(t: *const TestReplicas) []const vsr.Header {
        assert(t.replicas.count() == 1);
        return t.cluster.replicas[t.replicas.get(0)].view_headers.array.const_slice();
    }

    /// Simulate a storage determinism bug.
    /// (Replicas must be running and between compaction beats for this to run.)
    pub fn diverge(t: *TestReplicas) void {
        for (t.replicas.const_slice()) |r| {
            t.cluster.diverge(r);
        }
    }

    pub fn corrupt(
        t: *const TestReplicas,
        target: union(enum) {
            wal_header: usize, // slot
            wal_prepare: usize, // slot
            client_reply: usize, // slot
            grid_block: u64, // address
        },
    ) void {
        switch (target) {
            .wal_header => |slot| {
                const fault_offset = vsr.Zone.wal_headers.offset(slot * @sizeOf(vsr.Header));
                for (t.replicas.const_slice()) |r| {
                    t.cluster.storages[r].memory[fault_offset] +%= 1;
                }
            },
            .wal_prepare => |slot| {
                const fault_offset = vsr.Zone.wal_prepares.offset(slot * constants.message_size_max);
                const fault_sector = @divExact(fault_offset, constants.sector_size);
                for (t.replicas.const_slice()) |r| {
                    t.cluster.storages[r].faults.set(fault_sector);
                }
            },
            .client_reply => |slot| {
                const fault_offset = vsr.Zone.client_replies.offset(slot * constants.message_size_max);
                const fault_sector = @divExact(fault_offset, constants.sector_size);
                for (t.replicas.const_slice()) |r| {
                    t.cluster.storages[r].faults.set(fault_sector);
                }
            },
            .grid_block => |address| {
                const fault_offset = vsr.Zone.grid.offset((address - 1) * constants.block_size);
                const fault_sector = @divExact(fault_offset, constants.sector_size);
                for (t.replicas.const_slice()) |r| {
                    t.cluster.storages[r].faults.set(fault_sector);
                }
            },
        }
    }

    pub const LinkDirection = enum { bidirectional, incoming, outgoing };

    pub fn pass_all(t: *const TestReplicas, peer: ProcessSelector, direction: LinkDirection) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| {
            t.cluster.network.link_filter(path).* = LinkFilter.initFull();
        }
    }

    pub fn drop_all(t: *const TestReplicas, peer: ProcessSelector, direction: LinkDirection) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| t.cluster.network.link_filter(path).* = LinkFilter{};
    }

    pub fn pass(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        command: vsr.Command,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| t.cluster.network.link_filter(path).insert(command);
    }

    pub fn drop(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        command: vsr.Command,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| t.cluster.network.link_filter(path).remove(command);
    }

    pub fn record(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        command: vsr.Command,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| t.cluster.network.link_record(path).insert(command);
    }

    pub fn replay_recorded(
        t: *const TestReplicas,
    ) void {
        t.cluster.network.replay_recorded();
    }

    // -1: no route to self.
    const paths_max = constants.members_max * (constants.members_max - 1 + constants.clients_max);

    fn peer_paths(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
    ) stdx.BoundedArray(Network.Path, paths_max) {
        var paths = stdx.BoundedArray(Network.Path, paths_max){};
        const peers = t.context.processes(peer);
        for (t.replicas.const_slice()) |a| {
            const process_a = Process{ .replica = a };
            for (peers.const_slice()) |process_b| {
                if (direction == .bidirectional or direction == .outgoing) {
                    paths.append_assume_capacity(.{ .source = process_a, .target = process_b });
                }
                if (direction == .bidirectional or direction == .incoming) {
                    paths.append_assume_capacity(.{ .source = process_b, .target = process_a });
                }
            }
        }
        return paths;
    }

    fn expect_sync_done(t: TestReplicas) !void {
        assert(t.replicas.count() > 0);

        for (t.replicas.const_slice()) |replica_index| {
            const replica: *const Cluster.Replica = &t.cluster.replicas[replica_index];
            assert(replica.sync_content_done());

            // If the replica has finished syncing, but not yet checkpointed, then it might not have
            // updated its sync_op_max.
            maybe(replica.superblock.staging.vsr_state.sync_op_max > 0);

            try t.cluster.storage_checker.replica_sync(&replica.superblock);
        }
    }
};

const TestClients = struct {
    context: *TestContext,
    cluster: *Cluster,
    clients: stdx.BoundedArray(usize, constants.clients_max),
    requests: usize = 0,

    pub fn request(t: *TestClients, requests: usize, expect_replies: usize) !void {
        assert(t.requests <= requests);
        defer assert(t.requests == requests);

        outer: while (true) {
            for (t.clients.const_slice()) |c| {
                if (t.requests == requests) break :outer;
                t.context.client_requests[c] += 1;
                t.requests += 1;
            }
        }

        const tick_max = 3_000;
        var tick: usize = 0;
        while (tick < tick_max) : (tick += 1) {
            if (t.context.tick()) tick = 0;

            for (t.clients.const_slice()) |c| {
                const client = &t.cluster.clients[c];
                if (client.request_queue.empty() and
                    t.context.client_requests[c] > client.request_number)
                {
                    assert(client.messages_available == constants.client_request_queue_max);
                    const message = client.get_message();
                    errdefer client.release(message);

                    const body_size = 123;
                    @memset(message.buffer[@sizeOf(vsr.Header)..][0..body_size], 42);
                    t.cluster.request(c, .echo, message, body_size);
                }
            }
        }
        try std.testing.expectEqual(t.replies(), expect_replies);
    }

    pub fn replies(t: *const TestClients) usize {
        var replies_total: usize = 0;
        for (t.clients.const_slice()) |c| replies_total += t.context.client_replies[c];
        return replies_total;
    }
};
