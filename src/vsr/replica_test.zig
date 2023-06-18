const std = @import("std");
const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const log = std.log.scoped(.test_replica);
const expectEqual = std.testing.expectEqual;
const allocator = std.testing.allocator;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Process = @import("../testing/cluster/message_bus.zig").Process;
const Message = @import("../message_pool.zig").MessagePool.Message;
const parse_table = @import("../testing/table.zig").parse;
const StateMachineType = @import("../testing/state_machine.zig").StateMachineType;
const Cluster = @import("../testing/cluster.zig").ClusterType(StateMachineType);
const LinkFilter = @import("../testing/cluster/network.zig").LinkFilter;
const Network = @import("../testing/cluster/network.zig").Network;

const slot_count = constants.journal_slot_count;
const checkpoint_1 = checkpoint_trigger_1 - constants.lsm_batch_multiple;
const checkpoint_2 = checkpoint_trigger_2 - constants.lsm_batch_multiple;
const checkpoint_trigger_1 = slot_count - 1;
const checkpoint_trigger_2 = slot_count + checkpoint_trigger_1 - constants.lsm_batch_multiple;
const log_level = std.log.Level.info;

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
    assert(constants.journal_slot_count == 64);
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
    try expectEqual(t.replica(.R0).open(), .ok);
    try expectEqual(t.replica(.R0).status(), .recovering_head);
    try expectEqual(t.replica(.R1).open(), .ok);
    try c.request(24, 20);
    // With the aid of the last replica, the cluster can recover.
    try expectEqual(t.replica(.R2).open(), .ok);
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
    try expectEqual(t.replica(.R_).open(), .ok);
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
    try expectEqual(t.replica(.R0).open(), .ok);

    try c.request(21, 21);
    try expectEqual(t.replica(.R_).commit(), 21);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt checkpoint…head)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    // Trigger the first checkpoint.
    try c.request(checkpoint_trigger_1, checkpoint_trigger_1);
    t.replica(.R0).stop();

    // Corrupt op_checkpoint (59) and all ops that follow.
    var slot: usize = slot_count - constants.lsm_batch_multiple - 1;
    while (slot < slot_count) : (slot += 1) {
        t.replica(.R0).corrupt(.{ .wal_prepare = slot });
    }
    try expectEqual(t.replica(.R0).open(), .ok);
    try expectEqual(t.replica(.R0).status(), .recovering_head);

    try c.request(slot_count, slot_count);
    try expectEqual(t.replica(.R0).status(), .normal);
    t.replica(.R1).stop();
    try c.request(slot_count + 1, slot_count + 1);
}

test "Cluster: recovery: WAL prepare corruption (R=1, between checkpoint and head)" {
    // R=1 can never recover if a WAL-prepare is corrupt.
    const t = try TestContext.init(.{ .replica_count = 1 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_prepare = 15 });
    try expectEqual(t.replica(.R0).open(), .WALCorrupt);
}

test "Cluster: recovery: WAL header corruption (R=1)" {
    // R=1 locally repairs WAL-header corruption.
    const t = try TestContext.init(.{ .replica_count = 1 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_header = 15 });
    try expectEqual(t.replica(.R0).open(), .ok);
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
    try expectEqual(t.replica(.R0).open(), .ok);
    try c.request(30, 30);
    try expectEqual(t.replica(.R0).commit(), 30);
    try expectEqual(t.replica(.S0).commit(), 30);
}

test "Cluster: network: partition 2-1 (isolate backup, symmetric)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.B2).drop_all(.__, .bidirectional);
    try c.request(40, 40);
    try expectEqual(t.replica(.A0).commit(), 40);
    try expectEqual(t.replica(.B1).commit(), 40);
    try expectEqual(t.replica(.B2).commit(), 20);
}

test "Cluster: network: partition 2-1 (isolate backup, asymmetric, send-only)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.B2).drop_all(.__, .incoming);
    try c.request(40, 40);
    try expectEqual(t.replica(.A0).commit(), 40);
    try expectEqual(t.replica(.B1).commit(), 40);
    try expectEqual(t.replica(.B2).commit(), 20);
}

test "Cluster: network: partition 2-1 (isolate backup, asymmetric, receive-only)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    t.replica(.B2).drop_all(.__, .outgoing);
    try c.request(40, 40);
    try expectEqual(t.replica(.A0).commit(), 40);
    try expectEqual(t.replica(.B1).commit(), 40);
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
    // TODO: https://github.com/tigerbeetledb/tigerbeetle/issues/444
    // try c.request(40, 40);
    try c.request(40, 20);
}

test "Cluster: network: partititon client-primary (asymmetric, drop requests)" {
    // Primary cannot receive messages from the clients.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);

    t.replica(.A0).drop_all(.C_, .incoming);
    // TODO: https://github.com/tigerbeetledb/tigerbeetle/issues/444
    // try c.request(40, 40);
    try c.request(40, 20);
}

test "Cluster: network: partititon client-primary (asymmetric, drop replies)" {
    // Clients cannot receive replies from the primary, but they receive replies from a backup.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);

    t.replica(.A0).drop_all(.C_, .outgoing);
    // TODO: https://github.com/tigerbeetledb/tigerbeetle/issues/444
    // try c.request(40, 40);
    try c.request(40, 20);
}

test "Cluster: repair: partition 2-1, then backup fast-forward 1 checkpoint" {
    // A backup that has fallen behind by two checkpoints can catch up, without using state sync.
    // TODO(State Sync): How to assert that state sync is not used?
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

    r_lag.pass_all(.__, .bidirectional);
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

    try c.request(checkpoint_trigger_1 - 1, checkpoint_trigger_1 - 1);
    try expectEqual(a0.commit(), checkpoint_trigger_1 - 1);
    try expectEqual(b1.commit(), 20);
    try expectEqual(b2.commit(), checkpoint_trigger_1 - 1);

    b2.drop(.__, .incoming, .commit);
    try c.request(checkpoint_trigger_1 + 1, checkpoint_trigger_1 + 1);
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
    try expectEqual(b1.commit(), checkpoint_trigger_1);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);
}

test "Cluster: repair: view-change, new-primary lagging behind checkpoint, truncate all post-checkpoint ops" {
    const t = try TestContext.init(.{
        .replica_count = 3,
        .client_count = constants.pipeline_prepare_queue_max,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_trigger_1 - 1, checkpoint_trigger_1 - 1);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    // Drop SVCs to ensure A0 cannot abdicate (yet).
    t.replica(.R_).drop(.__, .bidirectional, .start_view_change);
    // B1 can see and ack the checkpoint-trigger prepare, but not its commit, so it cannot checkpoint.
    b1.drop(.__, .incoming, .commit);
    // B2 can see that the checkpoint-trigger prepare commits, but cannot receive the message itself.
    // It will try RSV to advances its head, but don't let it just yet.
    b2.drop(.__, .incoming, .prepare);
    b2.drop(.__, .incoming, .start_view);

    try c.request(checkpoint_trigger_1, checkpoint_trigger_1);
    try expectEqual(a0.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), 0);
    try expectEqual(b2.op_checkpoint(), 0);

    // B1 wouldn't prepare these anyway, but we prevent it from learning that the checkpoint
    // trigger is committed.
    b1.drop(.__, .incoming, .prepare);

    try c.request(checkpoint_trigger_1 + constants.pipeline_prepare_queue_max, checkpoint_trigger_1);
    try expectEqual(a0.op_head(), checkpoint_trigger_1 + constants.pipeline_prepare_queue_max);
    try expectEqual(b1.op_head(), checkpoint_trigger_1);
    try expectEqual(b2.op_head(), checkpoint_trigger_1 - 1);

    {
        // Now that A0 has a full pipeline past the checkpoint, allow B2 to advance its head via the
        // hook. But first kick it into recovering_head to force it to load the SV's headers into
        // its view_headers. Those SV headers have an op-max past the next checkpoint, so our
        // op_head does not increase beyond the checkpoint trigger.
        b2.pass(.__, .incoming, .start_view);
        b2.corrupt(.{ .wal_prepare = checkpoint_trigger_1 - 1 });

        // We must receive a prepare to trigger a view_jump, which is what will trigger the RSV.
        // But don't send an ack, since that would allow A0 to send us the next prepare,
        // which would cause us to learn that the trigger is committed,
        // which would cause us to checkpoint.
        b2.pass(.__, .incoming, .prepare);
        b2.drop(.__, .outgoing, .prepare_ok);

        // Corrupt headers & prevent header repair so that we can't commit.
        b2.corrupt(.{ .wal_header = 5 });
        b2.drop(.__, .incoming, .headers);

        b2.stop();
        try expectEqual(b2.open(), .ok);
        try expectEqual(b2.status(), .recovering_head);
        t.run();

        try expectEqual(b2.status(), .normal);
        try expectEqual(b2.op_checkpoint(), 0);
        try expectEqual(b2.op_head(), checkpoint_trigger_1);
        try expectEqual(b2.view_headers()[0].op, checkpoint_trigger_1 + constants.pipeline_prepare_queue_max);
    }

    // Take down A0 and allow a view-change.
    // B2 has not repaired yet, so it reuses the view_headers that it got from the SV (with headers
    // from the next wrap).
    // TODO Explicit code coverage marks: This hits the "discarded uncommitted ops after trigger"
    // log line in on_do_view_change().
    a0.stop();
    b1.pass_all(.__, .incoming);
    b2.pass_all(.__, .incoming);
    t.run();

    try expectEqual(b1.status(), .normal);
    try expectEqual(b2.status(), .normal);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);
    try expectEqual(b2.op_checkpoint(), checkpoint_1);
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
    // (Its easier than figuring out the reply's actual slot.)
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
    try expectEqual(t.replica(.R0).open(), .ok);
    try expectEqual(t.replica(.R1).open(), .ok);
    // TODO Explicit code coverage marks: This should hit the "quorum received, awaiting repair"
    // log line in on_do_view_change().
    t.run();
    try expectEqual(t.replica(.R0).status(), .view_change);
    try expectEqual(t.replica(.R1).status(), .view_change);

    // R2 provides the missing header, allowing the view-change to succeed.
    try expectEqual(t.replica(.R2).open(), .ok);
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

    try expectEqual(t.replica(.R_).open(), .ok);
    // TODO Explicit code coverage marks: This should hit the "quorum received, deadlocked"
    // log line in on_do_view_change().
    t.run();
    try expectEqual(t.replica(.R_).status(), .view_change);
}

test "Cluster: reconfiguration: smoke" {
    const t = try TestContext.init(.{
        .replica_count = 3,
        .standby_count = 3,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).epoch(), 0);
    try expectEqual(t.replica(.R_).view(), 1);
    try expectEqual(t.replica(.R_).commit(), 20);
    try c.request_reconfigure(&.{ 3, 4, 5, 0, 1, 2 });

    try c.request(30, 30);
    try expectEqual(t.replica(.R_).epoch(), 1);
    try expectEqual(t.replica(.R_).view(), 1);
    try expectEqual(t.replica(.R_).commit(), 30);
}

test "Cluster: reconfiguration: replica misses reconfiguration" {
    const t = try TestContext.init(.{
        .replica_count = 3,
        .standby_count = 3,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).epoch(), 0);
    try expectEqual(t.replica(.R_).view(), 1);
    try expectEqual(t.replica(.R_).commit(), 20);

    t.replica(.R0).stop();
    try c.request_reconfigure(&.{ 3, 4, 5, 0, 1, 2 });
    try c.request(25, 25);

    try expectEqual(t.replica(.R0).epoch(), 0);
    try expectEqual(t.replica(.R0).open(), .ok);
    t.run();
    try expectEqual(t.replica(.R0).epoch(), 1);

    try c.request(30, 30);
    try expectEqual(t.replica(.R_).epoch(), 1);
    try expectEqual(t.replica(.R_).view(), 1);
}

test "Cluster: reconfiguration: standby misses reconfiguration" {
    const t = try TestContext.init(.{
        .replica_count = 3,
        .standby_count = 3,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(20, 20);
    try expectEqual(t.replica(.R_).epoch(), 0);
    try expectEqual(t.replica(.R_).view(), 1);
    try expectEqual(t.replica(.R_).commit(), 20);

    t.replica(.S0).stop();
    try c.request_reconfigure(&.{ 3, 4, 5, 0, 1, 2 });
    try c.request(25, 25);
    try expectEqual(t.replica(.R_).epoch(), 1);
    try expectEqual(t.replica(.R_).view(), 1);

    try expectEqual(t.replica(.S0).epoch(), 0);
    try expectEqual(t.replica(.S0).open(), .ok);
    t.run();
    try expectEqual(t.replica(.S0).epoch(), 1);

    try c.request(30, 30);
    try expectEqual(t.replica(.R_).epoch(), 1);
    try expectEqual(t.replica(.R_).view(), 1);
    try expectEqual(t.replica(.S_).epoch(), 1);
    try expectEqual(t.replica(.S_).view(), 1);
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
            .storage_size_limit = vsr.sector_floor(
                constants.storage_size_max - random.uintLessThan(u64, constants.storage_size_max / 10),
            ),
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
            },
            .state_machine = .{},
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
        var replica_indexes = std.BoundedArray(u8, constants.replicas_max){ .buffer = undefined };
        // TODO Zig: This should iterate over values instead of pointers once the miscompilation
        // segfault is fixed.
        for (replica_processes.constSlice()) |*p| replica_indexes.appendAssumeCapacity(p.replica);
        return TestReplicas{
            .context = t,
            .cluster = t.cluster,
            .replicas = replica_indexes,
        };
    }

    pub fn clients(t: *TestContext, index: usize, count: usize) TestClients {
        var client_indexes = std.BoundedArray(usize, constants.clients_max){ .buffer = undefined };
        var i = index;
        while (i < index + count) : (i += 1) client_indexes.appendAssumeCapacity(i);
        return TestClients{
            .context = t,
            .cluster = t.cluster,
            .clients = client_indexes,
        };
    }

    pub fn run(t: *TestContext) void {
        const tick_max = 2_400;
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

    fn on_client_reply(cluster: *Cluster, client: usize, request: *Message, reply: *Message) void {
        _ = request;
        _ = reply;
        const t = @ptrCast(*TestContext, @alignCast(@alignOf(TestContext), cluster.context.?));
        t.client_replies[client] += 1;
    }

    const ProcessList = std.BoundedArray(Process, constants.replicas_max + constants.clients_max);

    fn processes(t: *const TestContext, selector: ProcessSelector) ProcessList {
        const replica_count = t.cluster.options.replica_count;

        var view: u32 = 0;
        for (t.cluster.replicas) |*r| view = @maximum(view, r.view);

        var array = ProcessList{ .buffer = undefined };
        switch (selector) {
            .R0 => array.appendAssumeCapacity(.{ .replica = 0 }),
            .R1 => array.appendAssumeCapacity(.{ .replica = 1 }),
            .R2 => array.appendAssumeCapacity(.{ .replica = 2 }),
            .R3 => array.appendAssumeCapacity(.{ .replica = 3 }),
            .R4 => array.appendAssumeCapacity(.{ .replica = 4 }),
            .R5 => array.appendAssumeCapacity(.{ .replica = 5 }),
            .S0 => array.appendAssumeCapacity(.{ .replica = replica_count + 0 }),
            .S1 => array.appendAssumeCapacity(.{ .replica = replica_count + 1 }),
            .S2 => array.appendAssumeCapacity(.{ .replica = replica_count + 2 }),
            .S3 => array.appendAssumeCapacity(.{ .replica = replica_count + 3 }),
            .S4 => array.appendAssumeCapacity(.{ .replica = replica_count + 4 }),
            .S5 => array.appendAssumeCapacity(.{ .replica = replica_count + 5 }),
            .A0 => array.appendAssumeCapacity(.{ .replica = @intCast(u8, (view + 0) % replica_count) }),
            .B1 => array.appendAssumeCapacity(.{ .replica = @intCast(u8, (view + 1) % replica_count) }),
            .B2 => array.appendAssumeCapacity(.{ .replica = @intCast(u8, (view + 2) % replica_count) }),
            .B3 => array.appendAssumeCapacity(.{ .replica = @intCast(u8, (view + 3) % replica_count) }),
            .B4 => array.appendAssumeCapacity(.{ .replica = @intCast(u8, (view + 4) % replica_count) }),
            .B5 => array.appendAssumeCapacity(.{ .replica = @intCast(u8, (view + 5) % replica_count) }),
            .__, .R_, .S_, .C_ => {
                if (selector == .__ or selector == .R_) {
                    for (t.cluster.replicas[0..replica_count]) |_, i| {
                        array.appendAssumeCapacity(.{ .replica = @intCast(u8, i) });
                    }
                }
                if (selector == .__ or selector == .S_) {
                    for (t.cluster.replicas[replica_count..]) |_, i| {
                        array.appendAssumeCapacity(.{ .replica = @intCast(u8, replica_count + i) });
                    }
                }
                if (selector == .__ or selector == .C_) {
                    for (t.cluster.clients) |*client| {
                        array.appendAssumeCapacity(.{ .client = client.id });
                    }
                }
            },
        }
        assert(array.len > 0);
        return array;
    }
};

const TestReplicas = struct {
    context: *TestContext,
    cluster: *Cluster,
    replicas: std.BoundedArray(u8, constants.replicas_max),

    pub fn stop(t: *TestReplicas) void {
        for (t.replicas.constSlice()) |r| {
            t.cluster.crash_replica(r);
        }
    }

    // TODO(Zig) Return ?anyerror when "unable to make error union out of null literal" is fixed.
    pub fn open(t: *TestReplicas) enum { ok, WALInvalid, WALCorrupt } {
        for (t.replicas.constSlice()) |r| {
            t.cluster.restart_replica(r) catch |err| {
                assert(t.replicas.len == 1);
                return switch (err) {
                    error.WALCorrupt => .WALCorrupt,
                    error.WALInvalid => .WALInvalid,
                    else => @panic("unexpected error"),
                };
            };
        }
        return .ok;
    }

    pub fn index(t: *const TestReplicas) u8 {
        assert(t.replicas.len == 1);
        return t.replicas.get(0);
    }

    fn get(
        t: *const TestReplicas,
        comptime field: std.meta.FieldEnum(Cluster.Replica),
    ) std.meta.fieldInfo(Cluster.Replica, field).field_type {
        var value_all: ?std.meta.fieldInfo(Cluster.Replica, field).field_type = null;
        for (t.replicas.constSlice()) |r| {
            const replica = &t.cluster.replicas[r];
            const value = @field(replica, @tagName(field));
            if (value_all) |all| {
                assert(all == value);
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

    pub fn epoch(t: *const TestReplicas) u32 {
        return t.get(.epoch);
    }

    pub fn op_head(t: *const TestReplicas) u64 {
        return t.get(.op);
    }

    pub fn commit(t: *const TestReplicas) u64 {
        return t.get(.commit_min);
    }

    const Role = enum { primary, backup, standby };

    pub fn role(t: *const TestReplicas) Role {
        var role_all: ?Role = null;
        for (t.replicas.constSlice()) |r| {
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

    pub fn op_checkpoint(t: *const TestReplicas) u64 {
        var checkpoint_all: ?u64 = null;
        for (t.replicas.constSlice()) |r| {
            const replica = &t.cluster.replicas[r];
            assert(checkpoint_all == null or checkpoint_all.? == replica.op_checkpoint());
            checkpoint_all = replica.op_checkpoint();
        }
        return checkpoint_all.?;
    }

    pub fn view_headers(t: *const TestReplicas) []const vsr.Header {
        assert(t.replicas.len == 1);
        return t.cluster.replicas[t.replicas.get(0)].view_headers.array.constSlice();
    }

    pub fn corrupt(
        t: *const TestReplicas,
        target: union(enum) {
            wal_header: usize, // slot
            wal_prepare: usize, // slot
            client_reply: usize, // slot
        },
    ) void {
        switch (target) {
            .wal_header => |slot| {
                const fault_offset = vsr.Zone.wal_headers.offset(slot * @sizeOf(vsr.Header));
                for (t.replicas.constSlice()) |r| {
                    t.cluster.storages[r].memory[fault_offset] +%= 1;
                }
            },
            .wal_prepare => |slot| {
                const fault_offset = vsr.Zone.wal_prepares.offset(slot * constants.message_size_max);
                const fault_sector = @divExact(fault_offset, constants.sector_size);
                for (t.replicas.constSlice()) |r| {
                    t.cluster.storages[r].faults.set(fault_sector);
                }
            },
            .client_reply => |slot| {
                const fault_offset = vsr.Zone.client_replies.offset(slot * constants.message_size_max);
                const fault_sector = @divExact(fault_offset, constants.sector_size);
                for (t.replicas.constSlice()) |r| {
                    t.cluster.storages[r].faults.set(fault_sector);
                }
            },
        }
    }

    pub const LinkDirection = enum { bidirectional, incoming, outgoing };

    pub fn pass_all(t: *const TestReplicas, peer: ProcessSelector, direction: LinkDirection) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.constSlice()) |path| {
            t.cluster.network.link_filter(path).* = LinkFilter.initFull();
        }
    }

    pub fn drop_all(t: *const TestReplicas, peer: ProcessSelector, direction: LinkDirection) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.constSlice()) |path| t.cluster.network.link_filter(path).* = LinkFilter{};
    }

    pub fn pass(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        command: vsr.Command,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.constSlice()) |path| t.cluster.network.link_filter(path).insert(command);
    }

    pub fn drop(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        command: vsr.Command,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.constSlice()) |path| t.cluster.network.link_filter(path).remove(command);
    }

    // -1: no route to self.
    const paths_max = constants.nodes_max * (constants.nodes_max - 1 + constants.clients_max);

    fn peer_paths(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
    ) std.BoundedArray(Network.Path, paths_max) {
        var paths = std.BoundedArray(Network.Path, paths_max){ .buffer = undefined };
        const peers = t.context.processes(peer);
        for (t.replicas.constSlice()) |a| {
            const process_a = Process{ .replica = a };
            // TODO Zig: This should iterate over values instead of pointers once the miscompilation
            // segfault is fixed.
            for (peers.constSlice()) |*process_b| {
                if (direction == .bidirectional or direction == .outgoing) {
                    paths.appendAssumeCapacity(.{ .source = process_a, .target = process_b.* });
                }
                if (direction == .bidirectional or direction == .incoming) {
                    paths.appendAssumeCapacity(.{ .source = process_b.*, .target = process_a });
                }
            }
        }
        return paths;
    }
};

const TestClients = struct {
    context: *TestContext,
    cluster: *Cluster,
    clients: std.BoundedArray(usize, constants.clients_max),
    requests: usize = 0,

    pub fn request(t: *TestClients, requests: usize, expect_replies: usize) !void {
        assert(t.requests <= requests);
        defer assert(t.requests == requests);

        outer: while (true) {
            for (t.clients.constSlice()) |c| {
                if (t.requests == requests) break :outer;
                t.context.client_requests[c] += 1;
                t.requests += 1;
            }
        }

        const tick_max = 2_400;
        var tick: usize = 0;
        while (tick < tick_max) : (tick += 1) {
            if (t.context.tick()) tick = 0;

            for (t.clients.constSlice()) |c| {
                const client = &t.cluster.clients[c];
                if (client.request_queue.empty() and
                    t.context.client_requests[c] > client.request_number)
                {
                    const message = client.get_message();
                    defer client.unref(message);

                    const body_size = 123;
                    std.mem.set(u8, message.buffer[@sizeOf(vsr.Header)..][0..body_size], 42);
                    t.cluster.request(c, .echo, message, body_size);
                }
            }
        }
        try std.testing.expectEqual(t.replies(), expect_replies);
    }

    pub fn request_reconfigure(t: *TestClients, replicas: []const u8) !void {
        var configuration = [_]u128{0} ** constants.nodes_max;
        for (replicas) |index, i| {
            configuration[i] = t.cluster.replicas[index].replica_id;
        }

        for (t.clients.constSlice()) |c| {
            t.context.client_requests[c] += 1;
            t.requests += 1;

            const client = &t.cluster.clients[c];
            const message = client.get_message();
            defer client.unref(message);

            const body_size = @sizeOf([constants.nodes_max]u128);
            stdx.copy_disjoint(.inexact, u8, message.buffer[@sizeOf(vsr.Header)..], std.mem.asBytes(&configuration));
            t.cluster.request(c, .reconfigure, message, body_size);
            break;
        }

        const tick_max = 2_000;
        var tick: usize = 0;
        while (tick < tick_max) : (tick += 1) {
            if (t.context.tick()) tick = 0;
        }
        t.cluster.update_client_epoch(replicas);
    }

    pub fn replies(t: *const TestClients) usize {
        var replies_total: usize = 0;
        for (t.clients.constSlice()) |c| replies_total += t.context.client_replies[c];
        return replies_total;
    }
};
