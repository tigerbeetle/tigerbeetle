//! Track corrupt/missing grid blocks.
//!
//! - The GridBlocksMissing is LSM-aware: it can repair entire tables.
//! - The GridBlocksMissing is shared by all Trees.
//! - The GridBlocksMissing is "coherent" – that is, all of the blocks in the queue belong in the
//!   replica's current checkpoint:
//!   - The GridBlocksMissing will not repair freed blocks.
//!   - The GridBlocksMissing will repair released blocks, until they are freed at the checkpoint.
//! - GridBlocksMissing.sync_table() is called immediately after superblock sync.
//! - GridBlocksMissing.repair_block() is called by the grid when non-repair reads encounter
//!   corrupt blocks.
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const constants = @import("../constants.zig");
const stdx = @import("stdx");
const schema = @import("../lsm/schema.zig");
const vsr = @import("../vsr.zig");

const QueueType = @import("../queue.zig").QueueType;
const BlockPtrConst = *align(constants.sector_size) const [constants.block_size]u8;

pub const GridBlocksMissing = struct {
    /// A block is removed from the collection when:
    /// - the block's write completes, or
    /// - the block is released and the release is checkpointed, or
    /// - the grid is canceled.
    ///
    /// The map is keyed by block address.
    const FaultyBlocks = std.AutoArrayHashMapUnmanaged(u64, FaultyBlock);

    const FaultyBlock = struct {
        checksum: u128,
        cause: Cause,
        /// Transitions:
        /// - Initial state is `waiting`.
        /// - `waiting → writing` when the block arrives and begins to repair.
        /// - `writing → aborting` when checkpoint becomes durable and the (writing) block is to be
        ///    freed.
        state: enum { waiting, writing, aborting } = .waiting,

        const Cause = union(enum) {
            /// Repair a single block.
            ///
            /// Originates from one of:
            /// - the grid scrubber
            /// - a grid read during prefetch/compaction
            /// - a grid read while opening the grid/forest
            repair,
            /// State syncing the index or a value block of a table.
            /// NB: when a replica decides to sync a block, it might already be repairing.
            sync: struct {
                table: *RepairTable,
                block: union(enum) {
                    table_index,
                    /// The index of the value block within the index block.
                    table_value: u32,
                },
            },
        };
    };

    pub const RepairTable = struct {
        table_info: schema.ManifestNode.TableInfo,
        /// Invariants:
        /// - value_blocks_received.count < table_blocks_total
        /// - value_blocks_received.capacity = constants.lsm_table_value_blocks_max
        /// TODO(Congestion control): This bitset is currently used only for extra validation.
        /// Eventually we should request tables using this + EWAH encoding, instead of
        /// block-by-block.
        value_blocks_received: *std.DynamicBitSetUnmanaged,
        /// This count includes the index block.
        /// Invariants:
        /// - table_blocks_written ≤ table_blocks_total
        table_blocks_written: u32 = 0,
        /// When null, the table is awaiting an index block.
        /// When non-null, the table is awaiting value blocks.
        /// This count includes the index block.
        table_blocks_total: ?u32 = null,
        /// For `faulty_tables`/`faulty_tables_free` queues.
        link: QueueType(RepairTable).Link = .{},
    };

    pub const Options = struct {
        /// Lower-bound for the limit of concurrent repair_block()'s available.
        blocks_max: usize,
        /// Maximum number of concurrent sync_table()'s.
        tables_max: usize,
    };

    options: Options,

    /// Invariants:
    /// - For every block address in faulty_blocks, ¬free_set.is_free(address).
    faulty_blocks: FaultyBlocks,
    /// Index within `faulty_blocks`, used to cycle through block-repair requests.
    ///
    /// Invariants:
    /// - faulty_blocks.count() > 0 implies faulty_blocks_repair_index < faulty_blocks.count()
    /// - faulty_blocks.count() = 0 implies faulty_blocks_repair_index = faulty_blocks.count()
    faulty_blocks_repair_index: usize = 0,

    /// On `sync_jump_commence()` and `sync_complete()`, swap this with `faulty_blocks` so that the
    /// (possibly invalid) table blocks don't interfere.
    ///
    /// See state.sync_jump for more information.
    syncing_faulty_blocks: FaultyBlocks,

    /// Invariants:
    /// - enqueued_blocks_sync + enqueued_blocks_repair =
    ///   faulty_blocks.count() + syncing_faulty_blocks.count()
    /// - enqueued_blocks_sync ≤ options.tables_max * lsm_table_content_blocks_max
    enqueued_blocks_repair: usize = 0,
    enqueued_blocks_sync: usize = 0,

    /// Invariants:
    /// - For every index address in faulty_tables: ¬free_set.is_free(address).
    /// - A given RepairTable is never in both `faulty_tables` and `faulty_tables_free`.
    /// - `faulty_tables` does not contain multiple items with the same underlying table
    ///   (address/checksum).
    faulty_tables: QueueType(RepairTable) = QueueType(RepairTable).init(.{
        .name = "grid_missing_blocks_tables",
    }),
    faulty_tables_free: QueueType(RepairTable) = QueueType(RepairTable).init(.{
        .name = "grid_missing_blocks_tables_free",
    }),

    state: union(enum) {
        repairing,
        /// Set while the replica is syncing its superblock and opening its grid/forest.
        ///
        /// While `state=sync_jump`, only repair single blocks, not tables. Table blocks are
        /// temporarily relegated to syncing_faulty_blocks:
        /// - When state≠sync_jump, faulty_blocks=big and syncing_faulty_blocks=small/unused.
        /// - When state=sync_jump, faulty_blocks=small and syncing_faulty_blocks=big.
        ///
        /// When we finish with state=sync_jump:
        /// - For any table belonging in the new checkpoint: pick up repair where we left off.
        /// - For any table not belonging in the new checkpoint: cancel.
        sync_jump,
        checkpoint_durable: struct {
            /// The number of faulty_blocks with state=aborting.
            aborting: u64,
        },
    } = .repairing,

    pub fn init(
        allocator: std.mem.Allocator,
        options: Options,
    ) error{OutOfMemory}!GridBlocksMissing {
        var faulty_blocks = FaultyBlocks{};
        errdefer faulty_blocks.deinit(allocator);

        var syncing_faulty_blocks = FaultyBlocks{};
        errdefer syncing_faulty_blocks.deinit(allocator);

        try faulty_blocks.ensureTotalCapacity(
            allocator,
            options.blocks_max + options.tables_max * constants.lsm_table_value_blocks_max,
        );
        // During state=sync_jump, we only need to sync single blocks, not full tables.
        // (This sounds backwards! But the reason is that state=sync_jump corresponds to grid
        // cancellation + checkpoint replacement, not table/content sync. We repair missing blocks
        // from the free set and checkpoint trailers.)
        try syncing_faulty_blocks.ensureTotalCapacity(allocator, options.blocks_max);

        return GridBlocksMissing{
            .options = options,
            .faulty_blocks = faulty_blocks,
            .syncing_faulty_blocks = syncing_faulty_blocks,
        };
    }

    pub fn deinit(queue: *GridBlocksMissing, allocator: std.mem.Allocator) void {
        queue.syncing_faulty_blocks.deinit(allocator);
        queue.faulty_blocks.deinit(allocator);

        queue.* = undefined;
    }

    pub fn verify(queue: *const GridBlocksMissing) void {
        assert(queue.faulty_blocks.count() + queue.syncing_faulty_blocks.count() ==
            queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);
        assert(queue.faulty_blocks_repair_index == 0 or
            queue.faulty_blocks_repair_index < queue.faulty_blocks.count());

        var enqueued_blocks_repair: u32 = 0;
        var enqueued_blocks_sync: u32 = 0;
        var enqueued_blocks_aborting: u32 = 0;
        for ([_]FaultyBlocks{
            queue.faulty_blocks,
            queue.syncing_faulty_blocks,
        }) |faulty_blocks| {
            for (faulty_blocks.values()) |fault| {
                switch (fault.cause) {
                    .sync => enqueued_blocks_sync += 1,
                    .repair => enqueued_blocks_repair += 1,
                }
                enqueued_blocks_aborting += @intFromBool(fault.state == .aborting);

                switch (fault.cause) {
                    .repair => {},
                    .sync => |sync| {
                        // These are not exclusive because the replica may reuse a RepairTable while
                        // we are still aborting the old blocks.
                        assert(queue.faulty_tables.contains(sync.table) or
                            (fault.state == .aborting));
                    },
                }
            }
        }
        assert(queue.enqueued_blocks_repair == enqueued_blocks_repair);
        assert(queue.enqueued_blocks_sync == enqueued_blocks_sync);
        if (enqueued_blocks_sync == 0) assert(queue.faulty_tables.empty());

        if (queue.state == .checkpoint_durable) {
            assert(enqueued_blocks_aborting == queue.state.checkpoint_durable.aborting);
        } else {
            assert(enqueued_blocks_aborting == 0);
        }

        assert(queue.syncing_faulty_blocks.capacity() != queue.faulty_blocks.capacity());
        if (queue.state == .sync_jump) {
            assert(queue.syncing_faulty_blocks.capacity() > queue.faulty_blocks.capacity());
        } else {
            assert(queue.syncing_faulty_blocks.capacity() < queue.faulty_blocks.capacity());
            assert(queue.syncing_faulty_blocks.count() == 0);
        }

        var faulty_tables_free = queue.faulty_tables_free.iterate();
        while (faulty_tables_free.next()) |table_free| {
            assert(!queue.faulty_tables.contains(table_free));
        }
    }

    /// Note that returning `null` doesn't necessarily indicate that there are no more blocks.
    pub fn next_request(queue: *GridBlocksMissing) ?vsr.BlockRequest {
        assert(queue.faulty_blocks.count() > 0);
        assert(queue.faulty_blocks_repair_index < queue.faulty_blocks.count());

        const fault_addresses = queue.faulty_blocks.keys();
        const fault_data = queue.faulty_blocks.values();
        const fault_index = queue.faulty_blocks_repair_index;

        queue.faulty_blocks_repair_index = (fault_index + 1) % queue.faulty_blocks.count();

        return switch (fault_data[fault_index].state) {
            .waiting => .{
                .block_address = fault_addresses[fault_index],
                .block_checksum = fault_data[fault_index].checksum,
            },
            .writing => null,
            .aborting => null,
        };
    }

    pub fn reclaim_table(queue: *GridBlocksMissing) ?*RepairTable {
        const table = queue.faulty_tables_free.pop() orelse return null;
        table.value_blocks_received.unsetAll();
        return table;
    }

    /// Count the number of *non-table* block repairs available.
    pub fn repair_blocks_available(queue: *const GridBlocksMissing) usize {
        assert(queue.faulty_tables.count() <= queue.options.tables_max);
        assert(queue.faulty_blocks.count() + queue.syncing_faulty_blocks.count() ==
            queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);
        assert(queue.enqueued_blocks_sync <=
            queue.options.tables_max * constants.lsm_table_value_blocks_max);

        if (queue.state == .sync_jump) {
            const faulty_blocks_free =
                queue.faulty_blocks.capacity() -
                queue.enqueued_blocks_repair;
            return faulty_blocks_free;
        } else {
            const faulty_blocks_free =
                queue.faulty_blocks.capacity() -
                queue.enqueued_blocks_repair -
                queue.options.tables_max * constants.lsm_table_value_blocks_max;
            return faulty_blocks_free;
        }
    }

    /// Queue a faulty block to request from the cluster and repair.
    pub fn repair_block(queue: *GridBlocksMissing, address: u64, checksum: u128) void {
        assert(queue.repair_blocks_available() > 0);
        assert(queue.faulty_tables.count() <= queue.options.tables_max);
        assert(queue.faulty_blocks.count() + queue.syncing_faulty_blocks.count() ==
            queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);

        const enqueue = queue.enqueue_faulty_block(address, checksum, .repair);
        switch (enqueue) {
            .insert => {},
            .duplicate => {},
            .replace => assert(queue.state == .sync_jump),
        }
    }

    pub fn sync_table(
        queue: *GridBlocksMissing,
        table: *RepairTable,
        table_bitset: *std.DynamicBitSetUnmanaged,
        table_info: *const schema.ManifestNode.TableInfo,
    ) enum { insert, duplicate } {
        assert(queue.state == .repairing or queue.state == .checkpoint_durable);
        assert(queue.faulty_tables.count() < queue.options.tables_max);
        assert(queue.faulty_blocks.count() + queue.syncing_faulty_blocks.count() ==
            queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);
        assert(table_bitset.capacity() == constants.lsm_table_value_blocks_max);
        assert(table_bitset.count() == 0);

        const address = table_info.address;
        const checksum = table_info.checksum;

        var tables = queue.faulty_tables.iterate();
        while (tables.next()) |queue_table| {
            assert(queue_table != table);
            assert(queue_table.value_blocks_received != table_bitset);

            if (queue_table.table_info.address == address) {
                // The ForestTableIterator does not repeat tables *except* when the table was first
                // encountered at level L, and then it was re-encountered having moved to a deeper
                // level (L+1, etc).
                assert(queue_table.table_info.checksum == checksum);
                return .duplicate;
            }
        }

        table.* = .{
            .table_info = table_info.*,
            .value_blocks_received = table_bitset,
        };
        queue.faulty_tables.push(table);

        const enqueue = queue.enqueue_faulty_block(address, checksum, .{
            .sync = .{ .table = table, .block = .table_index },
        });
        assert(enqueue == .insert or enqueue == .replace);

        return .insert;
    }

    fn enqueue_faulty_block(
        queue: *GridBlocksMissing,
        address: u64,
        checksum: u128,
        cause: FaultyBlock.Cause,
    ) union(enum) {
        insert,
        replace: *FaultyBlock,
        duplicate,
    } {
        assert(queue.faulty_tables.count() <= queue.options.tables_max);
        assert(queue.faulty_blocks.count() + queue.syncing_faulty_blocks.count() ==
            queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);

        defer {
            assert(queue.faulty_blocks.count() + queue.syncing_faulty_blocks.count() ==
                queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);
        }

        const fault_result = queue.faulty_blocks.getOrPutAssumeCapacity(address);
        if (fault_result.found_existing) {
            const fault = fault_result.value_ptr;
            assert(fault.checksum == checksum);
            assert(fault.state != .aborting);

            switch (cause) {
                .repair => return .duplicate,
                .sync => {
                    // The value block may already have been queued by either the scrubber or a
                    // commit/compaction grid read.
                    assert(fault.cause == .repair);

                    queue.enqueued_blocks_repair -= 1;
                    queue.enqueued_blocks_sync += 1;
                    fault.cause = cause;
                    return .{ .replace = fault };
                },
            }
        } else {
            switch (cause) {
                .repair => queue.enqueued_blocks_repair += 1,
                .sync => queue.enqueued_blocks_sync += 1,
            }

            fault_result.value_ptr.* = .{
                .checksum = checksum,
                .cause = cause,
            };
            return .insert;
        }
    }

    pub fn repairing_blocks(queue: *const GridBlocksMissing) bool {
        return queue.repairing_tables() or queue.enqueued_blocks_repair > 0;
    }

    pub fn repairing_tables(queue: *const GridBlocksMissing) bool {
        return queue.state != .sync_jump and queue.enqueued_blocks_sync > 0;
    }

    pub fn block_waiting(queue: *const GridBlocksMissing, address: u64, checksum: u128) bool {
        const fault_index = queue.faulty_blocks.getIndex(address) orelse return false;
        const fault = &queue.faulty_blocks.values()[fault_index];
        return fault.checksum == checksum and fault.state == .waiting;
    }

    pub fn write_commence(queue: *GridBlocksMissing, address: u64, checksum: u128) void {
        assert(queue.block_waiting(address, checksum));
        maybe(queue.state == .checkpoint_durable);
        maybe(queue.state == .sync_jump);

        const fault_index = queue.faulty_blocks.getIndex(address).?;
        const fault = &queue.faulty_blocks.values()[fault_index];
        assert(fault.checksum == checksum);
        assert(fault.state == .waiting);
        if (queue.state == .sync_jump) assert(fault.cause == .repair);

        if (fault.cause == .sync and
            fault.cause.sync.block == .table_value)
        {
            const cause = &fault.cause.sync;
            assert(cause.table.table_blocks_written < cause.table.table_blocks_total.?);
            assert(!cause.table.value_blocks_received.isSet(cause.block.table_value));

            cause.table.value_blocks_received.set(cause.block.table_value);
        }

        fault.state = .writing;
    }

    pub fn write_complete(queue: *GridBlocksMissing, block: BlockPtrConst) void {
        const block_header = schema.header_from_block(block);
        const fault_index = queue.faulty_blocks.getIndex(block_header.address).?;
        const fault_address = queue.faulty_blocks.keys()[fault_index];
        const fault: FaultyBlock = queue.faulty_blocks.values()[fault_index];
        assert(fault_address == block_header.address);
        assert(fault.checksum == block_header.checksum);
        assert(fault.state == .aborting or fault.state == .writing);
        if (queue.state == .sync_jump) assert(fault.cause == .repair);

        queue.release_fault(fault_index);

        if (fault.state == .aborting) {
            queue.state.checkpoint_durable.aborting -= 1;
            return;
        }

        switch (fault.cause) {
            .repair => {
                maybe(queue.state == .sync_jump);
            },
            .sync => |sync| {
                switch (sync.block) {
                    .table_index => {
                        assert(queue.state != .sync_jump);
                        assert(sync.table.value_blocks_received.count() == 0);

                        // The reason that the value blocks are queued here (when the write ends)
                        // rather than when the write begins is so that a `repair_block()` can be
                        // converted to a `sync_table()` after the former's write is already in
                        // progress.
                        queue.enqueue_table_value(fault.cause.sync.table, block);
                    },
                    .table_value => |value_index| {
                        assert(queue.state != .sync_jump);
                        assert(sync.table.value_blocks_received.isSet(value_index));
                    },
                }

                // We already received the index block.
                assert(sync.table.table_blocks_total != null);
                assert(sync.table.table_blocks_written < sync.table.table_blocks_total.?);
                assert(sync.table.value_blocks_received.count() <=
                    sync.table.table_blocks_total.? - 1);

                sync.table.table_blocks_written += 1;
                if (sync.table.table_blocks_written == sync.table.table_blocks_total.?) {
                    queue.faulty_tables.remove(sync.table);
                    queue.faulty_tables_free.push(sync.table);
                }
            },
        }
    }

    fn enqueue_table_value(
        queue: *GridBlocksMissing,
        table: *RepairTable,
        index_block: BlockPtrConst,
    ) void {
        assert(queue.state != .sync_jump);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);
        assert(table.table_blocks_total == null);
        assert(table.table_blocks_written == 0);
        assert(table.value_blocks_received.count() == 0);

        const index_schema = schema.TableIndex.from(index_block);
        const index_block_header = schema.header_from_block(index_block);
        assert(index_block_header.address == table.table_info.address);
        assert(index_block_header.checksum == table.table_info.checksum);
        assert(index_block_header.block_type == .index);

        table.table_blocks_total = index_schema.value_blocks_used(index_block) + 1;

        for (
            index_schema.value_addresses_used(index_block),
            index_schema.value_checksums_used(index_block),
            0..,
        ) |address, checksum, index| {
            const enqueue = queue.enqueue_faulty_block(
                address,
                checksum.value,
                .{ .sync = .{ .table = table, .block = .{ .table_value = @intCast(index) } } },
            );

            if (enqueue == .replace) {
                if (enqueue.replace.state == .writing) {
                    table.value_blocks_received.set(index);
                }
            } else {
                assert(enqueue == .insert);
            }
        }
    }

    fn release_fault(queue: *GridBlocksMissing, fault_index: usize) void {
        assert(queue.faulty_blocks_repair_index < queue.faulty_blocks.count());

        switch (queue.faulty_blocks.values()[fault_index].cause) {
            .repair => queue.enqueued_blocks_repair -= 1,
            .sync => queue.enqueued_blocks_sync -= 1,
        }

        queue.faulty_blocks.swapRemoveAt(fault_index);

        if (queue.faulty_blocks_repair_index == queue.faulty_blocks.count()) {
            queue.faulty_blocks_repair_index = 0;
        }
    }

    pub fn cancel(queue: *GridBlocksMissing) void {
        queue.verify();
        defer queue.verify();

        for (queue.faulty_blocks.values()) |*fault| {
            switch (fault.state) {
                .aborting => unreachable,
                .waiting => {},
                .writing => {
                    // Due to Grid.cancel() this write may not actually take place.
                    fault.state = .waiting;

                    if (fault.cause == .sync and
                        fault.cause.sync.block == .table_value)
                    {
                        const sync = &fault.cause.sync;
                        assert(sync.table.value_blocks_received.isSet(sync.block.table_value));
                        sync.table.value_blocks_received.unset(sync.block.table_value);
                    }
                },
            }
        }
    }

    /// When we state sync, cancellation of our already-queued missing blocks happens in two stages:
    /// 1. First (in this function, called immediately after grid.cancel()) we clean up single-block
    ///    faults.
    /// 2. Later (in sync_complete()), after the state machine is opened with the new checkpoint, we
    ///    clean up any tables which did not survive into the new checkpoint.
    pub fn sync_jump_commence(queue: *GridBlocksMissing) void {
        queue.verify();
        defer if (constants.verify) queue.verify();
        // The replica may call sync_jump_commence() without ever calling sync_complete() if it
        // syncs multiple checkpoints without successfully opening the state machine.
        assert(queue.state == .repairing or queue.state == .sync_jump);

        // Release the "single" blocks since when we finish syncing we have no easy way of checking
        // whether they will still be valid.
        var faulty_blocks = queue.faulty_blocks.iterator();
        while (faulty_blocks.next()) |fault_entry| {
            assert(fault_entry.value_ptr.state == .waiting);
            if (fault_entry.value_ptr.cause == .repair) {
                faulty_blocks.index -= 1;
                faulty_blocks.len -= 1;
                queue.release_fault(faulty_blocks.index);
            } else {
                assert(queue.state == .repairing);
            }
        }
        assert(queue.enqueued_blocks_repair == 0);

        if (queue.state == .repairing) {
            queue.state = .sync_jump;

            assert(queue.syncing_faulty_blocks.count() == 0);
            std.mem.swap(FaultyBlocks, &queue.faulty_blocks, &queue.syncing_faulty_blocks);
            queue.faulty_blocks_repair_index = 0;
        }
        assert(queue.faulty_blocks.count() == 0);
        assert(queue.syncing_faulty_blocks.count() == queue.enqueued_blocks_sync);
    }

    /// Cancel repair for tables that don't belong in the new (sync target) checkpoint.
    /// (Unlike checkpoint, we can't just use the free set to determine which blocks to discard.)
    pub fn sync_tables_cancel(
        queue: *GridBlocksMissing,
        tables: []const *RepairTable,
        free_set: *const vsr.FreeSet,
    ) void {
        queue.verify();
        defer if (constants.verify) queue.verify();
        assert(queue.state == .sync_jump);

        for (tables) |table| {
            assert(queue.faulty_tables.contains(table) != queue.faulty_tables_free.contains(table));

            // The table was already cancelled/completed, it just hasn't been reclaimed yet.
            if (queue.faulty_tables_free.contains(table)) continue;

            var faulty_blocks_removed: u32 = 0;
            var faulty_blocks = queue.syncing_faulty_blocks.iterator();
            while (faulty_blocks.next()) |fault_entry| {
                const fault = fault_entry.value_ptr;
                assert(fault.state != .aborting);

                switch (fault.cause) {
                    .repair => {},
                    .sync => |sync| {
                        assert(fault.state == .waiting);
                        if (sync.table == table) {
                            faulty_blocks_removed += 1;
                            faulty_blocks.index -= 1;
                            faulty_blocks.len -= 1;
                            queue.enqueued_blocks_sync -= 1;
                            queue.syncing_faulty_blocks.swapRemoveAt(faulty_blocks.index);
                        }
                    },
                }
            }
            assert(faulty_blocks_removed ==
                (table.table_blocks_total orelse 1) - table.table_blocks_written);
            assert(queue.faulty_blocks.count() + queue.syncing_faulty_blocks.count() ==
                queue.enqueued_blocks_sync + queue.enqueued_blocks_repair);

            queue.faulty_tables.remove(table);
            queue.faulty_tables_free.push(table);
        }
        queue.sync_complete(free_set);
    }

    fn sync_complete(queue: *GridBlocksMissing, free_set: *const vsr.FreeSet) void {
        queue.verify();
        defer if (constants.verify) queue.verify();
        assert(queue.state == .sync_jump);
        assert(free_set.opened);

        queue.state = .repairing;
        std.mem.swap(FaultyBlocks, &queue.faulty_blocks, &queue.syncing_faulty_blocks);

        // Move any leftover block repairs (from faults incurred during since
        // `sync_jump_commence()`) back to `faulty_blocks`.
        while (queue.syncing_faulty_blocks.pop()) |fault_entry| {
            assert(fault_entry.value.cause == .repair);

            const fault_address = fault_entry.key;
            const fault_result = queue.faulty_blocks.getOrPutAssumeCapacity(fault_address);
            assert(!fault_result.found_existing);
            fault_result.value_ptr.* = fault_entry.value;
        }

        for (queue.faulty_blocks.keys()) |fault_address| {
            assert(!free_set.is_free(fault_address));
        }
    }

    /// Aborts queued repairs to blocks to be freed, now that the current checkpoint is durable.
    pub fn checkpoint_durable_commence(
        queue: *GridBlocksMissing,
        free_set: *const vsr.FreeSet,
    ) void {
        queue.verify();
        defer if (constants.verify) queue.verify();
        assert(queue.state == .repairing);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);
        assert(free_set.opened);

        var aborting: usize = 0;

        var faulty_blocks = queue.faulty_blocks.iterator();
        while (faulty_blocks.next()) |fault_entry| {
            const fault_address = fault_entry.key_ptr.*;
            assert(!free_set.is_free(fault_address));
            assert(fault_entry.value_ptr.state != .aborting);
            // Use `to_be_freed_at_checkpoint_durability` instead of `is_released`; the latter also
            // contains the blocks that will be released when the *next* checkpoint becomes durable.
            if (free_set.to_be_freed_at_checkpoint_durability(fault_address)) {
                switch (fault_entry.value_ptr.state) {
                    .waiting => {
                        faulty_blocks.index -= 1;
                        faulty_blocks.len -= 1;
                        queue.release_fault(faulty_blocks.index);
                    },
                    .writing => {
                        fault_entry.value_ptr.state = .aborting;
                        aborting += 1;
                    },
                    .aborting => unreachable,
                }
            }
        }

        var tables: QueueType(RepairTable) = QueueType(RepairTable).init(.{
            .name = queue.faulty_tables.any.name,
        });
        while (queue.faulty_tables.pop()) |table| {
            assert(!free_set.is_free(table.table_info.address));

            if (free_set.to_be_freed_at_checkpoint_durability(table.table_info.address)) {
                queue.faulty_tables_free.push(table);
            } else {
                tables.push(table);
            }
        }
        queue.faulty_tables = tables;

        queue.state = .{ .checkpoint_durable = .{ .aborting = aborting } };
    }

    /// Returns `true` when the `state≠waiting` faults for blocks that are staged to be
    /// released have finished. (All other writes can safely complete after the checkpoint.)
    pub fn checkpoint_durable_complete(queue: *GridBlocksMissing) bool {
        queue.verify();
        assert(queue.state == .checkpoint_durable);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_repair + queue.enqueued_blocks_sync);

        if (queue.state.checkpoint_durable.aborting == 0) {
            queue.state = .repairing;

            for (queue.faulty_blocks.values()) |*faulty_block| {
                assert(faulty_block.state != .aborting);
            }

            return true;
        } else {
            return false;
        }
    }
};
