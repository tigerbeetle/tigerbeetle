const std = @import("std");
const assert = std.debug.assert;

pub const RepairBudgetJournal = struct {
    capacity: u32,
    available: u32,
    refill_max: u32,

    // Tracks the headers for prepares requested by inflight RequestPrepare messages.
    requested_prepares: std.AutoArrayHashMapUnmanaged(PrepareIdentifier, void),

    const PrepareIdentifier = struct { view: u32, op: u64 };

    pub fn init(gpa: std.mem.Allocator, options: struct {
        capacity: u32,
        refill_max: u32,
    }) !RepairBudgetJournal {
        assert(options.refill_max <= options.capacity);

        var requested_prepares: std.AutoArrayHashMapUnmanaged(PrepareIdentifier, void) = .{};
        try requested_prepares.ensureTotalCapacity(gpa, options.capacity);
        errdefer requested_prepares.deinit();

        return RepairBudgetJournal{
            .capacity = options.capacity,
            .available = options.capacity,
            .refill_max = options.refill_max,
            .requested_prepares = requested_prepares,
        };
    }

    pub fn deinit(budget: *RepairBudgetJournal, gpa: std.mem.Allocator) void {
        budget.requested_prepares.deinit(gpa);
    }

    pub fn decrement(budget: *RepairBudgetJournal, prepare_identifier: PrepareIdentifier) bool {
        assert(budget.capacity > 0);
        budget.assert_invariants();
        defer budget.assert_invariants();

        if (budget.available == 0) return false;

        const gop = budget.requested_prepares.getOrPutAssumeCapacity(prepare_identifier);
        if (gop.found_existing) return false;

        budget.available -= 1;

        return true;
    }

    pub fn increment(budget: *RepairBudgetJournal, prepare_identifier: PrepareIdentifier) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        // Increment budget iff we had requested this prepare.
        if (budget.requested_prepares.swapRemove(prepare_identifier)) {
            budget.available = @min((budget.available + 1), budget.capacity);
        }
    }

    pub fn refill(budget: *RepairBudgetJournal) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        budget.requested_prepares.clearRetainingCapacity();
        budget.available = @min((budget.available + budget.refill_max), budget.capacity);
    }

    fn assert_invariants(budget: *const RepairBudgetJournal) void {
        assert(budget.available <= budget.capacity);
        assert(budget.capacity - budget.available >=
            @as(u32, @intCast(budget.requested_prepares.count())));
    }
};

pub const RepairBudgetGrid = struct {
    capacity: u32,
    available: u32,
    refill_max: u32,
    requested: std.AutoArrayHashMapUnmanaged(BlockIdentifier, void),

    const BlockIdentifier = struct { address: u64, checksum: u128 };

    pub fn init(gpa: std.mem.Allocator, options: struct {
        capacity: u32,
        refill_max: u32,
    }) !RepairBudgetGrid {
        assert(options.refill_max <= options.capacity);

        var requested: std.AutoArrayHashMapUnmanaged(BlockIdentifier, void) = .{};
        try requested.ensureTotalCapacity(gpa, options.capacity);
        errdefer requested.deinit();

        return RepairBudgetGrid{
            .capacity = options.capacity,
            .available = options.capacity,
            .refill_max = options.refill_max,
            .requested = requested,
        };
    }

    pub fn deinit(budget: *RepairBudgetGrid, gpa: std.mem.Allocator) void {
        budget.requested.deinit(gpa);
    }

    fn assert_invariants(budget: *RepairBudgetGrid) void {
        assert(budget.available <= budget.capacity);
        assert(budget.available + budget.requested.count() <= budget.capacity);
    }

    pub fn decrement(budget: *RepairBudgetGrid, block_identifier: BlockIdentifier) bool {
        budget.assert_invariants();
        defer budget.assert_invariants();
        assert(budget.available > 0);
        assert(block_identifier.address > 0);

        const gop = budget.requested.getOrPutAssumeCapacity(block_identifier);
        if (gop.found_existing) {
            return false;
        } else {
            budget.available -= 1;
            return true;
        }
    }

    pub fn increment(budget: *RepairBudgetGrid, block_identifier: BlockIdentifier) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        if (budget.requested.swapRemove(block_identifier)) {
            budget.available = @min((budget.available + 1), budget.capacity);
        }
    }

    pub fn refill(budget: *RepairBudgetGrid) void {
        budget.assert_invariants();
        defer budget.assert_invariants();

        budget.available = @min((budget.available + budget.refill_max), budget.capacity);
        budget.requested.clearRetainingCapacity();
    }
};
