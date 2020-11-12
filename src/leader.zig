const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.leader);
const mem = std.mem;

usingnamespace @import("tigerbeetle.zig");

pub const Leader = struct {
    timestamp: u64,

    pub fn init() !Leader {
        return Leader {
            // TODO After recovery, take the max of wall clock and last replicated state timestamp:
            .timestamp = @intCast(u64, std.time.nanoTimestamp()),
        };
    }

    pub fn deinit(self: *Leader) void {}

    /// Assigns strictly increasing timestamps, even if the wall clock runs backwards.
    /// Returns true if all reserved timestamps were zero before being assigned, else false.
    pub fn assign_timestamps(self: *Leader, command: Command, batch: []u8) bool {
        return switch (command) {
            .create_accounts  => self.assign_timestamps_for_type(Account, batch),
            .create_transfers => self.assign_timestamps_for_type(Transfer, batch),
            .commit_transfers => self.assign_timestamps_for_type(Commit, batch),
            else => unreachable
        };
    }

    pub fn assign_timestamps_for_type(self: *Leader, comptime T: type, batch: []u8) bool {
        // Guard against the wall clock going backwards by taking the max with timestamps issued:
        self.timestamp = std.math.max(self.timestamp, @intCast(u64, std.time.nanoTimestamp()));
        var sum_reserved_timestamps: usize = 0;
        for (mem.bytesAsSlice(T, batch)) |*object| {
            sum_reserved_timestamps += object.timestamp;
            self.timestamp += 1;
            object.timestamp = self.timestamp;
            log.debug("assigned timestamp {}", .{ object.timestamp });
        }
        // Use a single branch condition to detect non-zero reserved timestamps.
        // Summing then branching once is much faster than branching every iteration of the loop.
        return sum_reserved_timestamps == 0;
    }
};
