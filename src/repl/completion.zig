const std = @import("std");
const stdx = @import("stdx");
const assert = std.debug.assert;

const RingBuffer = stdx.RingBufferType;
const BoundedArray = stdx.BoundedArrayType;

const keywords = [_][]const u8{
    "create_accounts",
    "create_transfers",
    "lookup_accounts",
    "lookup_transfers",
    "get_account_transfers",
    "get_account_balances",
    "query_accounts",
    "query_transfers",
    "help",
    "id",
    "code",
    "ledger",
    "flags",
    "account_id",
    "debit_account_id",
    "credit_account_id",
    "amount",
};
const completion_entries = keywords.len;
const completion_entry_bytes = 512;

pub const Completion = struct {
    const CompletionList = RingBuffer([completion_entry_bytes]u8, .{ .array = completion_entries });

    matches: CompletionList,
    prefix: BoundedArray(u8, completion_entry_bytes),
    suffix: BoundedArray(u8, completion_entry_bytes),
    query: BoundedArray(u8, completion_entry_bytes),
    match_index: usize = 0,

    pub fn init(self: *Completion) !void {
        self.* = Completion{
            .matches = CompletionList.init(),
            .prefix = BoundedArray(u8, completion_entry_bytes){},
            .suffix = BoundedArray(u8, completion_entry_bytes){},
            .query = BoundedArray(u8, completion_entry_bytes){},
        };
    }

    // Split the buffer into 3 parts -
    // prefix: Everything before the word being completed.
    // query : Partial word that needs completion, from last whitespace to buffer index.
    // suffix: Everything after the cursor position.

    /// Split the input buffer into prefix, query and suffix. Then tries to find the completions
    /// matching the current query.
    pub fn split_and_complete(self: *Completion, buffer: []const u8, buffer_index: usize) !void {
        var query_start_index = buffer_index;

        while (query_start_index > 0 and !std.ascii.isWhitespace(buffer[query_start_index - 1])) {
            query_start_index -= 1;
        }

        self.prefix.clear();
        self.prefix.push_slice(buffer[0..query_start_index]);

        self.suffix.clear();
        self.suffix.push_slice(buffer[buffer_index..]);

        if (self.matches.count == 0) {
            self.query.clear();
            self.query.push_slice(buffer[query_start_index..buffer_index]);

            try self.get_completions(self.query.const_slice());
        }
    }

    /// Returns the next match in the completion list. If the completion list is empty,
    /// returns the query.
    pub fn get_next_completion(self: *Completion) ![]const u8 {
        if (self.matches.count > 0) {
            const match_ptr = self.matches.get_ptr(self.match_index).?;
            self.match_index = (self.match_index + 1) % self.matches.count;
            return std.mem.sliceTo(match_ptr, '\x00');
        } else {
            return self.query.const_slice();
        }
    }

    /// Reset the completion list.
    pub fn clear(self: *Completion) void {
        self.matches.clear();
        self.prefix.clear();
        self.suffix.clear();
        self.query.clear();
        self.match_index = 0;
    }

    /// Returns number of items in the completion list.
    pub fn count(self: *Completion) usize {
        return self.matches.count;
    }

    fn get_completions(self: *Completion, target: []const u8) !void {
        if (std.mem.eql(u8, target, "")) {
            return;
        }

        for (keywords) |kw| {
            if (std.mem.startsWith(u8, kw, target)) {
                assert(kw.len < completion_entry_bytes - 1);

                if (self.matches.full()) {
                    self.matches.advance_head();
                }

                const completion_tail = self.matches.next_tail_ptr().?;
                stdx.copy_left(.exact, u8, completion_tail[0..kw.len], kw);
                completion_tail[kw.len] = '\x00';
                self.matches.advance_tail();
            }
        }
    }
};

test "completion.zig: Split buffer and complete" {
    const tests = [_]struct {
        buffer: []const u8,
        idx: usize,
        prefix: BoundedArray(u8, completion_entry_bytes),
        suffix: BoundedArray(u8, completion_entry_bytes),
        query: BoundedArray(u8, completion_entry_bytes),
        matches: BoundedArray([]const u8, 5),
    }{
        .{
            .buffer = "",
            .idx = 0,
            .prefix = try BoundedArray(u8, completion_entry_bytes).from_slice(""),
            .suffix = try BoundedArray(u8, completion_entry_bytes).from_slice(""),
            .query = try BoundedArray(u8, completion_entry_bytes).from_slice(""),
            .matches = try BoundedArray([]const u8, 5).from_slice(&.{}),
        },
        .{
            .buffer = "creat",
            .idx = 5,
            .prefix = try BoundedArray(u8, completion_entry_bytes).from_slice(""),
            .suffix = try BoundedArray(u8, completion_entry_bytes).from_slice(""),
            .query = try BoundedArray(u8, completion_entry_bytes).from_slice("creat"),
            .matches = try BoundedArray([]const u8, 5).from_slice(
                &.{ "create_accounts", "create_transfers" },
            ),
        },
        .{
            .buffer = "create_accounts id=1 co",
            .idx = 23,
            .prefix = try BoundedArray(u8, completion_entry_bytes).from_slice(
                "create_accounts id=1 ",
            ),
            .suffix = try BoundedArray(u8, completion_entry_bytes).from_slice(""),
            .query = try BoundedArray(u8, completion_entry_bytes).from_slice("co"),
            .matches = try BoundedArray([]const u8, 5).from_slice(&.{"code"}),
        },
        .{
            .buffer = "create_accounts id=1 co ledger=700",
            .idx = 23,
            .prefix = try BoundedArray(u8, completion_entry_bytes).from_slice(
                "create_accounts id=1 ",
            ),
            .suffix = try BoundedArray(u8, completion_entry_bytes).from_slice(" ledger=700"),
            .query = try BoundedArray(u8, completion_entry_bytes).from_slice("co"),
            .matches = try BoundedArray([]const u8, 5).from_slice(&.{"code"}),
        },
    };

    for (tests) |t| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();

        var completion: Completion = undefined;
        try completion.init();
        var c = &completion;

        try c.split_and_complete(t.buffer, t.idx);

        try std.testing.expectEqualSlices(u8, c.prefix.const_slice(), t.prefix.const_slice());
        try std.testing.expectEqualSlices(u8, c.suffix.const_slice(), t.suffix.const_slice());
        try std.testing.expectEqualSlices(u8, c.query.const_slice(), c.query.const_slice());
        try std.testing.expectEqual(c.count(), t.matches.count());

        var i: usize = 0;
        while (i < t.matches.count()) {
            const cur_match = try c.get_next_completion();
            try std.testing.expectEqualSlices(u8, cur_match, t.matches.get(i));
            i += 1;
        }

        c.clear();
        try std.testing.expectEqual(@as(usize, 0), c.count());
        try std.testing.expectEqual(@as(usize, 0), c.prefix.count());
        try std.testing.expectEqual(@as(usize, 0), c.suffix.count());
        try std.testing.expectEqual(@as(usize, 0), c.query.count());
    }
}
