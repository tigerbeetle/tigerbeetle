const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.concurrent_ranges);

pub const ConcurrentRanges = struct {
    name: []const u8,

    /// A queue of ranges acquired and in progress:
    acquired: ?*Range = null,

    pub fn acquire(self: *ConcurrentRanges, range: *Range) void {
        assert(range.status == .initialized);
        range.status = .acquiring;

        assert(range.prev == null);
        assert(range.next == null);
        assert(range.head == null);
        assert(range.tail == null);

        range.frame = @frame();

        while (true) {
            if (self.has_overlapping_range(range)) |overlapping_range| {
                log.debug("{s}: range {} overlaps with concurrent range {}, enqueueing", .{
                    self.name,
                    range,
                    overlapping_range,
                });
                overlapping_range.enqueue(range);
                suspend;
            } else {
                break;
            }
        }

        // TODO This can be removed for performance down the line:
        assert(self.has_overlapping_range(range) == null);

        // Add the range to the linked list:
        if (self.acquired) |next| {
            range.next = next;
            next.prev = range;
        }
        self.acquired = range;

        range.status = .acquired;

        log.debug("{s}: acquired: {}", .{ self.name, range });
    }

    pub fn release(self: *ConcurrentRanges, range: *Range) void {
        assert(range.status == .acquired);
        range.status = .releasing;

        log.debug("{s}: released: {}", .{ self.name, range });

        // Remove the range from the linked list:
        // Connect the previous range to the next range:
        if (range.prev) |prev| {
            prev.next = range.next;
        } else {
            self.acquired = range.next;
        }
        // ... and connect the next range to the previous range:
        if (range.next) |next| {
            next.prev = range.prev;
        }
        range.prev = null;
        range.next = null;

        range.resume_queued_ranges();
    }

    pub fn has_overlapping_range(self: *ConcurrentRanges, range: *const Range) ?*Range {
        var head = self.acquired;
        while (head) |concurrent_range| {
            head = concurrent_range.next;

            if (range.overlaps(concurrent_range)) return concurrent_range;
        }
        return null;
    }
};

pub const Range = struct {
    offset: u64,
    len: u64,
    frame: anyframe = undefined,
    status: RangeStatus = .initialized,

    /// Links to concurrently executing sibling ranges:
    prev: ?*Range = null,
    next: ?*Range = null,

    /// A queue of child ranges waiting on this range to complete:
    head: ?*Range = null,
    tail: ?*Range = null,

    const RangeStatus = enum {
        initialized,
        acquiring,
        acquired,
        releasing,
        released,
    };

    fn enqueue(self: *Range, range: *Range) void {
        assert(self.status == .acquired);
        assert(range.status == .acquiring);

        if (self.head == null) {
            assert(self.tail == null);
            self.head = range;
            self.tail = range;
        } else {
            self.tail.?.next = range;
            self.tail = range;
        }
    }

    fn overlaps(self: *const Range, range: *const Range) bool {
        if (self.offset < range.offset) {
            return self.offset + self.len > range.offset;
        } else {
            return range.offset + range.len > self.offset;
        }
    }

    fn resume_queued_ranges(self: *Range) void {
        assert(self.status == .releasing);
        self.status = .released;

        // This range should have been removed from the list of concurrent ranges:
        assert(self.prev == null);
        assert(self.next == null);

        var head = self.head;
        self.head = null;
        self.tail = null;
        while (head) |queued| {
            assert(queued.status == .acquiring);
            head = queued.next;
            resume queued.frame;
        }

        // This range should be finished executing and should no longer overlap or have any queue:
        assert(self.head == null);
        assert(self.tail == null);
    }

    pub fn format(
        value: Range,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print("offset={} len={}", .{
            value.offset,
            value.len,
        });
    }
};
