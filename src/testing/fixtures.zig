//! Convenient constors for TigerBeetle components, for fuzzing and testing.
//!
//! Consider Storage. In the actual database, there is only single call to Storage.init.
//! However, Storage is needed for most of our tests and fuzzers. If the init call is repeated
//! in every fuzzer, changing Storage creation flow becomes hard. To solve this, all fuzzers create
//! Storage through this file, such that we have one production and one test call to Storage.init.
//!
//! Design:
//!
//! - All fuctions take struct options as a last argument, even if it starts out as empty.
//!   All call-sites pass at least .{}, which makes adding new options cheap.
//! - Options should be spelled-out in this file, rather re-using StorageOptions and the like, to
//!   help the reader see the full API at a glance.
//! - Most options should have defaults. This is intentional deviation from TigerStyle, as, for
//!   tests, we gain a useful property: all options that are set are meaningful for a particular
//!   test.
//! - It could be convenient to export types themselves, in addition to constructors, but we avoid
//!   introducing two ways to import something.
const std = @import("std");
const constants = @import("../constants.zig");

const Time = @import("../time.zig").Time;
const OffsetType = @import("./time.zig").OffsetType;
const Tracer = @import("../trace.zig").Tracer;

const TimeSim = @import("./time.zig").TimeSim;

pub fn time(options: struct {
    resolution: u64 = constants.tick_ms * std.time.ns_per_ms,
    offset_type: OffsetType = .linear,
    offset_coefficient_A: i64 = 0,
    offset_coefficient_B: i64 = 0,
    offset_coefficient_C: u32 = 0,
}) TimeSim {
    const result: TimeSim = .{
        .resolution = options.resolution,
        .offset_type = options.offset_type,
        .offset_coefficient_A = options.offset_coefficient_A,
        .offset_coefficient_B = options.offset_coefficient_B,
        .offset_coefficient_C = options.offset_coefficient_C,
    };
    return result;
}

pub fn tracer(gpa: std.mem.Allocator, t: Time, options: struct {
    writer: ?std.io.AnyWriter = null,
    process_id: Tracer.ProcessID = .{ .replica = .{ .cluster = 0, .replica = 0 } },
}) !Tracer {
    return Tracer.init(gpa, t, options.process_id, .{ .writer = options.writer });
}
