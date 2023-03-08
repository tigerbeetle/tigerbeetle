//! The tracer records a tree of event spans.
//!
//! In order to create event spans, you need somewhere to store the `SpanStart`.
//!
//!     var slot: ?SpanStart = null;
//!     tracer.start(&slot, group, event, @src());
//!     ... do stuff ...
//!     tracer.end(&slot, group, event);
//!
//! Each slot can be used as many times as you like,
//! but you must alternate calls to start and end,
//! and you must end every event.
//!
//!     // good
//!     tracer.start(&slot, group_a, event_a, @src());
//!     tracer.end(&slot, group_a, event_a);
//!     tracer.start(&slot, group_b, event_b, @src());
//!     tracer.end(&slot, group_b, event_b);
//!
//!     // bad
//!     tracer.start(&slot, group_a, event_a, @src());
//!     tracer.start(&slot, group_b, event_b, @src());
//!     tracer.end(&slot, group_b, event_b);
//!     tracer.end(&slot, group_a, event_a);
//!
//!     // bad
//!     tracer.end(&slot, group_a, event_a);
//!     tracer.start(&slot, group_a, event_a, @src());
//!
//!     // bad
//!     tracer.start(&slot, group_a, event_a, @src());
//!     std.os.exit(0);
//!
//! Before freeing a slot, you should `assert(slot == null)`
//! to ensure that you didn't forget to end an event.
//!
//! Each `Event` has an `EventGroup`.
//! Within each group, event spans should form a tree.
//!
//!     // good
//!     tracer.start(&a, group, ...);
//!     tracer.start(&b, group, ...);
//!     tracer.end(&b, group, ...);
//!     tracer.end(&a, group, ...);
//!
//!     // bad
//!     tracer.start(&a, group, ...);
//!     tracer.start(&b, group, ...);
//!     tracer.end(&a, group, ...);
//!     tracer.end(&b, group, ...);
//!
//! The tracer itself will not object to non-tree spans, but
//! some constants.tracer_backends will either refuse to open the trace or will render it weirdly.
//!
//! If you're having trouble making your spans form a tree, feel free to just add new groups.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.tracer);

const constants = @import("./constants.zig");
const Time = @import("./time.zig").Time;
const stdx = @import("stdx.zig");

/// All strings in Event must be comptime constants to ensure that they live until after `tracer.deinit` is called.
pub const Event = union(enum) {
    tracer_flush,
    commit: struct {
        op: u64,
    },
    checkpoint,
    state_machine_prefetch,
    state_machine_commit,
    state_machine_compact,
    tree_compaction_beat,
    tree_compaction_tick: struct {
        level_b: u8,
    },
    tree_compaction_merge: struct {
        level_b: u8,
    },
    grid_read_iop: struct {
        index: usize,
    },
    grid_write_iop: struct {
        index: usize,
    },

    pub fn format(
        event: Event,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        switch (event) {
            .tracer_flush,
            .checkpoint,
            .state_machine_prefetch,
            .state_machine_commit,
            .state_machine_compact,
            .tree_compaction_beat,
            => try writer.writeAll(@tagName(event)),
            .commit => |args| try writer.print("commit({})", .{args.op}),
            .tree_compaction_tick => |args| {
                if (args.level_b == 0)
                    try writer.print(
                        "tree_compaction_tick({s}->{})",
                        .{
                            "immutable",
                            args.level_b,
                        },
                    )
                else
                    try writer.print(
                        "tree_compaction_tick({}->{})",
                        .{
                            args.level_b - 1,
                            args.level_b,
                        },
                    );
            },
            .tree_compaction_merge => |args| {
                if (args.level_b == 0)
                    try writer.print(
                        "tree_compaction_merge({s}->{})",
                        .{
                            "immutable",
                            args.level_b,
                        },
                    )
                else
                    try writer.print(
                        "tree_compaction_merge({}->{})",
                        .{
                            args.level_b - 1,
                            args.level_b,
                        },
                    );
            },
            .grid_read_iop => |args| try writer.print("grid_read_iop({})", .{args.index}),
            .grid_write_iop => |args| try writer.print("grid_write_iop({})", .{args.index}),
        }
    }
};

/// All strings in EventGroup must be comptime constants to ensure that they live until after `tracer.deinit` is called.
pub const EventGroup = union(enum) {
    main,
    tracer,
    tree: struct {
        tree_name: [:0]const u8,
    },
    tree_compaction: struct {
        compaction_name: [:0]const u8,
    },
    grid_read_iop: struct {
        index: usize,
    },
    grid_write_iop: struct {
        index: usize,
    },

    // NOTE: Returns a comptime constant because `tracy_fiber_enter` requires unique string pointers.
    fn name(event_group: EventGroup) [:0]const u8 {
        return switch (event_group) {
            .main, .tracer => @tagName(event_group),
            .tree => |args| args.tree_name,
            .tree_compaction => |args| args.compaction_name,
            .grid_read_iop => |args| {
                // TODO Use inline switch in zig 0.10
                comptime var index: usize = 0;
                inline while (index < constants.grid_iops_read_max) : (index += 1) {
                    if (args.index == index) {
                        return std.fmt.comptimePrint("grid_read_iop({})", .{index});
                    }
                }
                unreachable;
            },
            .grid_write_iop => |args| {
                // TODO Use inline switch in zig 0.10
                comptime var index: usize = 0;
                inline while (index < constants.grid_iops_write_max) : (index += 1) {
                    if (args.index == index) {
                        return std.fmt.comptimePrint("grid_write_iop({})", .{index});
                    }
                }
                unreachable;
            },
        };
    }
};

/// All strings in PlotId must be comptime constants to ensure that they live until after `tracer.deinit` is called.
pub const PlotId = union(enum) {
    grid_write_queue_count,
    grid_read_queue_count,
    grid_read_pending_queue_count,
    grid_read_recovery_queue_count,
    io_unqueued_count,
    io_completed_count,
    storage_next_tick_count,
    cache_hits: [:0]const u8,
    cache_misses: [:0]const u8,
    filter_block_hits: [:0]const u8,
    filter_block_misses: [:0]const u8,

    // NOTE: Returns a comptime constant because `tracy_emit_plot` prefers unique string pointers.
    fn name(plot_id: PlotId) [:0]const u8 {
        return switch (plot_id) {
            .grid_write_queue_count,
            .grid_read_queue_count,
            .grid_read_pending_queue_count,
            .grid_read_recovery_queue_count,
            .io_unqueued_count,
            .io_completed_count,
            .storage_next_tick_count,
            => @tagName(plot_id),
            .cache_hits,
            .cache_misses,
            .filter_block_hits,
            .filter_block_misses,
            => |name| name,
        };
    }
};

usingnamespace switch (constants.tracer_backend) {
    .none => TracerNone,
    .tracy => TracerTracy,
};

pub const TracerNone = struct {
    pub const SpanStart = void;
    pub fn init(allocator: Allocator) !void {
        _ = allocator;
    }
    pub fn deinit(allocator: Allocator) void {
        _ = allocator;
    }
    pub fn start(
        slot: *?SpanStart,
        event_group: EventGroup,
        event: Event,
        src: std.builtin.SourceLocation,
    ) void {
        _ = src;
        _ = slot;
        _ = event_group;
        _ = event;
    }
    pub fn end(slot: *?SpanStart, event_group: EventGroup, event: Event) void {
        _ = slot;
        _ = event_group;
        _ = event;
    }
    pub fn plot(plot_id: PlotId, value: f64) void {
        _ = plot_id;
        _ = value;
    }
};

const TracerTracy = struct {
    const c = @cImport({
        @cDefine("TRACY_ENABLE", "1");
        @cDefine("TRACY_FIBERS", "1");
        @cInclude("TracyC.h");
    });

    // TODO Ask constants.zig for a static bound on callstack depth.
    const callstack_depth = 64;

    pub const SpanStart = c.___tracy_c_zone_context;

    var print_buffer: [1024]u8 = undefined;

    pub fn init(allocator: Allocator) !void {
        _ = allocator;
    }

    pub fn deinit(allocator: Allocator) void {
        _ = allocator;
    }

    pub fn start(
        slot: *?SpanStart,
        event_group: EventGroup,
        event: Event,
        src: std.builtin.SourceLocation,
    ) void {
        // The event must not already have been started.
        assert(slot.* == null);
        c.___tracy_fiber_enter(event_group.name());
        const name = std.fmt.bufPrint(&print_buffer, "{}", .{event}) catch name: {
            const dots = "...";
            stdx.copy_disjoint(.exact, u8, print_buffer[print_buffer.len - dots.len ..], dots);
            break :name &print_buffer;
        };
        // TODO The alloc_srcloc here is not free and should be unnecessary,
        //      but the alloc-free version currently crashes:
        //      https://github.com/ziglang/zig/issues/13315#issuecomment-1331099909.
        slot.* = c.___tracy_emit_zone_begin_alloc_callstack(c.___tracy_alloc_srcloc_name(
            src.line,
            src.file.ptr,
            src.file.len,
            src.fn_name.ptr,
            src.fn_name.len,
            name.ptr,
            name.len,
        ), callstack_depth, 1);
    }

    pub fn end(slot: *?SpanStart, event_group: EventGroup, event: Event) void {
        _ = event;

        // The event must already have been started.
        const tracy_context = slot.*.?;
        c.___tracy_fiber_enter(event_group.name());
        c.___tracy_emit_zone_end(tracy_context);
        slot.* = null;
    }

    pub fn plot(plot_id: PlotId, value: f64) void {
        // TODO We almost always want staircase plots, but can't configure this from zig yet.
        //      See https://github.com/wolfpld/tracy/issues/537.
        c.___tracy_emit_plot(plot_id.name(), value);
    }

    pub fn log_fn(
        comptime level: std.log.Level,
        comptime scope: @TypeOf(.EnumLiteral),
        comptime format: []const u8,
        args: anytype,
    ) void {
        const level_text = comptime level.asText();
        const prefix = if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";
        const message = std.fmt.bufPrint(
            &print_buffer,
            level_text ++ prefix ++ format,
            args,
        ) catch message: {
            const dots = "...";
            stdx.copy_disjoint(.exact, u8, print_buffer[print_buffer.len - dots.len ..], dots);
            break :message &print_buffer;
        };
        c.___tracy_fiber_enter((EventGroup{ .main = {} }).name());
        c.___tracy_emit_message(message.ptr, message.len, callstack_depth);
    }

    // Copied from zig/src/tracy.zig
    pub const TracerAllocator = struct {
        parent_allocator: std.mem.Allocator,

        pub fn init(parent_allocator: std.mem.Allocator) TracerAllocator {
            return .{
                .parent_allocator = parent_allocator,
            };
        }

        pub fn allocator(self: *TracerAllocator) std.mem.Allocator {
            return std.mem.Allocator.init(self, allocFn, resizeFn, freeFn);
        }

        fn allocFn(
            self: *TracerAllocator,
            len: usize,
            ptr_align: u29,
            len_align: u29,
            ret_addr: usize,
        ) std.mem.Allocator.Error![]u8 {
            const result = self.parent_allocator.rawAlloc(len, ptr_align, len_align, ret_addr);
            if (result) |data| {
                if (data.len != 0) {
                    c.___tracy_emit_memory_alloc_callstack(data.ptr, data.len, callstack_depth, 0);
                }
            } else |_| {}
            return result;
        }

        fn resizeFn(
            self: *TracerAllocator,
            buf: []u8,
            buf_align: u29,
            new_len: usize,
            len_align: u29,
            ret_addr: usize,
        ) ?usize {
            if (self.parent_allocator.rawResize(
                buf,
                buf_align,
                new_len,
                len_align,
                ret_addr,
            )) |resized_len| {
                c.___tracy_emit_memory_free_callstack(buf.ptr, callstack_depth, 0);
                c.___tracy_emit_memory_alloc_callstack(buf.ptr, resized_len, callstack_depth, 0);

                return resized_len;
            }

            // during normal operation the compiler hits this case thousands of times due to this
            // emitting messages for it is both slow and causes clutter
            return null;
        }

        fn freeFn(self: *TracerAllocator, buf: []u8, buf_align: u29, ret_addr: usize) void {
            self.parent_allocator.rawFree(buf, buf_align, ret_addr);
            // this condition is to handle free being called on an empty slice that was never even allocated
            // example case: `std.process.getSelfExeSharedLibPaths` can return `&[_][:0]u8{}`
            if (buf.len != 0) {
                c.___tracy_emit_memory_free_callstack(buf.ptr, callstack_depth, 0);
            }
        }
    };
};
