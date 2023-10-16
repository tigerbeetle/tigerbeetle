//! The tracer records a tree of event spans.
//!
//! In order to create event spans, you need somewhere to store the `SpanStart`.
//!
//!     var slot: ?SpanStart = null;
//!     tracer.start(&slot, event, @src());
//!     ... do stuff ...
//!     tracer.end(&slot, event);
//!
//! Each slot can be used as many times as you like,
//! but you must alternate calls to start and end,
//! and you must end every event.
//!
//!     // good
//!     tracer.start(&slot, event_a, @src());
//!     tracer.end(&slot, event_a);
//!     tracer.start(&slot, event_b, @src());
//!     tracer.end(&slot, event_b);
//!
//!     // bad
//!     tracer.start(&slot, event_a, @src());
//!     tracer.start(&slot, event_b, @src());
//!     tracer.end(&slot, event_b);
//!     tracer.end(&slot, event_a);
//!
//!     // bad
//!     tracer.end(&slot, event_a);
//!     tracer.start(&slot, event_a, @src());
//!
//!     // bad
//!     tracer.start(&slot, event_a, @src());
//!     std.os.exit(0);
//!
//! Before freeing a slot, you should `assert(slot == null)`
//! to ensure that you didn't forget to end an event.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const panic = std.debug.panic;
const AutoHashMap = std.AutoHashMap;
const log = std.log.scoped(.tracer);

const constants = @import("./constants.zig");
const Time = @import("./time.zig").Time;
const stdx = @import("stdx.zig");

pub const Event = union(enum) {
    commit: struct {
        op: u64,
    },
    checkpoint,
    state_machine_prefetch,
    state_machine_commit,
    state_machine_compact,
    tree_compaction_beat: struct {
        tree_name: []const u8,
    },
    tree_compaction: struct {
        tree_name: []const u8,
        level_b: u8,
    },
    tree_compaction_iter: struct {
        tree_name: []const u8,
        level_b: u8,
    },
    tree_compaction_merge: struct {
        tree_name: []const u8,
        level_b: u8,
    },
    grid_read_iop: struct {
        index: usize,
    },
    grid_write_iop: struct {
        index: usize,
    },
    io_flush,
    io_callback,

    pub fn format(
        event: Event,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        switch (event) {
            .commit => |args| try writer.print("commit({})", .{args.op}),
            .checkpoint,
            .state_machine_prefetch,
            .state_machine_commit,
            .state_machine_compact,
            .io_flush,
            .io_callback,
            => try writer.writeAll(@tagName(event)),
            .tree_compaction_beat => |args| try writer.print(
                "tree_compaction_beat({s})",
                .{
                    args.tree_name,
                },
            ),
            .tree_compaction => |args| {
                const level_a = LevelA{ .level_b = args.level_b };
                try writer.print(
                    "tree_compaction({s}, {}->{})",
                    .{
                        args.tree_name,
                        level_a,
                        args.level_b,
                    },
                );
            },
            .tree_compaction_iter => |args| {
                const level_a = LevelA{ .level_b = args.level_b };
                try writer.print(
                    "tree_compaction_iter({s}, {}->{})",
                    .{
                        args.tree_name,
                        level_a,
                        args.level_b,
                    },
                );
            },
            .tree_compaction_merge => |args| {
                const level_a = LevelA{ .level_b = args.level_b };
                try writer.print(
                    "tree_compaction_merge({s}, {s}->{})",
                    .{
                        args.tree_name,
                        level_a,
                        args.level_b,
                    },
                );
            },
            .grid_read_iop => |args| try writer.print("grid_read_iop({})", .{args.index}),
            .grid_write_iop => |args| try writer.print("grid_write_iop({})", .{args.index}),
        }
    }

    fn fiber(event: Event) Fiber {
        return switch (event) {
            .commit,
            .checkpoint,
            .state_machine_prefetch,
            .state_machine_commit,
            .state_machine_compact,
            => .main,
            .tree_compaction_beat => |args| .{ .tree = .{
                .tree_name = args.tree_name,
            } },
            .tree_compaction => |args| .{ .tree_compaction = .{
                .tree_name = args.tree_name,
                .level_b = args.level_b,
            } },
            .tree_compaction_iter => |args| .{ .tree_compaction = .{
                .tree_name = args.tree_name,
                .level_b = args.level_b,
            } },
            .tree_compaction_merge => |args| .{ .tree_compaction = .{
                .tree_name = args.tree_name,
                .level_b = args.level_b,
            } },
            .grid_read_iop => |args| .{ .grid_read_iop = .{
                .index = args.index,
            } },
            .grid_write_iop => |args| .{ .grid_write_iop = .{
                .index = args.index,
            } },
            .io_flush, .io_callback => .io,
        };
    }
};

/// Tracy requires all spans within a single thread/fiber to be nested.
/// Since we don't have threads or fibers to structure our spans,
/// we hardcode a structure that nests events where possible.
const Fiber = union(enum) {
    main,
    tree: struct {
        tree_name: []const u8,
    },
    tree_compaction: struct {
        tree_name: []const u8,
        level_b: u8,
    },
    grid_read_iop: struct {
        index: usize,
    },
    grid_write_iop: struct {
        index: usize,
    },
    io,

    pub fn format(
        fiber: Fiber,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        return switch (fiber) {
            .main, .io => try writer.writeAll(@tagName(fiber)),
            .tree => |args| try writer.print(
                "tree({s})",
                .{
                    args.tree_name,
                },
            ),
            .tree_compaction => |args| {
                const level_a = LevelA{ .level_b = args.level_b };
                try writer.print(
                    "tree_compaction({s}, {}->{})",
                    .{
                        args.tree_name,
                        level_a,
                        args.level_b,
                    },
                );
            },
            .grid_read_iop => |args| try writer.print("grid_read_iop({})", .{args.index}),
            .grid_write_iop => |args| try writer.print("grid_write_iop({})", .{args.index}),
        };
    }
};

const LevelA = struct {
    level_b: u8,

    pub fn format(
        level_a: LevelA,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        if (level_a.level_b == 0)
            try writer.writeAll("immutable")
        else
            try writer.print("{}", .{level_a.level_b - 1});
    }
};

pub const PlotId = union(enum) {
    queue_count: struct {
        queue_name: []const u8,
    },
    cache_hits: struct {
        cache_name: []const u8,
    },
    cache_misses: struct {
        cache_name: []const u8,
    },

    pub fn format(
        plot_id: PlotId,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        return switch (plot_id) {
            .queue_count => |args| try writer.print("queue_count({s})", .{args.queue_name}),
            .cache_hits => |args| try writer.print("cache_hits({s})", .{args.cache_name}),
            .cache_misses => |args| try writer.print("cache_misses({s})", .{args.cache_name}),
        };
    }
};

pub usingnamespace switch (constants.tracer_backend) {
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
        event: Event,
        src: std.builtin.SourceLocation,
    ) void {
        _ = src;
        _ = slot;
        _ = event;
    }
    pub fn end(slot: *?SpanStart, event: Event) void {
        _ = slot;
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

    pub fn Interns(comptime Key: type) type {
        return std.HashMap(
            Key,
            [:0]const u8,
            struct {
                pub fn hash(self: @This(), key: Key) u64 {
                    _ = self;
                    var hasher = std.hash.Wyhash.init(0);
                    std.hash.autoHashStrat(
                        &hasher,
                        key,
                        // We can get away with shallow as long as all string fields are comptime constants.
                        .Shallow,
                    );
                    return hasher.final();
                }
                pub fn eql(self: @This(), a: Key, b: Key) bool {
                    _ = self;
                    return std.meta.eql(a, b);
                }
            },
            std.hash_map.default_max_load_percentage,
        );
    }

    var message_buffer: [1024]u8 = undefined;

    var allocator: Allocator = undefined;
    var fiber_interns: Interns(Fiber) = undefined;
    var event_interns: Interns(Event) = undefined;
    var plot_id_interns: Interns(PlotId) = undefined;

    pub fn init(allocator_: Allocator) !void {
        allocator = allocator_;
        fiber_interns = Interns(Fiber).init(allocator_);
        event_interns = Interns(Event).init(allocator_);
        plot_id_interns = Interns(PlotId).init(allocator_);
    }

    pub fn deinit(allocator_: Allocator) void {
        _ = allocator_;
        {
            var iter = plot_id_interns.iterator();
            while (iter.next()) |entry| {
                allocator.free(entry.value_ptr.*);
            }
            plot_id_interns.deinit();
        }
        {
            var iter = event_interns.iterator();
            while (iter.next()) |entry| {
                allocator.free(entry.value_ptr.*);
            }
            event_interns.deinit();
        }
        {
            var iter = fiber_interns.iterator();
            while (iter.next()) |entry| {
                allocator.free(entry.value_ptr.*);
            }
            fiber_interns.deinit();
        }
    }

    fn intern_name(item: anytype) [:0]const u8 {
        const interns = switch (@TypeOf(item)) {
            Fiber => &fiber_interns,
            Event => &event_interns,
            PlotId => &plot_id_interns,
            else => @compileError("Don't know how to intern " ++ @typeName(@TypeOf(item))),
        };
        const entry = interns.getOrPut(item) catch
            panic("OOM in tracer", .{});
        if (!entry.found_existing) {
            entry.value_ptr.* = std.fmt.allocPrintZ(allocator, "{}", .{item}) catch
                panic("OOM in tracer", .{});
        }
        return entry.value_ptr.*;
    }

    pub fn start(
        slot: *?SpanStart,
        event: Event,
        src: std.builtin.SourceLocation,
    ) void {
        // The event must not already have been started.
        assert(slot.* == null);
        c.___tracy_fiber_enter(@as([*c]const u8, @ptrCast(intern_name(event.fiber()))));
        const name = intern_name(event);
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

    pub fn end(slot: *?SpanStart, event: Event) void {
        // The event must already have been started.
        const tracy_context = slot.*.?;
        c.___tracy_fiber_enter(@as([*c]const u8, @ptrCast(intern_name(event.fiber()))));
        c.___tracy_emit_zone_end(tracy_context);
        slot.* = null;
    }

    pub fn plot(plot_id: PlotId, value: f64) void {
        // TODO We almost always want staircase plots, but can't configure this from zig yet.
        //      See https://github.com/wolfpld/tracy/issues/537.
        c.___tracy_emit_plot(@as([*c]const u8, @ptrCast(intern_name(plot_id))), value);
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
            &message_buffer,
            level_text ++ prefix ++ format,
            args,
        ) catch message: {
            const dots = "...";
            stdx.copy_disjoint(.exact, u8, message_buffer[message_buffer.len - dots.len ..], dots);
            break :message &message_buffer;
        };
        c.___tracy_fiber_enter(@as([*c]const u8, @ptrCast(intern_name(Fiber{ .main = {} }))));
        c.___tracy_emit_message(message.ptr, message.len, callstack_depth);
    }

    // Copied from zig/src/tracy.zig
    // This function only accepts comptime-known strings, see `messageColorCopy` for runtime strings
    pub inline fn messageColor(comptime msg: [:0]const u8, color: u32) void {
        c.___tracy_emit_messageLC(msg.ptr, color, callstack_depth);
    }
    pub fn TracyAllocator(comptime name: ?[:0]const u8) type {
        return struct {
            parent_allocator: std.mem.Allocator,

            const Self = @This();

            pub fn init(parent_allocator: std.mem.Allocator) Self {
                return .{
                    .parent_allocator = parent_allocator,
                };
            }

            pub fn allocator(self: *Self) std.mem.Allocator {
                return .{
                    .ptr = self,
                    .vtable = &.{
                        .alloc = allocFn,
                        .resize = resizeFn,
                        .free = freeFn,
                    },
                };
            }

            fn allocFn(ptr: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
                const self: *Self = @ptrCast(@alignCast(ptr));
                const result = self.parent_allocator.rawAlloc(len, ptr_align, ret_addr);
                if (result) |data| {
                    if (len != 0) {
                        if (name) |n| {
                            allocNamed(data, len, n);
                        } else {
                            alloc(data, len);
                        }
                    }
                } else {
                    messageColor("allocation failed", 0xFF0000);
                }
                return result;
            }

            fn resizeFn(ptr: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_addr: usize) bool {
                const self: *Self = @ptrCast(@alignCast(ptr));
                if (self.parent_allocator.rawResize(buf, buf_align, new_len, ret_addr)) {
                    if (name) |n| {
                        freeNamed(buf.ptr, n);
                        allocNamed(buf.ptr, new_len, n);
                    } else {
                        free(buf.ptr);
                        alloc(buf.ptr, new_len);
                    }

                    return true;
                }

                // during normal operation the compiler hits this case thousands of times due to this
                // emitting messages for it is both slow and causes clutter
                return false;
            }

            fn freeFn(ptr: *anyopaque, buf: []u8, buf_align: u8, ret_addr: usize) void {
                const self: *Self = @ptrCast(@alignCast(ptr));
                self.parent_allocator.rawFree(buf, buf_align, ret_addr);
                // this condition is to handle free being called on an empty slice that was never even allocated
                // example case: `std.process.getSelfExeSharedLibPaths` can return `&[_][:0]u8{}`
                if (buf.len != 0) {
                    if (name) |n| {
                        freeNamed(buf.ptr, n);
                    } else {
                        free(buf.ptr);
                    }
                }
            }
        };
    }
    inline fn alloc(ptr: [*]u8, len: usize) void {
        c.___tracy_emit_memory_alloc_callstack(ptr, len, callstack_depth, 0);
    }

    inline fn allocNamed(ptr: [*]u8, len: usize, comptime name: [:0]const u8) void {
        c.___tracy_emit_memory_alloc_callstack_named(ptr, len, callstack_depth, 0, name.ptr);
    }

    inline fn free(ptr: [*]u8) void {
        c.___tracy_emit_memory_free_callstack(ptr, callstack_depth, 0);
    }

    inline fn freeNamed(ptr: [*]u8, comptime name: [:0]const u8) void {
        c.___tracy_emit_memory_free_callstack_named(ptr, callstack_depth, 0, name.ptr);
    }
};
