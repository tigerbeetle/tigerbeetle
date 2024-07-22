const std = @import("std");
const os = std.os;
const posix = std.posix;
const mem = std.mem;
const assert = std.debug.assert;
const log = std.log.scoped(.io);

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const FIFO = @import("../fifo.zig").FIFO;
const buffer_limit = @import("../io.zig").buffer_limit;
const DirectIO = @import("../io.zig").DirectIO;

/// A very simple mock IO implementation that only implements what is needed to test Storage.
pub const IO = struct {
    pub const fd_t = u32;

    pub const File = struct {
        buffer: []u8,
        /// Each bit of the fault map represents a sector that will fault consistently.
        fault_map: ?[]const u8,
    };

    /// Options for fault injection during fuzz testing.
    pub const Options = struct {
        /// Seed for the storage PRNG.
        seed: u64 = 0,

        /// Chance out of 100 that a read larger than a logical sector
        /// will return an error.InputOutput.
        larger_than_logical_sector_read_fault_probability: u8 = 0,
    };

    files: []const File,
    options: Options,
    prng: std.rand.DefaultPrng,

    completed: FIFO(Completion) = .{ .name = "io_completed" },

    pub fn init(files: []const File, options: Options) IO {
        return .{
            .options = options,
            .prng = std.rand.DefaultPrng.init(options.seed),
            .files = files,
        };
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(io: *IO) !void {
        while (io.completed.pop()) |completion| {
            completion.callback(io, completion);
        }
    }

    /// This struct holds the data needed for a single IO operation.
    pub const Completion = struct {
        next: ?*Completion,
        context: ?*anyopaque,
        callback: *const fn (*IO, *Completion) void,
        operation: Operation,
    };

    const Operation = union(enum) {
        read: struct {
            fd: fd_t,
            buf: [*]u8,
            len: u32,
            offset: u64,
        },
        write: struct {
            fd: fd_t,
            buf: [*]const u8,
            len: u32,
            offset: u64,
        },
    };

    /// Return true with probability x/100.
    fn x_in_100(io: *IO, x: u8) bool {
        assert(x <= 100);
        return x > io.prng.random().uintLessThan(u8, 100);
    }

    fn submit(
        self: *IO,
        context: anytype,
        comptime callback: anytype,
        completion: *Completion,
        comptime operation_tag: std.meta.Tag(Operation),
        operation_data: anytype,
        comptime OperationImpl: type,
    ) void {
        const on_complete_fn = struct {
            fn on_complete(io: *IO, _completion: *Completion) void {
                // Perform the actual operation.
                const op_data = &@field(_completion.operation, @tagName(operation_tag));
                const result = OperationImpl.do_operation(io, op_data);

                // Complete the Completion.
                return callback(
                    @ptrCast(@alignCast(_completion.context)),
                    _completion,
                    result,
                );
            }
        }.on_complete;

        completion.* = .{
            .next = null,
            .context = context,
            .callback = on_complete_fn,
            .operation = @unionInit(Operation, @tagName(operation_tag), operation_data),
        };

        self.completed.push(completion);
    }

    pub const ReadError = error{
        WouldBlock,
        NotOpenForReading,
        ConnectionResetByPeer,
        Alignment,
        InputOutput,
        IsDir,
        SystemResources,
        Unseekable,
        ConnectionTimedOut,
    } || posix.UnexpectedError;

    pub fn read(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ReadError!usize,
        ) void,
        completion: *Completion,
        fd: fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        assert(fd < self.files.len);

        self.submit(
            context,
            callback,
            completion,
            .read,
            .{
                .fd = fd,
                .buf = buffer.ptr,
                .len = @as(u32, @intCast(buffer_limit(buffer.len))),
                .offset = offset,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) ReadError!usize {
                    const sector_marked_in_fault_map = if (io.files[op.fd].fault_map) |fault_map|
                        std.mem.readPackedIntNative(
                            u1,
                            fault_map,
                            @divExact(op.offset, constants.sector_size),
                        ) != 0
                    else
                        false;

                    const sector_has_larger_than_logical_sector_read_fault =
                        (op.len > constants.sector_size and
                        io.x_in_100(io.options.larger_than_logical_sector_read_fault_probability));

                    if (sector_marked_in_fault_map or
                        sector_has_larger_than_logical_sector_read_fault)
                    {
                        return error.InputOutput;
                    }

                    const data = io.files[op.fd].buffer;
                    stdx.copy_disjoint(.exact, u8, op.buf[0..op.len], data[op.offset..][0..op.len]);
                    return op.len;
                }
            },
        );
    }

    pub const WriteError = posix.PWriteError;

    pub fn write(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: WriteError!usize,
        ) void,
        completion: *Completion,
        fd: fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        assert(fd < self.files.len);

        self.submit(
            context,
            callback,
            completion,
            .write,
            .{
                .fd = fd,
                .buf = buffer.ptr,
                .len = @as(u32, @intCast(buffer_limit(buffer.len))),
                .offset = offset,
            },
            struct {
                fn do_operation(io: *IO, op: anytype) WriteError!usize {
                    const data = io.files[op.fd].buffer;
                    if (op.offset + op.len >= data.len) {
                        @panic("write beyond simulated file size");
                    }
                    stdx.copy_disjoint(.exact, u8, data[op.offset..][0..op.len], op.buf[0..op.len]);
                    return op.len;
                }
            },
        );
    }
};
