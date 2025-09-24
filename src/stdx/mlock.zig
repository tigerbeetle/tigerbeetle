const builtin = @import("builtin");
const std = @import("std");
const os = std.os;

const stdx = @import("stdx.zig");

const MiB = stdx.MiB;

const log = std.log.scoped(.mlock);

const MemoryLockError = error{memory_not_locked} || std.posix.UnexpectedError;

const mlockall_error = "Unable to lock pages in memory ({s})" ++
    " - kernel swap would otherwise bypass TigerBeetle's storage fault tolerance. ";

/// Pin virtual memory pages allocated so far to physical pages in RAM, preventing the pages from
/// being swapped out and introducing storage error into memory, bypassing ECC RAM.
pub fn memory_lock_allocated(options: struct { allocated_size: usize }) MemoryLockError!void {
    switch (builtin.os.tag) {
        .linux => try memory_lock_allocated_linux(),
        .macos => {
            // macOS has mlock() but not mlockall(). mlock() requires an address range which
            // would be difficult to gather for non-heap memory that is also faulted in,
            // such as the stack, globals, etc.
        },
        .windows => try memory_lock_allocated_windows(options.allocated_size),
        else => @compileError("unsupported platform"),
    }
}

fn memory_lock_allocated_linux() MemoryLockError!void {
    // https://github.com/torvalds/linux/blob/v6.12/include/uapi/asm-generic/mman.h#L18-L20
    const MCL_CURRENT = 1; // Lock all currently mapped pages.
    const MCL_ONFAULT = 4; // Lock all pages faulted in (i.e. stack space).
    const result = os.linux.syscall1(.mlockall, MCL_CURRENT | MCL_ONFAULT);
    switch (os.linux.E.init(result)) {
        .SUCCESS => return,
        .AGAIN => log.warn(mlockall_error, .{"some addresses could not be locked"}),
        .NOMEM => log.warn(mlockall_error, .{"memory would exceed RLIMIT_MEMLOCK"}),
        .PERM => log.warn(mlockall_error, .{
            "insufficient privileges to lock memory",
        }),
        .INVAL => unreachable, // MCL_ONFAULT specified without MCL_CURRENT.
        else => |err| return stdx.unexpected_errno("mlockall", err),
    }
    return error.memory_not_locked;
}

fn memory_lock_allocated_windows(allocated_size: usize) MemoryLockError!void {
    // Windows has VirtualLock which works similar to mlock with an address range.
    // It would be difficult to gather the addresses of non-heap memory that is also
    // faulted in, such as the stack, globals, etc. SetProcessWorkingSetSize can be
    // used instead to lock all existing pages into memory to avoid swapping.
    const process_handle = os.windows.kernel32.GetCurrentProcess();
    var working_set_min: os.windows.SIZE_T = 0;
    var working_set_max: os.windows.SIZE_T = 0;

    if (stdx.windows.GetProcessWorkingSetSize(
        process_handle,
        &working_set_min,
        &working_set_max,
    ) == os.windows.FALSE) {
        working_set_min = allocated_size; // Count bytes allocated so far.
        working_set_min += 64 * MiB; // 64mb buffer room for stack/globals.
        working_set_max = working_set_min * 2; // Buffer room for new page faults.
    }

    if (stdx.windows.SetProcessWorkingSetSize(
        process_handle,
        working_set_min,
        working_set_max,
    ) == os.windows.FALSE) {
        // From std.os.windows.unexpectedError():
        const format_flags = os.windows.FORMAT_MESSAGE_FROM_SYSTEM |
            os.windows.FORMAT_MESSAGE_IGNORE_INSERTS;

        // 614 is the length of the longest windows error description.
        var buffer: [614:0]os.windows.WCHAR = undefined;
        const buffer_size = os.windows.kernel32.FormatMessageW(
            format_flags,
            null,
            os.windows.kernel32.GetLastError(),
            os.windows.LANG.NEUTRAL | (os.windows.SUBLANG.DEFAULT << 10),
            &buffer,
            buffer.len,
            null,
        );

        log.warn(mlockall_error, .{std.unicode.fmtUtf16Le(buffer[0..buffer_size])});
        return error.memory_not_locked;
    }
}
