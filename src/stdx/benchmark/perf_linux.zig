const std = @import("std");
const assert = std.debug.assert;
const PERF = std.os.linux.PERF;

const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;
const Instant = stdx.Instant;

const Time = @import("time.zig");
const CounterType = @import("perf.zig").CounterType;
const PerfMeasurement = @import("perf.zig").PerfMeasurement;

/// Bitflags specifying events from which domain
/// the given performance counter should include
pub const PerfEventDomain = packed struct(u32) {
    user: bool,
    kernel: bool,
    hypervisor: bool,
    padding: u29 = 0,

    fn user_only() PerfEventDomain {
        return .{ .user = true, .kernel = false, .hypervisor = false };
    }

    fn kernel_only() PerfEventDomain {
        return .{ .user = false, .kernel = true, .hypervisor = false };
    }

    fn all() PerfEventDomain {
        return .{ .user = true, .kernel = true, .hypervisor = true };
    }

    fn int(domain: PerfEventDomain) u32 {
        return @bitCast(domain);
    }
};

comptime {
    assert(PerfEventDomain.user_only().int() == 0b001);
    assert(PerfEventDomain.kernel_only().int() == 0b010);
    assert(PerfEventDomain.all().int() == 0b111);
}

/// Bitflags specifying the format returned by read() calls to perf event counters
const PerfEventFormat = packed struct(u64) {
    /// Adds the 64-bit time_enabled field. This can be used to calculate
    /// estimated totals if the PMU is overcommitted and multiplexing is
    /// happening.
    total_time_enabled: bool = true,

    /// Adds the 64-bit time_running field. This can be used to calculate
    /// estimated totals if the PMU is overcommitted and multiplexing is
    /// happening.
    total_time_running: bool = true,

    /// Adds a 64-bit unique value that corresponds to the event group.
    id: bool = false,

    /// Allows all counter values in an event group to be read with one read.
    format_group: bool = false,

    /// (since Linux 6.0) Adds a 64-bit value that is the number of lost samples
    /// for this event. This would be only meaningful when sample_period or
    /// sample_freq is set.
    format_lost: bool = false,

    padding: u59 = 0,

    pub fn default() PerfEventFormat {
        return .{};
    }
};

inline fn cache_event_id(
    cache: PERF.COUNT.HW.CACHE,
    op: PERF.COUNT.HW.CACHE.OP,
    result: PERF.COUNT.HW.CACHE.RESULT,
) u64 {
    return (@as(u64, @intFromEnum(cache)) // L1D, L1I, L2, DTLB, ...
        | (@as(u64, @intFromEnum(op)) << 8) // read, write, ...
        | (@as(u64, @intFromEnum(result)) << 16) // access, miss, ...
    );
}

const EventConfig = struct {
    event_type: PERF.TYPE,
    event_id: u64,
    event_domain: PerfEventDomain,
};

const PerfEventCounter = struct {
    const ReadFormat = extern struct {
        value: u64 = 0,
        time_enabled: u64 = 0,
        time_running: u64 = 0,
    };
    count_initial: ?ReadFormat = null,
    count_current: ?ReadFormat = null,
    fd: std.posix.fd_t,

    fn init(config: EventConfig) !PerfEventCounter {
        // open the file descriptor for this event
        const fd = try PerfEventCounter.register_perf_event_fd(&config);
        return .{ .fd = fd };
    }

    pub fn start(counter: *PerfEventCounter) !void {
        // we are allowed to restart the counters
        maybe(counter.count_initial == null);
        maybe(counter.count_current == null);

        // reset and enable the counter
        const success_reset = std.os.linux.ioctl(counter.fd, PERF.EVENT_IOC.RESET, 0);
        if (success_reset > 0) return error.PerfCounterInit;
        const success_enable = std.os.linux.ioctl(counter.fd, PERF.EVENT_IOC.ENABLE, 0);
        if (success_enable > 0) return error.PerfCounterInit;

        // read the start value
        counter.count_initial = PerfEventCounter.read_perf_event_fd(counter.fd) catch {
            return error.PerfCounterRead;
        };
        counter.count_current = null;
    }

    fn read(counter: *PerfEventCounter) !void {
        assert(counter.count_initial != null);
        maybe(counter.count_current == null);

        counter.count_current = PerfEventCounter.read_perf_event_fd(counter.fd) catch {
            return error.PerfCounterRead;
        };
    }

    fn start_new_lap(counter: *PerfEventCounter) void {
        assert(counter.count_initial != null);
        assert(counter.count_current != null);

        counter.count_initial = counter.count_current;
        counter.count_current = null;
    }

    fn duration_enabled(counter: *const PerfEventCounter) u64 {
        assert(counter.count_initial != null);
        assert(counter.count_current != null);

        return counter.count_current.?.time_enabled - counter.count_initial.?.time_enabled;
    }

    fn duration_running(counter: *const PerfEventCounter) u64 {
        assert(counter.count_initial != null);
        assert(counter.count_current != null);

        return counter.count_current.?.time_running - counter.count_initial.?.time_running;
    }

    fn event_count_raw(counter: *const PerfEventCounter) u64 {
        assert(counter.count_initial != null);
        assert(counter.count_current != null);

        return counter.count_current.?.value - counter.count_initial.?.value;
    }

    fn event_count(counter: *const PerfEventCounter) f64 {
        const time_enabled: f64 = @floatFromInt(counter.duration_enabled());
        const time_running: f64 = @floatFromInt(counter.duration_running());
        // read the event count, corrected by a calculated multiplexing factor, since
        // the hardware swaps out the underlying counters if there are fewer performance registers
        // than traced performance events
        const count_measured: f64 = @floatFromInt(counter.event_count_raw());
        const count_inferred = count_measured * (time_enabled / time_running);
        return count_inferred;
    }

    fn deinit(counter: *PerfEventCounter) void {
        assert(counter.fd > 0);
        _ = std.posix.system.close(counter.fd);
        counter.fd = -1;
    }

    fn register_perf_event_fd(config: *const EventConfig) !std.posix.fd_t {
        var perf_event_attr: std.os.linux.perf_event_attr = .{
            .type = config.event_type,
            .config = config.event_id,
            .flags = .{
                .disabled = true,
                .inherit = true,
                .inherit_stat = false,
                .exclude_kernel = !config.event_domain.kernel,
                .exclude_hv = !config.event_domain.hypervisor,
                .exclude_user = !config.event_domain.user,
            },
            .read_format = @bitCast(PerfEventFormat.default()),
        };
        return std.posix.perf_event_open(&perf_event_attr, 0, -1, -1, PERF.FLAG.FD_CLOEXEC);
    }

    fn read_perf_event_fd(fd: std.posix.fd_t) !PerfEventCounter.ReadFormat {
        var read_format: PerfEventCounter.ReadFormat = undefined;
        const bytes_read = try std.posix.read(fd, std.mem.asBytes(&read_format));
        assert(bytes_read == @sizeOf(PerfEventCounter.ReadFormat));
        return read_format;
    }
};

fn event_config_from_event_type(event_type: CounterType) EventConfig {
    return switch (event_type) {
        .cycles_cpu => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.CPU_CYCLES),
            .event_domain = PerfEventDomain.all(),
        },
        .cycles_kernel => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.CPU_CYCLES),
            .event_domain = PerfEventDomain.kernel_only(),
        },
        .cycles_stall => .{
            .event_type = PERF.TYPE.RAW,
            .event_id = 0x43FFAE,
            .event_domain = PerfEventDomain.all(),
        },
        .instructions => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.INSTRUCTIONS),
            .event_domain = PerfEventDomain.all(),
        },
        .cache_references => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.CACHE_REFERENCES),
            .event_domain = PerfEventDomain.all(),
        },
        .cache_misses => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.CACHE_MISSES),
            .event_domain = PerfEventDomain.all(),
        },
        .branch_misses => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.BRANCH_MISSES),
            .event_domain = PerfEventDomain.all(),
        },
        .task_clock => .{
            .event_type = PERF.TYPE.SOFTWARE,
            .event_id = @intFromEnum(PERF.COUNT.SW.TASK_CLOCK),
            .event_domain = PerfEventDomain.all(),
        },
        .dtlb_load_misses => .{
            .event_type = PERF.TYPE.HW_CACHE,
            .event_id = cache_event_id(
                PERF.COUNT.HW.CACHE.DTLB,
                PERF.COUNT.HW.CACHE.OP.READ,
                PERF.COUNT.HW.CACHE.RESULT.MISS,
            ),
            .event_domain = PerfEventDomain.all(),
        },
    };
}

pub const PerfCounters = struct {
    counters: std.enums.EnumArray(CounterType, PerfEventCounter),
    time: Time = .{},
    timer: ?Instant = null,

    pub fn init() !PerfCounters {
        var counters = std.enums.EnumArray(CounterType, PerfEventCounter).initUndefined();

        inline for (comptime std.enums.values(CounterType)) |event_type| {
            counters.getPtr(event_type).fd = -1;
        }

        inline for (comptime std.enums.values(CounterType)) |event_type| {
            const counter_config = event_config_from_event_type(event_type);
            const counter = try PerfEventCounter.init(counter_config);
            counters.set(event_type, counter);
        }

        errdefer {
            inline for (comptime std.enums.values(CounterType)) |event_type| {
                const counter = counters.get(event_type);
                if (counter.fd != -1) counter.deinit();
            }
        }

        return .{ .counters = counters };
    }

    pub fn deinit(perf_counters: *PerfCounters) void {
        for (&perf_counters.counters.values) |*counter| {
            counter.deinit();
        }
    }

    pub fn start(perf_counters: *PerfCounters) !void {
        for (&perf_counters.counters.values) |*counter| {
            try counter.start();
        }
        perf_counters.timer = perf_counters.time.benchmark_monotonic();
    }

    fn read(perf_counters: *PerfCounters, scale: f64, checksum: u64) !PerfMeasurement {
        assert(perf_counters.timer != null);
        assert(scale != 0);

        for (&perf_counters.counters.values) |*counter| {
            _ = try counter.read();
        }
        const elapsed = perf_counters.timer.?.elapsed(perf_counters.time.benchmark_monotonic());
        var measurement: PerfMeasurement = .{
            .counters = .initUndefined(),
            .elapsed = elapsed,
            .checksum = checksum,
            .scale = scale,
        };
        inline for (comptime std.enums.values(CounterType)) |counter_type| {
            measurement.counters.set(counter_type, perf_counters.event_count(counter_type));
        }
        return measurement;
    }

    pub fn lap(perf_counters: *PerfCounters, scale: f64, checksum: u64) !PerfMeasurement {
        assert(perf_counters.timer != null);
        assert(scale != 0);

        const measurement = perf_counters.read(scale, checksum);
        for (&perf_counters.counters.values) |*counter| {
            counter.start_new_lap();
        }
        return measurement;
    }

    pub fn event_count(perf_counters: *PerfCounters, event_type: CounterType) f64 {
        const counter = perf_counters.counters.getPtr(event_type);
        return counter.event_count();
    }
};
