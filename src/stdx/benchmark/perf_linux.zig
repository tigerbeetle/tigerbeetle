const std = @import("std");
const assert = std.debug.assert;
const PERF = std.os.linux.PERF;

const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;
const Instant = stdx.Instant;

const perf = @import("perf.zig");
const Time = @import("time.zig");
const EventType = perf.EventType;
const EventInterpretation = perf.EventType;
const PerfMeasurement = perf.PerfMeasurements;

/// Bitflags specifying events from which domain
/// the given performance counter should include
pub const PerfEventDomain = struct {
    flags: u32,

    const DomainFlag = enum(u32) {
        user = 1 << 0,
        kernel = 1 << 1,
        hypervisor = 1 << 2,
    };

    pub fn includes(domain: PerfEventDomain, subset: PerfEventDomain) bool {
        return domain.flags & subset.flags;
    }
    pub fn user() PerfEventDomain {
        return .{ .flags = @intFromEnum(.user) };
    }

    pub fn kernel() PerfEventDomain {
        return .{ .flags = @intFromEnum(.kernel) };
    }

    pub fn hypervisor() PerfEventDomain {
        return .{ .flags = @intFromEnum(.hypervisor) };
    }

    pub fn all() PerfEventDomain {
        return .{ .flags = user().flags | kernel().flags | hypervisor().flags };
    }
};

/// Bitflags specifying the format returned by read() calls to perf event counters
const PerfEventFormat = struct {
    flags: u64,
    /// Adds the 64-bit time_enabled field. This can be used to calculate
    /// estimated totals if the PMU is overcommitted and multiplexing is
    /// happening.
    const format_total_time_enabled = 1 << 0;
    /// Adds the 64-bit time_running field. This can be used to calculate
    /// estimated totals if the PMU is overcommitted and multiplexing is
    /// happening.
    const format_total_time_running = 1 << 1;
    /// Adds a 64-bit unique value that corresponds to the event group.
    // const format_id = 1 << 2;
    /// Allows all counter values in an event group to be read with one read.
    // const format_group = 1 << 3;
    /// (since Linux 6.0) Adds a 64-bit value that is the number of lost samples
    /// for this event. This would be only mean‐ ingful when sample_period or
    /// sample_freq is set.
    const format_lost = 1 << 4;

    pub fn default() PerfEventFormat {
        return .{ .flags = format_total_time_enabled | format_total_time_running | format_lost };
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
    interpretation: EventInterpretation,
};

const PerfEventCounter = struct {
    const ReadFormat = extern struct {
        value: u64 = 0,
        time_enabled: u64 = 0,
        time_running: u64 = 0,
    };
    prev: ?ReadFormat = null,
    current: ?ReadFormat = null,
    fd: std.posix.fd_t,
    interpretation: EventInterpretation,

    fn init(config: EventConfig) !PerfEventCounter {
        // open the file descriptor for this event
        const fd = try PerfEventCounter.register_perf_event_fd(&config);
        return .{ .fd = fd, .interpretation = config.interpretation };
    }

    fn start(counter: *PerfEventCounter) !void {
        // we are allowed to restart the counters
        maybe(counter.prev == null);
        maybe(counter.current == null);
        // reset and enable the counter
        const success_reset = std.os.linux.ioctl(counter.fd, PERF.EVENT_IOC.RESET, 0);
        if (success_reset > 0) return error.PerfCounterInit;
        const success_enable = std.os.linux.ioctl(counter.fd, PERF.EVENT_IOC.ENABLE, 0);
        if (success_enable > 0) return error.PerfCounterInit;
        // read the start value
        PerfEventCounter.read_perf_event_fd(counter.fd, &counter.prev) catch {
            return error.PerfCounterRead;
        };
        counter.current = null;
    }

    fn read(counter: *PerfEventCounter) !void {
        assert(counter.prev != null);
        maybe(counter.current == null);
        PerfEventCounter.read_perf_event_fd(counter.fd, &counter.current) catch {
            return error.PerfCounterRead;
        };
    }

    fn duration_enabled(counter: *PerfEventCounter) u64 {
        assert(counter.prev != null);
        assert(counter.current != null);
        return counter.current.time_enabled - counter.prev.time_enabled;
    }

    fn duration_running(counter: *PerfEventCounter) u64 {
        assert(counter.prev != null);
        assert(counter.current != null);
        return counter.current.time_running - counter.prev.time_running;
    }

    fn event_count(counter: *PerfEventCounter) u64 {
        assert(counter.prev != null);
        assert(counter.current != null);
        return counter.current.value - counter.prev.value;
    }

    fn get_counter(counter: *const PerfEventCounter, scale: f64) f64 {
        const time_enabled: f64 = @floatFromInt(counter.duration_enabled());
        const time_running: f64 = @floatFromInt(counter.duration_running());
        // read the event count, corrected by a calculated multiplexing factor, since
        // the hardware swaps out the underlying counters if there are fewer performance registers
        // than traced performance events
        const corrected_value = counter.event_count() * (time_enabled / time_running);
        return switch (counter.interpretation) {
            .raw => corrected_value,
            .scaled => corrected_value / scale,
        };
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
                .exclude_kernel = !config.event_domain.includes(.kernel),
                .exclude_hv = !config.event_domain.includes(.hypervisor),
                .exclude_user = !config.event_domain.includes(.user),
            },
            .read_format = PerfEventFormat.default(),
        };
        return std.posix.perf_event_open(&perf_event_attr, 0, -1, -1, PERF.FLAG.FD_CLOEXEC);
    }

    fn read_perf_event_fd(fd: std.posix.fd_t, read_format: *PerfEventCounter.ReadFormat) !void {
        const bytes_expected = @sizeOf(PerfEventCounter.ReadFormat);
        var bytes_read: usize = 0;
        var buffer = std.mem.asBytes(read_format);
        while (bytes_read < bytes_expected) {
            const n = try std.posix.read(fd, buffer[bytes_read..bytes_expected]);
            bytes_read += n;
        }
        assert(bytes_read == bytes_expected);
    }
};

fn event_config_from_event_type(event_type: EventType) EventConfig {
    return switch (event_type) {
        .cpu_cycles => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.CPU_CYCLES),
            .event_domain = PerfEventDomain.all(),
            .interpretation = .event_count_scaled,
        },
        .kernel_cycles => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.CPU_CYCLES),
            .event_domain = PerfEventDomain.kernel(),
            .interpretation = .event_count_scaled,
        },
        .stall_cycles => .{
            .event_type = PERF.TYPE.RAW,
            .event_id = 0x43FFAE,
            .event_domain = PerfEventDomain.all(),
            .interpretation = .event_count_scaled,
        },
        .instructions => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.INSTRUCTIONS),
            .event_domain = PerfEventDomain.all(),
            .interpretation = .event_count_scaled,
        },
        .cache_references => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.CACHE_REFERENCES),
            .event_domain = PerfEventDomain.all(),
            .interpretation = .event_count_scaled,
        },
        .cache_misses => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.CACHE_MISSES),
            .event_domain = PerfEventDomain.all(),
            .interpretation = .event_count_scaled,
        },
        .branch_misses => .{
            .event_type = PERF.TYPE.HARDWARE,
            .event_id = @intFromEnum(PERF.COUNT.HW.BRANCH_MISSES),
            .event_domain = PerfEventDomain.all(),
            .interpretation = .event_count_scaled,
        },
        .task_clock => .{
            .event_type = PERF.TYPE.SOFTWARE,
            .event_id = @intFromEnum(PERF.COUNT.SW.TASK_CLOCK),
            .event_domain = PerfEventDomain.all(),
            .event_interpretation = .event_count,
        },
        .dtlb_load_misses => .{ .event_type = PERF.TYPE.HW_CACHE, .event_id = cache_event_id(
            PERF.COUNT.HW.CACHE.DTLB,
            PERF.COUNT.HW.CACHE.OP.READ,
            PERF.COUNT.HW.CACHE.RESULT.MISS,
        ), .event_domain = PerfEventDomain.all(), .interpretation = .event_count_scaled },
    };
}

pub const PerfCounters = struct {
    counters: CounterMap,
    time: Time = .{},
    timer: ?Instant = null,

    const CounterMap = std.enums.EnumArray(EventType, PerfEventCounter);
    const DerivedCounters = struct { ipc: f64, ghz: f64, cores: f64 };

    pub fn init() !PerfCounters {
        const counters = CounterMap.initUndefined();
        inline for (std.enums.values(EventType)) |event_type| {
            const counter_config = event_config_from_event_type(event_type);
            counters.set(event_type, try .init(counter_config));
        }
        return .{ .counters = counters };
    }

    fn deinit(perf_counters: *PerfCounters) void {
        for (&perf_counters.counters) |*counter| {
            counter.deinit();
        }
    }

    fn start(perf_counters: *PerfCounters) !void {
        for (&perf_counters.counters) |*counter| {
            try counter.start();
        }
        perf_counters.timer = perf_counters.time.monotonic();
    }

    fn read(perf_counters: *PerfCounters, scale: f64) !PerfMeasurement {
        for (&perf_counters.counters) |*counter| {
            _ = try counter.read();
        }
        const elapsed = perf_counters.timer.elapsed(perf_counters.time.monotonic());
        const measurement: PerfMeasurement = .{ .counters = .initUndefined(), .elapsed = elapsed };
        inline for (std.enums.values(EventType)) |event_type| {
            measurement.counters.set(event_type, perf_counters.get_counter(event_type, scale));
        }
        return measurement;
    }

    pub fn get_counter(perf_counters: *PerfCounters, event_type: EventType, scale: f64) ?f64 {
        const counter = perf_counters.counters.getPtr(event_type) orelse return null;
        return counter.get_counter(scale);
    }

    fn derive_counters(
        perf_counters: *const PerfCounters,
        elapsed_ns: u64,
    ) DerivedCounters {
        // default counters are always in the set
        const cpu_cycles = perf_counters.get_counter(.cpu_cycles, 1.0) orelse unreachable;
        const task_clock = perf_counters.get_counter(.task_clock, 1.0) orelse unreachable;
        const instructions = perf_counters.get_counter(.instructions, 1.0) orelse unreachable;
        return .{
            .ipc = instructions / cpu_cycles,
            .cores = task_clock / @as(f64, @floatFromInt(elapsed_ns)),
            .ghz = cpu_cycles / task_clock,
        };
    }
};
