const std = @import("std");
const assert = std.debug.assert;

// bit flags of event domains
const PerfEventDomain = struct {
    const Flags = enum(u32) {
        user = 0x1,
        kernel = 0x2,
        hypervisor = 0x4,
    };

    flags: u32,

    pub fn is_user(self: PerfEventDomain) bool {
        return (self.flags & user().flags) != 0;
    }
    pub fn is_kernel(self: PerfEventDomain) bool {
        return (self.flags & kernel().flags) != 0;
    }
    pub fn is_hypervisor(self: PerfEventDomain) bool {
        return (self.flags & hypervisor().flags) != 0;
    }

    pub fn user() PerfEventDomain {
        return .{ .flags = @intFromEnum(Flags.user) };
    }

    pub fn kernel() PerfEventDomain {
        return .{ .flags = @intFromEnum(Flags.kernel) };
    }

    pub fn hypervisor() PerfEventDomain {
        return .{ .flags = @intFromEnum(Flags.hypervisor) };
    }

    pub fn all() PerfEventDomain {
        return .{ .flags = user().flags | kernel().flags | hypervisor().flags };
    }
};

// This is the perf measurement event
const PerfMeasurement = struct {
    name: []const u8,
    type: std.os.linux.PERF.TYPE, // this is the perf event type (HW or SW)
    id: u64, // this is the perf ID
    event_domain: PerfEventDomain,
};

const PERF_MEASUREMENTS = [_]PerfMeasurement{
    // 0
    .{
        .name = "cpu_cycles",
        .type = std.os.linux.PERF.TYPE.HARDWARE,
        .id = @intFromEnum(std.os.linux.PERF.COUNT.HW.CPU_CYCLES),
        .event_domain = PerfEventDomain.all(),
    },
    // 1
    .{
        .name = "k_cycles",
        .type = std.os.linux.PERF.TYPE.HARDWARE,
        .id = @intFromEnum(std.os.linux.PERF.COUNT.HW.CPU_CYCLES),
        .event_domain = PerfEventDomain.kernel(),
    },
    // 2
    .{
        .name = "instructions",
        .type = std.os.linux.PERF.TYPE.HARDWARE,
        .id = @intFromEnum(std.os.linux.PERF.COUNT.HW.INSTRUCTIONS),
        .event_domain = PerfEventDomain.all(),
    },
    // 3
    .{
        .name = "cache_references",
        .type = std.os.linux.PERF.TYPE.HARDWARE,
        .id = @intFromEnum(std.os.linux.PERF.COUNT.HW.CACHE_REFERENCES),
        .event_domain = PerfEventDomain.all(),
    },
    // 4
    .{
        .name = "cache_misses",
        .type = std.os.linux.PERF.TYPE.HARDWARE,
        .id = @intFromEnum(std.os.linux.PERF.COUNT.HW.CACHE_MISSES),
        .event_domain = PerfEventDomain.all(),
    },
    // 5
    .{
        .name = "branch_misses",
        .type = std.os.linux.PERF.TYPE.HARDWARE,
        .id = @intFromEnum(std.os.linux.PERF.COUNT.HW.BRANCH_MISSES),
        .event_domain = PerfEventDomain.all(),
    },
    // 6
    .{
        .name = "task_clock",
        .type = std.os.linux.PERF.TYPE.SOFTWARE,
        .id = @intFromEnum(std.os.linux.PERF.COUNT.SW.TASK_CLOCK),
        .event_domain = PerfEventDomain.all(),
    },
};

pub fn PerfEventBlockType(comptime BenchParamType: type) type {
    return struct {
        const Self = @This();
        const Event = struct {
            const ReadFormat = extern struct {
                value: u64 = 0,
                time_enabled: u64 = 0,
                time_running: u64 = 0,
            };
            fd: std.posix.fd_t = -1,
            prev: ReadFormat = .{},
            current: ReadFormat = .{},
        };

        perf_events: [PERF_MEASUREMENTS.len]Event = [_]Event{.{}} ** PERF_MEASUREMENTS.len,
        begin_time: u64,
        timer: std.time.Timer,
        begin_rusage: std.posix.system.rusage,
        print_header: bool,
        scale: u64 = 1.0,
        bench_params: *BenchParamType,

        const Sample = struct {
            wall_time: f64,
            cpu_cycles: f64,
            k_cycles: f64,
            instructions: f64,
            cache_references: f64,
            cache_misses: f64,
            branch_misses: f64,
            ipc: f64,
            GHZ: f64,
            CPUs: f64,
            maxrss_mb: usize,
            scale: u64,
        };

        fn timeval_to_ns(tv: std.posix.timeval) u64 {
            const ns_per_us = std.time.ns_per_s / std.time.us_per_s;
            return @as(usize, @bitCast(tv.sec)) * std.time.ns_per_s + @as(usize, @bitCast(tv.usec)) * ns_per_us;
        }

        fn read_perf_fd(fd: std.posix.fd_t, read_format: *Event.ReadFormat) void {
            var read: usize = 0;
            var buffer = std.mem.asBytes(read_format);
            while (read < @sizeOf(Event.ReadFormat)) {
                const n = std.posix.read(fd, buffer[read..24]) catch |err| {
                    std.debug.panic("unable to read perf fd: {s}\n", .{@errorName(err)});
                };
                read += n;
            }
            std.debug.assert(read == @sizeOf(Event.ReadFormat));
        }

        fn read_counter(event: *Event) f64 {
            const multiplexing_correction = ((@as(f64, @floatFromInt(event.current.time_enabled))) - (@as(f64, @floatFromInt(event.prev.time_enabled)))) / ((@as(f64, @floatFromInt(event.current.time_running))) - (@as(f64, @floatFromInt(event.prev.time_running))));
            return (@as(f64, @floatFromInt(event.current.value - event.prev.value))) * multiplexing_correction;
        }

        fn register_counter(measurement: PerfMeasurement) std.posix.fd_t {
            var attr: std.os.linux.perf_event_attr = .{
                .type = measurement.type,
                .config = measurement.id,
                .flags = .{
                    .disabled = true,
                    .inherit = true,
                    .inherit_stat = false,
                    .exclude_kernel = !measurement.event_domain.is_kernel(),
                    .exclude_hv = !measurement.event_domain.is_hypervisor(),
                    .exclude_user = !measurement.event_domain.is_user(),
                },
                .read_format = 1 | 2,
            };

            const fd = std.posix.perf_event_open(&attr, 0, -1, -1, std.os.linux.PERF.FLAG.FD_CLOEXEC) catch |err| {
                switch (err) {
                    error.PermissionDenied => std.debug.panic("PermissionDenied: Adjust the permissions by executing e.g. `echo '-1' | sudo tee /proc/sys/kernel/perf_event_paranoid`", .{}),
                    else => std.debug.panic("Unable to open perf event: {s}\n", .{@errorName(err)}),
                }
            };
            return fd;
        }

        pub fn set_scale(self: *Self, scale: u64) void {
            self.scale = scale;
        }

        pub fn init(bench_params: *BenchParamType, print_header: bool) Self {
            var perf_events: [PERF_MEASUREMENTS.len]Event = [_]Event{.{}} ** PERF_MEASUREMENTS.len;
            //var perf_events = [_]u64{0} ** PERF_MEASUREMENTS.len;
            var timer = std.time.Timer.start() catch @panic("need timer to work");
            for (PERF_MEASUREMENTS, 0..) |measurement, i| {
                perf_events[i].fd = register_counter(measurement);
            }
            const begin_rusage = std.posix.getrusage(std.posix.rusage.SELF);

            for (PERF_MEASUREMENTS, 0..) |_, i| {
                {
                    const ioctl_success = std.os.linux.ioctl(perf_events[i].fd, std.os.linux.PERF.EVENT_IOC.RESET, 0);
                    if (ioctl_success != 0) @panic("ioctl return none-zero");
                }
                {
                    const ioctl_success = std.os.linux.ioctl(perf_events[i].fd, std.os.linux.PERF.EVENT_IOC.ENABLE, 0);
                    if (ioctl_success != 0) @panic("ioctl return none-zero");
                }
                read_perf_fd(perf_events[i].fd, &perf_events[i].prev);
            }
            const start = timer.read();
            return .{
                .begin_time = start,
                .perf_events = perf_events,
                .timer = timer,
                .begin_rusage = begin_rusage,
                .print_header = print_header,
                .bench_params = bench_params,
            };
        }

        pub fn deinit(self: *Self) void {
            for (&self.perf_events) |*event| {
                read_perf_fd(event.fd, &event.current);
            }

            const end_time = self.timer.read();
            const end_rusage = std.posix.getrusage(std.posix.rusage.SELF);
            const scale_f = @as(f64, @floatFromInt(self.scale));
            const cycles = read_counter(&self.perf_events[0]);
            const k_cycles = read_counter(&self.perf_events[1]);
            const task_clock = read_counter(&self.perf_events[6]);
            const instructions = (read_counter(&self.perf_events[2]));
            const sample = Sample{
                .wall_time = @as(f64, @floatFromInt(end_time - self.begin_time)) / std.time.ns_per_s,
                .cpu_cycles = (cycles / scale_f),
                .k_cycles = (k_cycles / scale_f),
                .instructions = (instructions / scale_f),
                .cache_references = (read_counter(&self.perf_events[3]) / scale_f),
                .cache_misses = (read_counter(&self.perf_events[4]) / scale_f),
                .branch_misses = (read_counter(&self.perf_events[5]) / scale_f),
                .ipc = instructions / cycles,
                .CPUs = task_clock / (@as(f64, @floatFromInt(timeval_to_ns(end_rusage.utime) - timeval_to_ns(self.begin_rusage.utime)))),
                .GHZ = cycles / task_clock,
                .maxrss_mb = (@as(usize, @bitCast(end_rusage.maxrss)) / 1024),
                .scale = self.scale,
            };
            for (&self.perf_events) |*event| {
                std.posix.close(event.fd);
                event.fd = -1;
            }
            // normalization and print
            const writer = std.io.getStdErr().writer();

            if (self.print_header) {
                // benchmark params
                serialize_header(writer, BenchParamType, self.bench_params.*);
                // print missing comma
                writer.print(",", .{}) catch {};
                // serialize sample header
                serialize_header(writer, Sample, sample);
                writer.print("\n", .{}) catch {};
            }
            // benchmark params
            serialize_values(writer, BenchParamType, self.bench_params.*);
            // print missing comma
            writer.print(",", .{}) catch {};
            // serialize sample header
            serialize_values(writer, Sample, sample);
            writer.print("\n", .{}) catch {};
        }

        fn serialize_header(writer: anytype, comptime Row: type, row: Row) void {
            inline for (std.meta.fields(@TypeOf(row)), 0..) |f, i| {
                if (i > 0) writer.print(",", .{}) catch {};
                writer.print("{s}", .{f.name}) catch {};
            }
        }

        fn serialize_values(writer: anytype, comptime Row: type, row: Row) void {
            inline for (std.meta.fields(@TypeOf(row)), 0..) |f, i| {
                if (i > 0) writer.print(",", .{}) catch {};
                switch (@typeInfo(f.type)) {
                    .int => {
                        writer.print("{d}", .{@field(row, f.name)}) catch {
                            std.log.debug("", .{});
                        };
                    },
                    .float => {
                        writer.print("{d:.2}", .{@field(row, f.name)}) catch {
                            std.log.debug("", .{});
                        };
                    },
                    .pointer => {
                        writer.print("{s}", .{@field(row, f.name)}) catch {
                            std.log.debug("", .{});
                        };
                    },
                    .bool => {
                        writer.print("{}", .{@field(row, f.name)}) catch {
                            std.log.debug("", .{});
                        };
                    },
                    .@"enum" => {
                        writer.print("{any}", .{@field(row, f.name)}) catch {
                            std.log.debug("", .{});
                        };
                    },
                    else => {
                        @panic("Type not supported for serialization");
                    },
                }
            }
        }
    };
}
