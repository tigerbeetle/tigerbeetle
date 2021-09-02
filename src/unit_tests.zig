test {
    _ = @import("vr.zig");
    _ = @import("vr/journal.zig");
    // TODO: clean up logging of clock test and enable it here.
    //_ = @import("vr/clock.zig");

    _ = @import("state_machine.zig");

    _ = @import("fifo.zig");
    _ = @import("ring_buffer.zig");
    _ = @import("marzullo.zig");

    _ = @import("io.zig");
}
