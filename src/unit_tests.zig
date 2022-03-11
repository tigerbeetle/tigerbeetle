test {
    _ = @import("vsr.zig");
    _ = @import("vsr/journal.zig");
    _ = @import("vsr/marzullo.zig");
    // TODO: clean up logging of clock test and enable it here.
    //_ = @import("vsr/clock.zig");

    _ = @import("state_machine.zig");

    _ = @import("fifo.zig");
    _ = @import("ring_buffer.zig");

    _ = @import("io.zig");

    _ = @import("ewah.zig");
    _ = @import("util.zig");
    _ = @import("lsm/block_free_set.zig");
}
