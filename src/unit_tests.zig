test {
    _ = @import("vsr.zig");
    _ = @import("vsr/journal.zig");
    _ = @import("vsr/marzullo.zig");
    _ = @import("vsr/superblock.zig");
    _ = @import("vsr/superblock_manifest.zig");
    _ = @import("vsr/superblock_free_set.zig");
    // TODO: clean up logging of clock test and enable it here.
    //_ = @import("vsr/clock.zig");

    _ = @import("state_machine.zig");

    _ = @import("fifo.zig");
    _ = @import("ring_buffer.zig");

    _ = @import("io.zig");

    _ = @import("ewah.zig");
    _ = @import("util.zig");

    // TODO Add remaining unit tests from lsm namespace.
    _ = @import("lsm/forest.zig");
    _ = @import("lsm/manifest_level.zig");
    _ = @import("lsm/segmented_array.zig");

    _ = @import("test/id.zig");
    _ = @import("test/accounting/auditor.zig");
    _ = @import("test/accounting/workload.zig");
}
