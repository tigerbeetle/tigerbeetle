test {
    _ = @import("vsr.zig");
    _ = @import("vsr/journal.zig");
    _ = @import("vsr/marzullo.zig");
    _ = @import("vsr/replica_format.zig");
    _ = @import("vsr/replica_test2.zig");
    _ = @import("vsr/superblock.zig");
    _ = @import("vsr/superblock_free_set.zig");
    _ = @import("vsr/superblock_manifest.zig");
    _ = @import("vsr/superblock_quorums.zig");
    // TODO: clean up logging of clock test and enable it here.
    //_ = @import("vsr/clock.zig");

    _ = @import("state_machine.zig");
    _ = @import("state_machine/auditor.zig");
    _ = @import("state_machine/workload.zig");

    _ = @import("fifo.zig");
    _ = @import("ring_buffer.zig");

    _ = @import("io.zig");
    _ = @import("ewah.zig");
    _ = @import("util.zig");

    _ = @import("clients/c/test.zig");
    _ = @import("clients/c/tb_client_header_test.zig");

    // TODO Add remaining unit tests from lsm namespace.
    _ = @import("lsm/forest.zig");
    _ = @import("lsm/manifest_level.zig");
    _ = @import("lsm/segmented_array.zig");

    _ = @import("test/id.zig");
    _ = @import("test/storage.zig");
    _ = @import("test/table.zig");

    _ = @import("clients/go/go_bindings_test.zig");
    _ = @import("clients/dotnet/dotnet_bindings_test.zig");

    // This one is a bit sketchy: we rely on tests not actually using the `vsr` package.
    _ = @import("tigerbeetle/cli.zig");
}
