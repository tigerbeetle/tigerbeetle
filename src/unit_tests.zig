// zig fmt: off

test "src/ewah.zig" { _ = @import("ewah.zig"); }
test "src/fifo.zig" { _ = @import("fifo.zig"); }
test "src/io.zig" { _ = @import("io.zig"); }
test "src/ring_buffer.zig" { _ = @import("ring_buffer.zig"); }
test "src/stdx.zig" { _ = @import("stdx.zig"); }

test "src/clients/c/test.zig" { _ = @import("clients/c/test.zig"); }
test "src/clients/c/tb_client_header_test.zig" { _ = @import("clients/c/tb_client_header_test.zig"); }
test "src/clients/dotnet/dotnet_bindings_test.zig" { _ = @import("clients/dotnet/dotnet_bindings_test.zig"); }
test "src/clients/go/go_bindings_test.zig" { _ = @import("clients/go/go_bindings_test.zig"); }
test "src/clients/java/java_bindings.zig" { _ = @import("clients/java/java_bindings.zig"); }

// TODO Add remaining unit tests from lsm namespace.
test "src/lsm/forest.zig" { _ = @import("lsm/forest.zig"); }
test "src/lsm/manifest_level.zig" { _ = @import("lsm/manifest_level.zig"); }
test "src/lsm/segmented_array.zig" { _ = @import("lsm/segmented_array.zig"); }

test "src/state_machine.zig" { _ = @import("state_machine.zig"); }
test "src/state_machine/auditor.zig" { _ = @import("state_machine/auditor.zig"); }
test "src/state_machine/workload.zig" { _ = @import("state_machine/workload.zig"); }

test "src/testing/id.zig" { _ = @import("testing/id.zig"); }
test "src/testing/storage.zig" { _ = @import("testing/storage.zig"); }
test "src/testing/table.zig" { _ = @import("testing/table.zig"); }

// This one is a bit sketchy: we rely on tests not actually using the `vsr` package.
test "src/tigerbeetle/cli.zig" { _ = @import("tigerbeetle/cli.zig"); }

test "src/vsr.zig" { _ = @import("vsr.zig"); }
// TODO: clean up logging of clock test and enable it here.
//test "src/vsr/clock.zig" { _ = @import("vsr/clock.zig"); }
test "src/vsr/journal.zig" { _ = @import("vsr/journal.zig"); }
test "src/vsr/marzullo.zig" { _ = @import("vsr/marzullo.zig"); }
test "src/vsr/replica_format.zig" { _ = @import("vsr/replica_format.zig"); }
test "src/vsr/superblock.zig" { _ = @import("vsr/superblock.zig"); }
test "src/vsr/superblock_free_set.zig" { _ = @import("vsr/superblock_free_set.zig"); }
test "src/vsr/superblock_manifest.zig" { _ = @import("vsr/superblock_manifest.zig"); }
test "src/vsr/superblock_quorums.zig" { _ = @import("vsr/superblock_quorums.zig"); }
