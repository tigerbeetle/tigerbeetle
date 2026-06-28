const std = @import("std");
const stdx = @import("stdx");

const Shell = stdx.Shell;
const Snap = stdx.Snap;
const snap = Snap.snap_fn("src");

const tigerbeetle: []const u8 = @import("test_options").tigerbeetle_exe;

test "inspect constants snapshot" {
    const shell = try Shell.create(std.testing.allocator);
    defer shell.destroy();

    const output = try shell.exec_stdout(
        "{tigerbeetle} inspect constants",
        .{ .tigerbeetle = tigerbeetle },
    );

    // Note: This test is compatibility-sensitive. If you update it,
    // please request a third pair of eyes.
    try snap(@src(),
        \\VSR:
        \\prepare_queue                   8
        \\request_queue                   57
        \\prepare_cache                   65
        \\
        \\LSM:
        \\compaction_ops                  32
        \\
        \\Checkpoint Schedule:
        \\checkpoint_ops                  960
        \\journal_slot_count              1024
        \\op_checkpoint                   959
        \\op_checkpoint_trigger           991
        \\op_prepare_ok_max               999
        \\op_prepare_max                  1007
        \\op_checkpoint_next              1919
        \\
        \\Data File Layout:
        \\superblock                      96KiB
        \\  copy                          24KiB x4
        \\
        \\wal_headers                     256KiB
        \\  sector                        4KiB x64
        \\    header                      256B x16
        \\
        \\wal_prepares                    1GiB
        \\  prepare                       1MiB x1024
        \\
        \\client_replies                  64MiB
        \\  reply                         1MiB x64
        \\
        \\grid_padding                    160KiB
        \\
        \\grid                            elastic
        \\  block                         512KiB
        \\
        \\StateMachine:
        \\Account                         272B
        \\  object                        id=7  K=8B  V=128B T=524160 B=4094  BC=129 IL=256+4128,4384/5416+1032,6448+1032,7480+516808 VL=256+524032,524288+0
        \\  id                            id=1  K=16B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1072+272,1344+136,1480+522808 VL=256+524032,524288+0
        \\  user_data_128                 id=2  K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  user_data_64                  id=3  K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  user_data_32                  id=4  K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  ledger                        id=5  K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  code                          id=6  K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  imported                      id=23 K=8B  V=8B   T=262080 B=65504 BC=5   IL=256+160,416/456+40,496+40,536+523752 VL=256+524032,524288+0
        \\  closed                        id=25 K=8B  V=8B   T=524160 B=65504 BC=9   IL=256+288,544/616+72,688+72,760+523528 VL=256+524032,524288+0
        \\
        \\Transfer                        416B
        \\  object                        id=18 K=8B  V=128B T=262080 B=4094  BC=65  IL=256+2080,2336/2856+520,3376+520,3896+520392 VL=256+524032,524288+0
        \\  id                            id=8  K=16B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1072+272,1344+136,1480+522808 VL=256+524032,524288+0
        \\  debit_account_id              id=9  K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  credit_account_id             id=10 K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  amount                        id=11 K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  pending_id                    id=12 K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  user_data_128                 id=13 K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  user_data_64                  id=14 K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  user_data_32                  id=15 K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  ledger                        id=16 K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  code                          id=17 K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  expires_at                    id=19 K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  imported                      id=24 K=8B  V=8B   T=262080 B=65504 BC=5   IL=256+160,416/456+40,496+40,536+523752 VL=256+524032,524288+0
        \\  closing                       id=26 K=8B  V=8B   T=262080 B=65504 BC=5   IL=256+160,416/456+40,496+40,536+523752 VL=256+524032,524288+0
        \\
        \\TransferPending                 32B
        \\  object                        id=20 K=8B  V=16B  T=524160 B=32752 BC=17  IL=256+544,800/936+136,1072+136,1208+523080 VL=256+524032,524288+0
        \\  status                        id=21 K=16B V=16B  T=524160 B=32752 BC=17  IL=256+544,800/1072+272,1344+136,1480+522808 VL=256+524032,524288+0
        \\
        \\AccountEvent                    424B
        \\  object                        id=22 K=8B  V=256B T=262080 B=2047  BC=129 IL=256+4128,4384/5416+1032,6448+1032,7480+516808 VL=256+524032,524288+0
        \\  transfer_pending_status       id=28 K=16B V=16B  T=262080 B=32752 BC=9   IL=256+288,544/688+144,832+72,904+523384 VL=256+524032,524288+0
        \\  account_timestamp             id=27 K=16B V=16B  T=524160 B=32752 BC=17  IL=256+544,800/1072+272,1344+136,1480+522808 VL=256+524032,524288+0
        \\  dr_account_id_expired         id=29 K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  cr_account_id_expired         id=30 K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  transfer_pending_id_expired   id=31 K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  ledger_expired                id=32 K=32B V=32B  T=262080 B=16376 BC=17  IL=256+544,800/1344+544,1888+136,2024+522264 VL=256+524032,524288+0
        \\  prunable                      id=33 K=8B  V=8B   T=262080 B=65504 BC=5   IL=256+160,416/456+40,496+40,536+523752 VL=256+524032,524288+0
        \\
        \\Memory (approximate):
        \\datafile (on disk)              64TiB
        \\free_set                        32.29MiB
        \\
    ).diff(output);
}
