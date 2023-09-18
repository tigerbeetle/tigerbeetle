---
sidebar_position: 5
---

# Testing

Documentation for (roughly) code in the `src/testing` directory.

## VOPR Output

### Columns

1. Replica index.
2. Event:
    - `$`: crash
    - `^`: recover
    - ` `: commit
    - `[`: checkpoint start
    - `]`: checkpoint done
    - `<`: sync start (or change target)
    - `>`: sync done
3. Role (according to the replica itself):
    - `/`: primary
    - `\`: backup
    - `|`: standby
    - `~`: syncing
    - `#`: (crashed)
4. Status:
    - The column (e.g. `.   ` vs `   .`) corresponds to the replica index. (This can help identify events' replicas at a quick glance.)
    - The symbol indicates the `replica.status`.
    - `.`: `normal`
    - `v`: `view_change`
    - `r`: `recovering`
    - `h`: `recovering_head`
    - `s`: `sync`
5. View: e.g. `74V` indicates `replica.view=74`.
6. Commit: e.g. `150/160C` indicates `replica.commit_min=150` and `replica.commit_max=160`.
7. Journal op: e.g. `87:150Jo` indicates that the minimum op in the journal is `87` and the maximum is `150`.
8. Journal faulty/dirty: `0/1Jd` indicates that the journal has 0 faulty headers and 1 dirty headers.
9. WAL prepare ops: e.g. `85:149Wo` indicates that the op of the oldest prepare in the WAL is `85` and the op of the newest prepare in the WAL is `149`.
10. Syncing ops: e.g. `<0:123>` indicates that `vsr_state.sync_op_min=0` and `vsr_state.sync_op_max=123`.
11. Grid blocks free: e.g. `122695Gf` indicates that the grid has `122695` blocks free.
12. Grid blocks queued `grid.read_remote_queue`: e.g. `0G!` indicates that there are `0` reads awaiting remote fulfillment.
13. Grid blocks queued `grid_blocks_missing`: e.g. `0G?` indicates that there are `0` blocks awaiting remote repair.
14. Pipeline prepares (primary-only): e.g. `1/4Pp` indicates that the primary's pipeline has 2 prepares queued, out of a capacity of 4.
15. Pipeline requests (primary-only): e.g. `0/3Pq` indicates that the primary's pipeline has 0 requests queued, out of a capacity of 3.

### Example

(The first line labels the columns, but is not part of the actual VOPR output).

```
 1 2 3 4--------------- 5- 6-------  7-------  8-----  9-------   10-----   11-----  12-   13-   14---  15---

11   |            .     1V 126/126C  64:127Jo  0/_1J!  64:127Wo   <0:__0>   44857Gf  0G!   0G?
 0   \ .                1V 126/126C  64:127Jo  0/_2J!  63:126Wo   <0:__0>   44857Gf  0G!   0G?
 6   |       .          1V 126/126C  64:127Jo  0/_2J!  62:125Wo   <0:__0>   44857Gf  0G!   0G?
 7   |        .         1V 126/126C  64:127Jo  0/_2J!  64:127Wo   <0:__0>   44857Gf  0G!   0G?
 9   |          .       1V 126/126C  64:127Jo  0/_2J!  62:125Wo   <0:__0>   44857Gf  0G!   0G?
 1   /  .               1V 127/127C  64:127Jo  0/_0J!  64:127Wo   <0:__0>   44857Gf  0G!   0G?   1/4Pp  0/3Rq
 2   \   .              1V 127/127C  64:127Jo  0/_0J!  64:127Wo   <0:__0>   44857Gf  0G!   0G?
 9   |          .       1V 127/127C  64:127Jo  0/_2J!  63:126Wo   <0:__0>   44857Gf  0G!   0G?
 7   |        .         1V 127/127C  64:127Jo  0/_2J!  64:127Wo   <0:__0>   44857Gf  0G!   0G?
 6   |       .          1V 127/127C  64:127Jo  0/_2J!  63:126Wo   <0:__0>   44857Gf  0G!   0G?
11   |            .     1V 127/127C  64:127Jo  0/_0J!  64:127Wo   <0:__0>   44857Gf  0G!   0G?
10 ^ |           h      1V   0/_11C  15:_15Jo 31/31J!   0:_15Wo   <0:__0>   45056Gf  0G!   0G?
 0   \ .                1V 127/127C  64:127Jo  0/_1J!  63:126Wo   <0:__0>   44857Gf  0G!   0G?
 4   \     .            1V 127/127C  64:127Jo  0/_1J!  63:126Wo   <0:__0>   44857Gf  0G!   0G?
 5   \      .           1V 127/127C  64:127Jo  0/_0J!  64:127Wo   <0:__0>   44857Gf  0G!   0G?
 3   \    .             1V 127/127C  64:127Jo  0/_1J!  63:126Wo   <0:__0>   44857Gf  0G!   0G?
 8   |         .        1V 127/127C  64:127Jo  0/_1J!  63:126Wo   <0:__0>   44857Gf  0G!   0G?
10 < ~           h      1V   0/_11C  15:_15Jo 31/31J!   0:_15Wo   <0:__0>   45056Gf  0G!   0G?
10 > |           h      1V 119/119C  15:_15Jo 31/31J!   0:_15Wo   <0:123>   44877Gf  0G!   0G?
```
