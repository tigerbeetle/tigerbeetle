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
6. Checkpoint and Commit: e.g. `83/_90/_98C` indicates that:
   - the highest checkpointed op at the replica is `83` (`replica.op_checkpoint()=83`),
   - on top of that checkpoint, the replica applied ops up to and including `90` (`replica.commit_min=90`),
   - replica knows that ops at least up to `98` are committed in the cluster (`replica.commit_max=98`).
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
 1 2 3 4------------- 5---  6----------  7-------  8-----  9------- 10-----     11-----  12-   13-   14---  15---

11   |            .   354V  83/_97/_97C  66:_97Jo  0/_0J!  66:_97Wo <__0:__0>   48894Gf  0G!   0G?
 7   |        .       354V  83/_97/_97C  66:_97Jo  0/_0J!  66:_97Wo <__0:__0>   48894Gf  0G!   0G?
 6   |       .        354V  83/_97/_97C  66:_97Jo  0/_0J!  66:_97Wo <__0:__0>   48894Gf  0G!   0G?
 9   |          .     354V  83/_97/_97C  66:_97Jo  0/_0J!  66:_97Wo <__0:__0>   48894Gf  0G!   0G?
 2   \   .            354V  83/_97/_97C  66:_97Jo  0/_0J!  66:_97Wo <__0:__0>   48894Gf  0G!   0G?
 0   / .              366V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?   1/4Pp  0/3Rq
 3   \    .           366V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?
 6   |       .        366V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?
 7   |        .       366V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?
 5 ^ \      v         192V  27/_27/_49C  20:_51Jo  0/_0J!  20:_51Wo <__0:__0>    nullGf  0G!   0G?
 5 < ~      v         367V  27/_27/_49C  20:_51Jo  0/_0J!  20:_51Wo <__0:__0>   49108Gf  0G!   0G?
 1   /  .             367V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?   1/4Pp  0/3Rq
 2   \   .            367V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?
 4   \     .          367V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?
 9   |          .     367V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?
11   |            .   367V  83/_98/_98C  67:_98Jo  0/_0J!  67:_98Wo <__0:__0>   48894Gf  0G!   0G?
 5 > \      h         367V  83/_83/_83C  20:_51Jo  0/_0J!  20:_51Wo <__0:_87>    nullGf  0G!   0G?
```
