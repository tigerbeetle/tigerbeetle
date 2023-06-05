---
sidebar_position: 3
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
3. Role (according to the replica itself):
    - `/`: primary
    - `\`: backup
    - `|`: standby
    - `#`: (crashed)
4. Status:
    - The column (e.g. `.   ` vs `   .`) corresponds to the replica index. (This can help identify events' replicas at a quick glance.)
    - The symbol indicates the `replica.status`.
    - `.`: `normal`
    - `v`: `view_change`
    - `r`: `recovering`
    - `h`: `recovering_head`
5. View: e.g. `74V` indicates `replica.view=74`.
6. Commit: e.g. `150/160C` indicates `replica.commit_min=150` and `replica.commit_max=160`.
7. Journal op: e.g. `87:150Jo` indicates that the minimum op in the journal is `87` and the maximum is `150`.
8. Journal dirty: `0Jd` indicates that the journal has 0 dirty headers.
9. Journal faulty: `0Jf` indicates that the journal has 0 faulty headers.
10. WAL prepare ops: e.g. `85:149Wo` indicates that the op of the oldest prepare in the WAL is `85` and the op of the newest prepare in the WAL is `149`.
11. Grid blocks free: e.g. `122695Gf` indicates that the grid has `122695` blocks free.
12. Pipeline prepares (primary-only): e.g. `2/4Pp` indicates that the primary's pipeline has 2 prepares queued, out of a capacity of 4.
13. Pipeline requests (primary-only): e.g. `0/3Pq` indicates that the primary's pipeline has 0 requests queued, out of a capacity of 3.

### Example

```
 1 2 3 4------- 5-- 6-------  7-------  8--  9--  10------  11------   12---  13---

 0   \ .        74V 146/146C  86:149Jo  2Jd  0Jf  86:149Wo  122695Gf
 4   /     .    74V 147/147C  86:149Jo  0Jd  0Jf  86:149Wo  122695Gf   3/4Pp  0/3Pq
 0   \ .        74V 147/147C  86:149Jo  2Jd  0Jf  86:149Wo  122695Gf
 3   \    .     74V 147/147C  84:147Jo  0Jd  0Jf  84:147Wo  122695Gf
 2   \   .      74V 147/147C  84:147Jo  0Jd  0Jf  84:147Wo  122695Gf
 1   \  .       74V 147/147C  85:148Jo  0Jd  0Jf  85:148Wo  122695Gf
 4   /     .    74V 148/148C  86:149Jo  0Jd  0Jf  86:149Wo  122695Gf   2/4Pp  0/3Pq
 1   \  .       74V 148/148C  85:148Jo  0Jd  0Jf  85:148Wo  122695Gf
 2   \   .      74V 148/148C  85:148Jo  0Jd  0Jf  85:148Wo  122695Gf
 0   \ .        74V 148/148C  87:150Jo  1Jd  0Jf  86:149Wo  122695Gf
 3   \    .     74V 148/148C  85:148Jo  0Jd  0Jf  85:148Wo  122695Gf
 2 $ #   #
 4   /     .    74V 149/149C  87:150Jo  0Jd  0Jf  87:150Wo  122695Gf   2/4Pp  0/3Pq
 0   \ .        74V 149/149C  87:150Jo  0Jd  0Jf  87:150Wo  122695Gf
 3   \    .     74V 149/149C  86:149Jo  0Jd  0Jf  86:149Wo  122695Gf
 1   \  .       74V 149/149C  86:149Jo  0Jd  0Jf  86:149Wo  122695Gf
 4   /     .    74V 150/150C  87:150Jo  0Jd  0Jf  87:150Wo  122695Gf   1/4Pp  0/3Pq
 0   \ .        74V 150/150C  87:150Jo  0Jd  0Jf  87:150Wo  122695Gf
 1   \  .       74V 150/150C  87:150Jo  0Jd  0Jf  87:150Wo  122695Gf
 3   \    .     74V 150/150C  87:150Jo  0Jd  0Jf  87:150Wo  122695Gf
 4   /     .    74V 151/151C  91:154Jo  0Jd  0Jf  91:154Wo  122695Gf   4/4Pp  0/3Pq
```
