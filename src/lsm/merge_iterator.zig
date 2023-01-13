const std = @import("std");
const assert = std.debug.assert;

const util = @import("../util.zig");

pub fn MergeIteratorType(
    comptime Table: type,
    comptime IteratorA: type,
    comptime IteratorB: type,
) type {
    return struct {
        const Self = @This();

        iterator_a: *IteratorA,
        iterator_b: *IteratorB,
        iterator_b_may_contain_tombstones: bool,

        empty_a: bool,
        empty_b: bool,

        previous_key_popped: ?Table.Key,

        pub fn init(
            iterator_a: *IteratorA,
            iterator_b: *IteratorB,
            iterator_b_may_contain_tombstones: bool,
        ) Self {
            return Self{
                .iterator_a = iterator_a,
                .iterator_b = iterator_b,
                .iterator_b_may_contain_tombstones = iterator_b_may_contain_tombstones,
                .empty_a = false,
                .empty_b = false,
                .previous_key_popped = null,
            };
        }

        /// Returns `true` once both `iterator_a` and `iterator_b` have raised `error.Empty`.
        pub fn empty(it: Self) bool {
            return it.empty_a and it.empty_b;
        }

        /// Returns `null` if either `iterator_a` or `iterator_b` return `error.Empty` or `error.Drained`.
        /// Check `it.empty()` to disambiguate.
        pub fn pop(it: *Self) ?Table.Value {
            while (true) {
                if (it.empty_a and it.empty_b) {
                    return null;
                }

                if (it.empty_a) {
                    _ = it.iterator_b.peek() catch |error_b| switch (error_b) {
                        error.Drained => return null,
                        error.Empty => {
                            it.empty_b = true;
                            continue;
                        },
                    };
                    const value_b = it.iterator_b.pop();
                    assert(it.iterator_b_may_contain_tombstones or !Table.tombstone(&value_b));
                    return value_b;
                }

                if (it.empty_b) {
                    _ = it.iterator_a.peek() catch |error_a| switch (error_a) {
                        error.Drained => return null,
                        error.Empty => {
                            it.empty_a = true;
                            continue;
                        },
                    };
                    return it.iterator_a.pop();
                }

                const key_a = it.iterator_a.peek() catch |error_a| switch (error_a) {
                    error.Drained => return null,
                    error.Empty => {
                        it.empty_a = true;
                        continue;
                    },
                };
                const key_b = it.iterator_b.peek() catch |error_b| switch (error_b) {
                    error.Drained => return null,
                    error.Empty => {
                        it.empty_b = true;
                        continue;
                    },
                };

                switch (Table.compare_keys(key_a, key_b)) {
                    .lt => return it.iterator_a.pop(),
                    .gt => {
                        const value_b = it.iterator_b.pop();
                        assert(it.iterator_b_may_contain_tombstones or !Table.tombstone(&value_b));
                        return value_b;
                    },
                    .eq => {
                        const value_a = it.iterator_a.pop();
                        const value_b = it.iterator_b.pop();
                        assert(it.iterator_b_may_contain_tombstones or !Table.tombstone(&value_b));
                        switch (Table.usage) {
                            .general => return value_a,
                            .secondary_index => {
                                // In secondary indexes, puts and removes alternate and
                                // can be safely cancelled out.
                                assert(Table.tombstone(&value_a) != Table.tombstone(&value_b));
                                continue;
                            },
                        }
                    },
                }
            }
        }
    };
}
