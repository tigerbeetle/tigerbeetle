const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const config = @import("../config.zig");
const div_ceil = @import("../util.zig").div_ceil;
const binary_search = @import("binary_search.zig");
const snapshot_latest = @import("tree.zig").snapshot_latest;

pub fn TableImmutableType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableImmutable = @This();

        value_count_max: u32,
        values: []Value,
        snapshot_min: u64,
        free: bool,

        /// `commit_entries_max` is the maximum number of Values that can be inserted by a single commit.
        pub fn init(allocator: mem.Allocator, commit_entries_max: u32) !TableImmutable {
            assert(commit_entries_max > 0);

            // The in-memory immutable table is the same size as the mutable table:
            const value_count_max = commit_entries_max * config.lsm_batch_multiple;
            const data_block_count = div_ceil(value_count_max, Table.data.value_count_max);
            assert(data_block_count <= Table.data_block_count_max);

            const values = try allocator.alloc(Value, value_count_max);
            errdefer allocator.free(values);

            return TableImmutable{
                .value_count_max = value_count_max,
                .snapshot_min = undefined,
                .values = values,
                .free = true,
            };
        }

        pub inline fn values_max(table: *const TableImmutable) []Value {
            assert(table.values.len <= table.value_count_max);
            return table.values.ptr[0..table.value_count_max];
        }

        pub inline fn key_min(table: *const TableImmutable) Key {
            assert(!table.free);
            assert(table.values.len > 0);
            return key_from_value(&table.values[0]);
        }

        pub inline fn key_max(table: *const TableImmutable) Key {
            assert(!table.free);
            assert(table.values.len > 0);
            return key_from_value(&table.values[table.values.len - 1]);
        }

        pub fn deinit(table: *TableImmutable, allocator: mem.Allocator) void {
            allocator.free(table.values_max());
        }

        pub fn clear(table: *TableImmutable) void {
            // This hack works around the stage1 compiler's problematic handling of pointers to
            // zero-bit types. In particular, `slice = slice[0..0]` is not equivalent to
            // `slice.len = 0` but in fact sets `slice.ptr = undefined` as well. This happens
            // since the type of `slice[0..0]` is `*[0]Value` which is a pointer to a zero-bit
            // type. Using slice bounds that are not comptime known avoids the issue.
            // See: https://github.com/ziglang/zig/issues/6706
            // TODO(zig) Remove this hack when upgrading to 0.10.0.
            var runtime_zero: usize = 0;

            table.* = .{
                .value_count_max = table.value_count_max,
                .snapshot_min = undefined,
                .values = table.values[runtime_zero..runtime_zero],
                .free = true,
            };
        }

        pub fn reset_with_sorted_values(
            table: *TableImmutable,
            snapshot_min: u64,
            sorted_values: []const Value,
        ) void {
            assert(table.free);
            assert(snapshot_min > 0);
            assert(snapshot_min < snapshot_latest);

            assert(sorted_values.ptr == table.values.ptr);
            assert(sorted_values.len > 0);
            assert(sorted_values.len <= table.value_count_max);
            assert(sorted_values.len <= Table.data.value_count_max * Table.data_block_count_max);

            if (config.verify) {
                var i: usize = 1;
                while (i < sorted_values.len) : (i += 1) {
                    assert(i > 0);
                    const left_key = key_from_value(&sorted_values[i - 1]);
                    const right_key = key_from_value(&sorted_values[i]);
                    assert(compare_keys(left_key, right_key) == .lt);
                }
            }

            table.* = .{
                .value_count_max = table.value_count_max,
                .values = table.values.ptr[0..sorted_values.len],
                .snapshot_min = snapshot_min,
                .free = false,
            };
        }

        // TODO(ifreund) This would be great to unit test.
        pub fn get(table: *const TableImmutable, key: Key) ?*const Value {
            assert(!table.free);

            const result = binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                compare_keys,
                table.values,
                key,
                .{},
            );
            if (result.exact) {
                const value = &table.values[result.index];
                if (config.verify) assert(compare_keys(key, key_from_value(value)) == .eq);
                return value;
            }

            return null;
        }
    };
}

pub fn TableImmutableIteratorType(comptime Table: type, comptime Storage: type) type {
    _ = Storage;

    return struct {
        const TableImmutableIterator = @This();
        const TableImmutable = TableImmutableType(Table);

        table: *const TableImmutable,
        values_index: u32,

        pub fn init(allocator: mem.Allocator) !TableImmutableIterator {
            _ = allocator; // This only iterates an existing immutable table.

            return TableImmutableIterator{
                .table = undefined,
                .values_index = undefined,
            };
        }

        pub fn deinit(it: *TableImmutableIterator, allocator: mem.Allocator) void {
            _ = allocator; // No memory allocation was initially performed.
            it.* = undefined;
        }

        pub const Context = struct {
            table: *const TableImmutable,
        };

        pub fn start(
            it: *TableImmutableIterator,
            context: Context,
            read_done: *const fn (*TableImmutableIterator) void,
        ) void {
            _ = read_done; // No asynchronous operations are performed.
            it.* = .{
                .table = context.table,
                .values_index = 0,
            };
        }

        pub fn tick(it: *const TableImmutableIterator) bool {
            assert(!it.table.free);
            return false; // No I/O is performed as it's all in memory.
        }

        pub fn buffered_all_values(it: *const TableImmutableIterator) bool {
            assert(!it.table.free);
            return true; // All values are "buffered" in memory.
        }

        pub fn peek(it: *const TableImmutableIterator) error{ Empty, Drained }!Table.Key {
            // NOTE: This iterator is never Drained as all values are in memory (tick is a no-op).
            assert(!it.table.free);
            if (it.values_index == it.table.values.len) return error.Empty;
            return Table.key_from_value(&it.table.values[it.values_index]);
        }

        pub fn pop(it: *TableImmutableIterator) Table.Value {
            assert(!it.table.free);
            defer it.values_index += 1;
            return it.table.values[it.values_index];
        }
    };
}
