const std = @import("std");
const stdx = @import("stdx");
const api = @import("conformance_test_api.zig");

pub const ClientError = api.ClientError;

pub const Suite = struct {
    name: []const u8,
    cases: []const Case,
};

pub const Case = struct {
    description: []const u8,
    steps: []const Step,
    requirement: ?Requirement = null,

    pub const Requirement = stdx.EnumType(api_decl_names(is_requirement));
};

pub const Step = union(enum) {
    binding: Binding,
    call: Call,
    assertion: Assertion,
};

pub const Binding = struct {
    name: []const u8,
    value: Expression,
};

/// `generate_ids` sleeps a millisecond every `ids_between_sleeps` ids, so that a batch spans
/// several clock ticks rather than a single one.
pub const ids_between_sleeps = 10_000;

pub const Expression = union(enum) {
    generate_id,
    generate_ids: u32,
    call: Call,
    record: Record,
    integer: []const u8,
    boolean: bool,
    enum_literal: []const u8,
    reference: []const u8,
    index: struct {
        reference: []const u8,
        index: u32,
        record_type: Record.Type,
    },
    field_access: FieldReference,
    increment: Arithmetic,
    decrement: Arithmetic,

    pub const Arithmetic = struct {
        reference: []const u8,
        by: u64,
    };
};

pub const FieldReference = struct {
    reference: []const u8,
    field: Field,
};

pub const Call = struct {
    name: Name,
    arguments: []const Expression = &.{},
    concurrency: u32 = 1,

    // NOTE: we're trying to keep the operations in sync with what is defined in
    // conformance_test_api.zig. I will reevaluate this approach later, for now
    // it's GoodEnough™.
    pub const Name = stdx.EnumType(api_decl_names(is_call));
};

pub const Assertion = union(enum) {
    equal: struct {
        actual: []const u8,
        expected: []const Record,
    },
    equal_field: FieldComparison,
    empty: []const u8,
    ascending: []const u8,
    greater_than: FieldComparison,
    fail: struct {
        call: Call,
        client_error: ?ClientError,
    },

    pub const FieldComparison = struct {
        actual: FieldReference,
        expected: union(enum) {
            expression: Expression,
            field_reference: FieldReference,
        },
    };
};

// Parsed form of the anonymous struct literals used in tests.
pub const Record = struct {
    type: Type,
    fields: []const FieldValue,

    pub const Type = stdx.EnumType(api_decl_names(is_record));

    // NOTE: should we generate this so it won't get out of sync?
    const flags_records = .{
        api.AccountFlags, api.AccountFilterFlags, api.QueryFilterFlags, api.TransferFlags,
    };

    pub fn Struct(comptime record_type: Type) type {
        return @field(api, @tagName(record_type));
    }

    pub fn is_flags(record_type: Type) bool {
        switch (record_type) {
            inline else => |comptime_type| {
                inline for (flags_records) |Flags| if (Struct(comptime_type) == Flags) return true;
                return false;
            },
        }
    }
};

pub const FieldValue = struct {
    field: Field,
    value: Expression,
};

pub const Field = struct {
    name: []const u8,
    type: Type,

    pub const Type = union(enum) {
        int: u16,
        enum_name: []const u8,
        boolean,
        flags,

        pub fn resolve(record_type: Record.Type, field_name: []const u8) Type {
            switch (record_type) {
                inline else => |comptime_type| {
                    inline for (std.meta.fields(Record.Struct(comptime_type))) |field| {
                        if (std.mem.eql(u8, field.name, field_name)) {
                            return switch (@typeInfo(field.type)) {
                                .int => |info| .{ .int = info.bits },
                                .@"enum" => .{ .enum_name = type_name(field.type) },
                                .bool => .boolean,
                                .@"struct" => .flags,
                                else => @panic("unsupported field type"),
                            };
                        }
                    }
                    unreachable;
                },
            }
        }
    };
};

fn type_name(comptime T: type) []const u8 {
    const name = @typeName(T);
    const index = std.mem.lastIndexOfScalar(u8, name, '.').?;
    return name[index + 1 ..];
}

pub fn api_decl_names(
    comptime filter: fn (comptime name: [:0]const u8) bool,
) []const [:0]const u8 {
    const decls = @typeInfo(api).@"struct".decls;
    var names: [decls.len][:0]const u8 = undefined;
    var count = 0;
    for (decls) |decl| {
        if (!filter(decl.name)) continue;
        names[count] = decl.name;
        count += 1;
    }
    return stdx.comptime_slice(names, count);
}

fn is_call(comptime name: [:0]const u8) bool {
    if (std.mem.eql(u8, name, "close_client")) return true;
    if (std.mem.eql(u8, name, "sleep_ms")) return true;
    for ([_][]const u8{ "create_", "lookup_", "get_", "query_" }) |prefix| {
        if (std.mem.startsWith(u8, name, prefix)) return true;
    }
    return false;
}

pub fn is_assertion(comptime name: [:0]const u8) bool {
    return std.mem.startsWith(u8, name, "assert_");
}

fn is_requirement(comptime name: [:0]const u8) bool {
    return std.mem.startsWith(u8, name, "requires_");
}

fn is_record(comptime name: [:0]const u8) bool {
    if (std.mem.eql(u8, name, "ClientError")) return false;
    return @TypeOf(@field(api, name)) == type;
}
