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

    pub const Requirement = stdx.EnumType(api_decl_names_matching(is_requirement));
};

pub const Step = union(enum) {
    binding: Binding,
    call: Call,
    assertion: Assertion,
    directive: HarnessDirective,
    create_client: ClientInitArgs,
};

pub const Binding = struct {
    name: []const u8,
    value: Expression,
};

// `generate_ids` sleeps a millisecond every `ids_between_sleeps` ids, so
// that a batch spans several clock ticks rather than a single one.
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
    receiver: []const u8,
    name: Name,
    arguments: []const Expression = &.{},

    pub const Name = std.meta.DeclEnum(api.Client);
};

pub const ClientInitArgs = struct {
    cluster_id: []const u8,
    replica_addresses: ReplicaAddresses,

    pub const ReplicaAddresses = union(enum) {
        server,
        literal: []const u8,
    };
};

pub const HarnessDirective = union(enum) {
    sleep_ms: u32,
    concurrently: struct {
        n: u32,
        call: Call,
    },
};

pub const Assertion = union(enum) {
    equal: struct {
        actual: []const u8,
        expected: []const Record,
    },
    equal_field: FieldComparison,
    empty: []const u8,
    id_ascending: []const u8,
    greater_than: FieldComparison,
    fail: struct {
        action: Action,
        client_error: ?ClientError,
    },

    pub const Name = stdx.EnumType(api_decl_names_matching(is_assertion));

    pub const Action = union(enum) {
        call: Call,
        create_client: ClientInitArgs,
    };

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

    pub const Type = stdx.EnumType(api_decl_names_matching(is_record));

    // Returns the API struct type for a `Record.Type`.
    pub fn Struct(comptime record_type: Type) type {
        return @field(api, @tagName(record_type));
    }

    // NOTE: This assumes that all packed structs are flags.
    pub fn is_flags(record_type: Type) bool {
        switch (record_type) {
            inline else => |comptime_type| {
                return @typeInfo(Struct(comptime_type)).@"struct".layout == .@"packed";
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

// Returns the unqualified type name: a.b.SomeEnum -> SomeEnum.
fn type_name(comptime T: type) []const u8 {
    const name = @typeName(T);
    const index = std.mem.lastIndexOfScalar(u8, name, '.').?;
    return name[index + 1 ..];
}

fn api_decl_names_matching(
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

fn is_assertion(comptime name: [:0]const u8) bool {
    return std.mem.startsWith(u8, name, "assert_");
}

fn is_requirement(comptime name: [:0]const u8) bool {
    return std.mem.startsWith(u8, name, "requires_");
}

fn is_record(comptime name: [:0]const u8) bool {
    if (std.mem.eql(u8, name, "ClientError")) return false;
    if (std.mem.eql(u8, name, "Client")) return false;
    return @TypeOf(@field(api, name)) == type;
}
