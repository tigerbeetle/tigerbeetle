const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");

pub fn ForestType(comptime grove_fields_config: anytype) type {
    // TODO: expose all grove fields in the config while also adding decls somehow
}