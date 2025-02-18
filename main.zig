const std = @import("std");


pub fn caller(errors: *Errors) void {
    helper(errors) catch |err| switch (err) {
        error.FileNotFound => {
            const path = errors.payload(error.FileNotFound, []const u8);
        },
    };
}

pub fn helper(errors: *Errors) error{FileNotFound}!void {
    return errors.throw(error.FileNotFound, "my-file.zig");
}

const Errors = struct {
    var error_stack = []u8;
    pub fn throw(self: *@This(), comptime err: anytype, comptime payload_value: anytype) @TypeOf(err) {
        comptime register(err, @TypeOf(payload_value));
        // push palyload_value to error_stack;
    }

    pub fn payload(self: *@This(), comptime err: anytype, comptime PayloadType: type) PayloadType {
        comptime register(err, PayloadType);
        // pop @sizeOf(PayloadType) bytes off the stack
    }
};

fn register(comptime err: anytype, comptime PayloadTyper: u32) void {
    // Side-effect somehow to check for consistency:
    // - @export a symbol to a custom section
    // - or emit a `test` that does something at runtime during test
    // - or @compileLog under a flag and parse compiler output.
}
