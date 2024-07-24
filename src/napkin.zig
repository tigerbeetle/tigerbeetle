//! Napkin math module --- prints the key constants and measurements of TigerBeetle, for example
//! the layout of the journal or the total size, in bytes, of a single lsm_batch_multiple of
//! prepares. This is the map of the physics of the database. Best accompanied by
//! <https://github.com/sirupsen/napkin-math>
//!
//! It is primarily accessed as `tigerbeetle benchmark --napkin`, but you can also use it as a
//! library from unit tests or vopr.

const std = @import("std");
const vsr = @import("./vsr.zig");
const constants = @import("./constants.zig");

const AnyWriter = std.io.AnyWriter;

pub fn print_napkin_math_to_stdout() !void {
    const stdout = std.io.getStdOut();
    const stdout_writer = stdout.writer();
    try print_napkin_math(stdout_writer.any());
}

pub fn print_napkin_math(out: AnyWriter) !void {
    try napkin_journal(out);
    try napkin_state_machine(out);
}

fn napkin_journal(out: std.io.AnyWriter) !void {
    try print_section(out, "Journal");
    try print_value(out, "journal_slot_count", constants.journal_slot_count);
    try print_value(out, "checkpoint_interval", constants.vsr_checkpoint_interval);
    try print_value(out, "lsm_batch_multiple", constants.lsm_batch_multiple);
    try print_value(out, "pipeline_prepare_queue_max", constants.pipeline_prepare_queue_max);
}

fn napkin_state_machine(out: std.io.AnyWriter) !void {
    try print_section(out, "State Machine");

    try print_value_size(
        out,
        "prepare_body_size",
        constants.message_body_size_max,
    );
    try print_value_size(
        out,
        "prepare_body_size_batch",
        constants.message_body_size_max * constants.lsm_batch_multiple,
    );

    try print_value(out, "transfers_max", @divExact(
        constants.message_body_size_max,
        @sizeOf(vsr.tigerbeetle.Transfer),
    ));
    try print_value(out, "accounts_max", @divExact(
        constants.message_body_size_max,
        @sizeOf(vsr.tigerbeetle.Account),
    ));

    try print_value(out, "transfers_max_batch", @divExact(
        constants.message_body_size_max * constants.lsm_batch_multiple,
        @sizeOf(vsr.tigerbeetle.Transfer),
    ));
    try print_value(out, "accounts_max_batch", @divExact(
        constants.message_body_size_max * constants.lsm_batch_multiple,
        @sizeOf(vsr.tigerbeetle.Account),
    ));
}

fn print_section(out: std.io.AnyWriter, section_title: []const u8) !void {
    try out.print("\n# {s}\n", .{section_title});
}

fn print_value(out: std.io.AnyWriter, name: []const u8, value: u64) !void {
    try out.print("{s:<32} = {}\n", .{ name, value });
}

fn print_value_size(out: std.io.AnyWriter, name: []const u8, value: u64) !void {
    if (value >= 1024 * 1024) {
        try out.print("{s:<32} = {} bytes (~{} MiB)\n", .{ name, value, @divFloor(value, 1024 * 1024) });
    } else if (value >= 1024) {
        try out.print("{s:<32} = {} bytes (~{} KiB)\n", .{ name, value, @divFloor(value, 1024) });
    } else {
        try out.print("{s:<32} = {} bytes\n", .{ name, value });
    }
}
