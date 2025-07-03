//! Entry point for exporting the `tb_client` library.
//! Used by language clients that rely on the shared or static library exposed by `tb_client.h`.
//! For an idiomatic Zig API, use `vsr.tb_client` directly instead.
const builtin = @import("builtin");
const std = @import("std");

pub const vsr = @import("vsr");
const exports = vsr.tb_client.exports;

pub const std_options: std.Options = .{
    .log_level = .debug,
    .logFn = exports.Logging.application_logger,
};

comptime {
    if (!builtin.link_libc) {
        @compileError("Must be built with libc to export tb_client symbols.");
    }

    @export(&exports.init, .{ .name = "tb_client_init", .linkage = .strong });
    @export(&exports.init_echo, .{ .name = "tb_client_init_echo", .linkage = .strong });
    @export(&exports.submit, .{ .name = "tb_client_submit", .linkage = .strong });
    @export(&exports.deinit, .{ .name = "tb_client_deinit", .linkage = .strong });
    @export(
        &exports.completion_context,
        .{ .name = "tb_client_completion_context", .linkage = .strong },
    );
    @export(
        &exports.register_log_callback,
        .{ .name = "tb_client_register_log_callback", .linkage = .strong },
    );
    @export(
        &exports.init_parameters,
        .{ .name = "tb_client_init_parameters", .linkage = .strong },
    );
}
