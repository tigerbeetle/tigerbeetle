const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("pom.xml"));

    // Java's maven doesn't support a separate test command, or a way to add dependency on a
    // project (as opposed to a compiled jar file).
    //
    // For this reason, we do all our testing in one go, imperatively building a client jar and
    // installing it into the local maven repository.
    try shell.exec("mvn --batch-mode --file pom.xml --quiet formatter:validate", .{});
    try shell.exec("mvn --batch-mode --file pom.xml --quiet install", .{});

    inline for (.{ "basic", "two-phase", "two-phase-many" }) |sample| {
        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
        defer tmp_beetle.deinit(gpa);

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec(
            \\mvn --batch-mode --file pom.xml --quiet
            \\  package exec:java
        , .{});
    }
}

pub fn verify_release(shell: *Shell, gpa: std.mem.Allocator, tmp_dir: std.fs.Dir) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
    defer tmp_beetle.deinit(gpa);

    // TODO
    _ = shell;
    _ = tmp_dir;
}
