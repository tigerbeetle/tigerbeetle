const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    // TODO: we should check formatting here, but that's not trivial to enable because
    // `dotnet format` gives different results on Windows and Unix.
    // try shell.exec("dotnet format --verify-no-changes", .{});

    // Unit tests.
    try shell.exec("dotnet build --configuration Release", .{});
    // Disable coverage on CI, as it is flaky, see
    // <https://github.com/coverlet-coverage/coverlet/issues/865>
    try shell.exec(
        \\dotnet test
        \\    /p:CollectCoverage=false
        \\    /p:Threshold="95,85,95"
        \\    /p:ThresholdType="line,branch,method"
    , .{});

    // Integration tests.
    inline for (.{ "basic", "two-phase", "two-phase-many" }) |sample| {
        var sample_dir = try shell.project_root.openDir(
            "src/clients/dotnet/samples/" ++ sample,
            .{},
        );
        defer sample_dir.close();

        try sample_dir.setAsCwd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
        defer tmp_beetle.deinit(gpa);

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec("dotnet run", .{});
    }

    // Container smoke tests.
    if (builtin.target.os.tag == .linux) {
        var client_dir = try shell.project_root.openDir("src/clients/dotnet/", .{});
        defer client_dir.close();

        try client_dir.setAsCwd();

        // Here, we want to check that our package does not break horrible on upstream containers
        // due to missing runtime dependencies, mismatched glibc ABI and similar issues.
        //
        // We don't necessary want to be able to _build_ code inside such a container, we only
        // need to check that pre-built code runs successfully. So, build a package on host,
        // mount it inside the container and smoke test.
        //
        // We run an sh script inside a container, because it is trivial. If it grows larger,
        // we should consider running a proper zig program inside.
        try shell.exec("dotnet pack --configuration Release", .{});

        const image_tags = .{
            "7.0",        "6.0",        "3.1",
            "7.0-alpine", "6.0-alpine", "3.1-alpine",
        };

        inline for (image_tags) |image_tag| {
            try shell.exec(
                \\docker run
                \\--security-opt seccomp=unconfined
                \\--volume ./TigerBeetle/bin/Release:/nuget
                \\{image}
                \\sh
                \\-c {script}
            , .{
                .image = "mcr.microsoft.com/dotnet/sdk:" ++ image_tag,
                .script =
                \\set -ex
                \\mkdir test-project && cd test-project
                \\dotnet nuget add source /nuget
                \\dotnet new console
                \\dotnet add package tigerbeetle --source /nuget > /dev/null
                \\cat <<EOF > Program.cs
                \\using System;
                \\public class Program {
                \\  public static void Main() {
                \\    new TigerBeetle.Client(0, new [] {"3001"}).Dispose();
                \\    Console.WriteLine("SUCCESS");
                \\  }
                \\}
                \\EOF
                \\dotnet run
                ,
            });
        }
    }
}

pub fn verify_release(shell: *Shell, gpa: std.mem.Allocator, tmp_dir: std.fs.Dir) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
    defer tmp_beetle.deinit(gpa);

    try shell.exec("dotnet new console", .{});
    try shell.exec("dotnet add package tigerbeetle", .{});

    try Shell.copy_path(
        shell.project_root,
        "src/clients/dotnet/samples/basic/Program.cs",
        tmp_dir,
        "Program.cs",
    );
    try shell.exec("dotnet run", .{});
}
