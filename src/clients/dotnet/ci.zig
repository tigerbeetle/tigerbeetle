const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("TigerBeetle.sln"));

    try shell.zig("build clients:dotnet -Drelease -Dconfig=production", .{});
    try shell.zig("build -Drelease -Dconfig=production", .{});

    try shell.exec("dotnet format --verify-no-changes", .{});

    // Unit tests.
    try shell.exec("dotnet build --configuration Release", .{});
    // Disable coverage on CI, as it is flaky, see
    // <https://github.com/coverlet-coverage/coverlet/issues/865>
    try shell.exec(
        \\dotnet test
        \\    /p:CollectCoverage=false
        \\    /p:Threshold={threshold}
        \\    /p:ThresholdType={threshold_type}
    , .{
        .threshold = "\"95,85,95\"", // sic, coverlet wants quotes inside the argument
        .threshold_type = "\"line,branch,method\"",
    });

    // Integration tests.
    inline for (.{ "basic", "two-phase", "two-phase-many", "walkthrough" }) |sample| {
        log.info("testing sample '{s}'", .{sample});

        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec("dotnet run", .{});
    }

    // Container smoke tests.
    if (builtin.target.os.tag == .linux) {
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
            "8.0", "8.0-alpine",
        };

        inline for (image_tags) |image_tag| {
            const image = "mcr.microsoft.com/dotnet/sdk:" ++ image_tag;
            log.info("testing docker image: '{s}'", .{image});

            for (0..5) |attempt| {
                if (attempt > 0) std.time.sleep(1 * std.time.ns_per_min);
                if (shell.exec("docker image pull {image}", .{ .image = image })) {
                    break;
                } else |_| {}
            }

            try shell.exec(
                \\docker run
                \\--security-opt seccomp=unconfined
                \\--volume ./TigerBeetle/bin/Release:/host
                \\{image}
                \\sh
                \\-c {script}
            , .{
                .image = image,
                .script =
                \\set -ex
                \\mkdir test-project && cd test-project
                \\dotnet nuget add source /host
                \\dotnet new console
                \\dotnet add package tigerbeetle --source /host > /dev/null
                \\cat <<EOF > Program.cs
                \\using System;
                \\using TigerBeetle;
                \\public class Program {
                \\  public static void Main() {
                \\    new Client(UInt128.Zero, new [] {"3001"}).Dispose();
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

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);
    errdefer tmp_beetle.log_stderr();

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());

    try shell.exec("dotnet new console", .{});
    try shell.exec("dotnet add package tigerbeetle --version {version}", .{
        .version = options.version,
    });

    try Shell.copy_path(
        shell.project_root,
        "src/clients/dotnet/samples/basic/Program.cs",
        shell.cwd,
        "Program.cs",
    );
    try shell.exec("dotnet run", .{});
}
