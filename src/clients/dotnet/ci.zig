const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("TigerBeetle.sln"));

    try shell.exec_zig("build clients:dotnet -Drelease", .{});
    try shell.exec_zig("build -Drelease", .{});

    try shell.exec("dotnet restore", .{});
    try shell.exec("dotnet format --no-restore --verify-no-changes", .{});

    // Unit tests.
    try shell.exec("dotnet build --no-restore  --configuration Release", .{});
    // Disable coverage on CI, as it is flaky, see
    // <https://github.com/coverlet-coverage/coverlet/issues/865>
    try shell.exec(
        \\dotnet test --no-restore
        \\    --logger:{logger}
        \\    /p:CollectCoverage=false
        \\    /p:Threshold={threshold}
        \\    /p:ThresholdType={threshold_type}
    , .{
        // Dotnet wants quotes inside the argument.
        .logger = "\"console;verbosity=detailed\"",
        .threshold = "\"95,85,95\"",
        .threshold_type = "\"line,branch,method\"",
    });

    // Integration tests.
    inline for (.{ "basic", "two-phase", "two-phase-many", "walkthrough" }) |sample| {
        log.info("testing sample '{s}'", .{sample});

        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
            .development = true,
        });
        defer tmp_beetle.deinit(gpa);
        errdefer tmp_beetle.log_stderr();

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
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
        .development = true,
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);
    errdefer tmp_beetle.log_stderr();

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);
    try shell.exec("dotnet new console", .{});

    // NuGet may take a few minutes to make the new package available for download.
    for (0..9) |_| {
        if (try nuget_install(shell, .{ .version = options.version }) == .ok) break;
        log.warn("waiting for 5 minutes for the {s} version to appear in nuget.org", .{
            options.version,
        });
        std.time.sleep(5 * std.time.ns_per_min);
    } else {
        switch (try nuget_install(shell, .{ .version = options.version })) {
            .ok => {},
            .retry => |err| {
                log.err("package is not available in nuget.org", .{});
                return err;
            },
        }
    }

    try Shell.copy_path(
        shell.project_root,
        "src/clients/dotnet/samples/basic/Program.cs",
        shell.cwd,
        "Program.cs",
    );
    try shell.exec("dotnet run", .{});
}

fn nuget_install(shell: *Shell, options: struct {
    version: []const u8,
}) !union(enum) { ok, retry: anyerror } {
    const command: []const u8 = "dotnet add package tigerbeetle --version {version}";
    if (shell.exec(command, options)) {
        return .ok;
    } else |err| {
        const exec_result = try shell.exec_raw(command, options);
        switch (exec_result.term) {
            .Exited => |code| if (code == 0) return .ok,
            else => {},
        }

        // Error message:
        // NU1102: Unable to find package tigerbeetle with version (>= {version}).
        const package_missing = std.mem.indexOf(
            u8,
            exec_result.stdout,
            "NU1102",
        ) != null;
        if (package_missing) return .{ .retry = err };
        return err;
    }
}

pub fn release_published_latest(shell: *Shell) ![]const u8 {
    const DotnetSearch = struct {
        const SearchResult = struct {
            const Packages = struct {
                id: []const u8,
                latestVersion: []const u8,
            };
            packages: []Packages,
        };
        searchResult: []SearchResult,
    };

    const output = try shell.exec_stdout("dotnet package search tigerbeetle --format json", .{});
    const dotnet_search_results = try std.json.parseFromSliceLeaky(
        DotnetSearch,
        shell.arena.allocator(),
        output,
        .{ .ignore_unknown_fields = true },
    );

    assert(dotnet_search_results.searchResult.len == 1);
    assert(dotnet_search_results.searchResult[0].packages.len == 1);

    assert(std.mem.eql(u8, dotnet_search_results.searchResult[0].packages[0].id, "tigerbeetle"));

    return dotnet_search_results.searchResult[0].packages[0].latestVersion;
}
