const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("package.json"));

    // We have some unit-tests for node, but they are likely bitrotted, as they are not run on CI.

    // Integration tests.

    // We need to build the tigerbeetle-node library manually for samples to be able to pick it up.
    try shell.exec("npm install", .{});
    try shell.exec("npm pack --quiet", .{});

    inline for (.{ "basic", "two-phase", "two-phase-many" }) |sample| {
        try shell.pushd("./samples/" ++ sample);
        defer shell.popd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
        defer tmp_beetle.deinit(gpa);

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec("npm install", .{});
        try shell.exec("node main.js", .{});
    }

    // Container smoke tests.
    if (builtin.target.os.tag == .linux) {

        // Installing node through <https://github.com/nodesource/distributions>.

        const deb_install_script =
            \\apt-get update
            \\apt-get install -y ca-certificates curl gnupg
            \\mkdir -p /etc/apt/keyrings
            \\
            \\curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
            \\  | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
            \\
            \\echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] \
            \\    https://deb.nodesource.com/node_18.x nodistro main" \
            \\  | tee /etc/apt/sources.list.d/nodesource.list
            \\
            \\apt-get update
            \\apt-get install nodejs -y
        ;

        const rpm_install_script =
            \\yum install \
            \\  https://rpm.nodesource.com/pub_16.x/nodistro/repo/nodesource-release-nodistro-1.noarch.rpm -y
            \\yum install nodejs -y --setopt=nodesource-nodejs.module_hotfixes=1
        ;

        const distributions = .{
            "alpine", "debian",      "ubuntu",
            "fedora", "redhat/ubi9", "amazonlinux:2.0.20230307.0",
        };

        const install_scripts = .{
            \\apk add --update nodejs npm
            ,
            deb_install_script,
            deb_install_script,
            rpm_install_script,
            "update-crypto-policies --set DEFAULT:SHA1\n" ++ rpm_install_script,
            rpm_install_script,
        };

        inline for (distributions, install_scripts) |distribution, install_script| {
            try shell.exec(
                \\docker run
                \\--security-opt seccomp=unconfined
                \\--volume ./:/host
                \\{image}
                \\sh
                \\-c {script}
            , .{
                .image = distribution,
                .script = 
                \\set -ex
                \\mkdir test-project && cd test-project
                ++ "\n" ++ install_script ++ "\n" ++
                    \\npm install /host/tigerbeetle-node-*.tgz
                    \\node -e 'require("tigerbeetle-node"); console.log("SUCCESS!")'
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

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());

    try shell.exec("npm install tigerbeetle-node@{version}", .{
        .version = options.version,
    });

    try Shell.copy_path(
        shell.project_root,
        "src/clients/node/samples/basic/main.js",
        shell.cwd,
        "main.js",
    );
    try shell.exec("node main.js", .{});
}
