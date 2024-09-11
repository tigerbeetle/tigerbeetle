//! Deployment script for our systest (src/testing/systest).

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");

pub const CLIArgs = struct {
    tag: []const u8,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    // if (builtin.os.tag == .windows) {
    //     return error.NotSupported;
    // }
    //
    _ = gpa;

    assert(try shell.exec_status_ok("docker --version", .{}));

    // Docker tag to build and push
    const tag = std.mem.trim(u8, cli_args.tag, &std.ascii.whitespace);
    assert(tag.len > 0);

    try shell.zig("build -Drelease", .{});

    // Build Java client library
    {
        try shell.pushd("./src/clients/java");
        defer shell.popd();

        try shell.exec("mvn clean install --batch-mode --quiet -Dmaven.test.skip", .{});
    }

    // Build workload
    {
        try shell.pushd("./src/testing/systest/workload");
        defer shell.popd();

        try shell.exec("mvn clean package --batch-mode --quiet", .{});
    }

    try build_images(shell, tag);
}

fn build_images(shell: *Shell, tag: []const u8) !void {
    try shell.exec_options(.{
        .echo = true,
        .stdin_slice =
        \\FROM debian:stable-slim
        \\
        \\ARG TAG
        \\
        \\RUN mkdir -p /volumes/database
        \\RUN echo "TAG=${TAG}" > /.env
        \\
        \\FROM scratch
        \\
        \\COPY src/testing/systest/config/docker-compose.yaml docker-compose.yaml
        \\COPY --from=0 /.env .env
        \\COPY --from=0 /volumes/database /volumes/database
        ,
    },
        \\docker build 
        \\  --file - .
        \\  --build-arg TAG={tag}
        \\  --tag=config:{tag}
    , .{ .tag = tag });

    try shell.exec_options(.{
        .echo = true,
        .stdin_slice =
        \\FROM debian:stable-slim
        \\WORKDIR /opt/tigerbeetle
        \\
        \\COPY zig-out/bin/tigerbeetle ./tigerbeetle
        \\COPY src/testing/systest/scripts/run-tigerbeetle.sh ./run-tigerbeetle.sh
        \\
        \\ENTRYPOINT ["./run-tigerbeetle.sh"]
        ,
    },
        \\docker build 
        \\  --file - .
        \\  --tag=replica:{tag}
    , .{ .tag = tag });

    try shell.exec_options(.{
        .echo = true,
        .stdin_slice =
        \\FROM debian:stable-slim
        \\WORKDIR /opt/tigerbeetle
        \\
        \\ENV DEBIAN_FRONTEND=noninteractive
        \\
        \\RUN apt update && apt install -y --no-install-recommends wget ca-certificates
        \\
        \\RUN wget https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.deb && dpkg -i jdk-21_linux-x64_bin.deb && rm jdk-21_linux-x64_bin.deb
        \\
        \\COPY src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar ./tigerbeetle-java-0.0.1-SNAPSHOT.jar
        \\COPY src/testing/systest/workload/target/workload-0.0.1-SNAPSHOT.jar ./workload-0.0.1-SNAPSHOT.jar
        \\
        \\ENTRYPOINT ["java", "-ea", "-cp", "workload-0.0.1-SNAPSHOT.jar:tigerbeetle-java-0.0.1-SNAPSHOT.jar", "Main"]
        ,
    },
        \\docker build 
        \\  --file - .
        \\  --tag=workload:{tag}
    , .{ .tag = tag });
}
