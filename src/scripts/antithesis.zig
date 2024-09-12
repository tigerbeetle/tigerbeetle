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
    push: bool = false,
};

const Image = enum { config, workload, replica };

pub fn main(shell: *Shell, cli_args: CLIArgs) !void {
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

    const images = comptime std.enums.values(Image);
    inline for (images) |image| {
        try build_image(shell, image, tag);
        if (cli_args.push) {
            try push_image(shell, image, tag);
        }
    }
}

fn build_image(shell: *Shell, comptime image: Image, tag: []const u8) !void {
    try shell.exec_options(.{ .echo = true, .stdin_slice = @field(dockerfiles, @tagName(image)) },
        \\docker build 
        \\  --file - .
        \\  --build-arg TAG={tag}
        \\  --tag={image}:{tag}
    , .{ .image = @tagName(image), .tag = tag });
}

fn push_image(shell: *Shell, image: Image, tag: []const u8) !void {
    const url_prefix = "us-central1-docker.pkg.dev/molten-verve-216720/tigerbeetle-repository";
    try shell.exec(
        "docker tag {image}:{tag} {url_prefix}/{image}:{tag}",
        .{ .image = @tagName(image), .tag = tag, .url_prefix = url_prefix },
    );
    try shell.exec(
        "docker push {url_prefix}/{image}:{tag}",
        .{ .image = @tagName(image), .tag = tag, .url_prefix = url_prefix },
    );
}

const dockerfiles = .{
    .config =
    \\FROM debian:stable-slim
    \\
    \\ARG TAG
    \\
    \\RUN mkdir -p /volumes/database
    \\RUN echo "TAG=${TAG}" > /.env
    \\
    \\FROM scratch
    \\
    \\COPY src/testing/systest/docker-compose.yaml docker-compose.yaml
    \\COPY --from=0 /.env .env
    \\COPY --from=0 /volumes/database /volumes/database
    ,
    .workload =
    \\FROM debian:stable-slim
    \\WORKDIR /opt/tigerbeetle
    \\
    \\ENV DEBIAN_FRONTEND=noninteractive
    \\
    \\RUN apt update && apt install -y --no-install-recommends wget ca-certificates
    \\
    \\RUN wget https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.deb \
    \\      && dpkg -i jdk-21_linux-x64_bin.deb \
    \\      && rm jdk-21_linux-x64_bin.deb
    \\
    \\COPY src/clients/java/target/tigerbeetle-java-0.0.1-SNAPSHOT.jar \
    \\      ./tigerbeetle-java.jar
    \\COPY src/testing/systest/workload/target/workload-0.0.1-SNAPSHOT.jar \
    \\      ./workload.jar
    \\
    \\ENTRYPOINT ["java", "-ea", "-cp", "workload.jar:tigerbeetle-java.jar", "Main"]
    ,

    .replica =
    \\FROM debian:stable-slim
    \\WORKDIR /opt/tigerbeetle
    \\
    \\COPY zig-out/bin/tigerbeetle ./tigerbeetle
    \\COPY src/testing/systest/scripts/run.sh ./run.sh
    \\
    \\ENTRYPOINT ["./run.sh"]
    ,
};
