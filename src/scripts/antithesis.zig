//! Deployment script for our systest (src/testing/systest).
//!
//! * Builds the Java client and the associated workload using Maven
//! * Builds Docker images for the workload, replicas, and config
//! * Optionally pushes the images to the Antithesis registry
//!
//! Currently there's no support for triggering tests with this script.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const flags = @import("../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../shell.zig");

pub const CLIArgs = struct {
    // Docker tag to build (and possibly push)
    tag: []const u8,
    // Whether to push the built tag to the Antithesis registry
    push: bool = false,
};

const Image = enum { config, workload, replica };

pub fn main(shell: *Shell, _: std.mem.Allocator, cli_args: CLIArgs) !void {
    assert(try shell.exec_status_ok("docker --version", .{}));

    assert(cli_args.tag.len > 0);
    assert(std.mem.indexOfAny(u8, cli_args.tag, &std.ascii.whitespace) == null);

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
        try build_image(shell, image, cli_args.tag);
        if (cli_args.push) {
            try push_image(shell, image, cli_args.tag);
        }
    }
}

fn build_image(
    shell: *Shell,
    comptime image: Image,
    tag: []const u8,
) !void {
    switch (image) {
        .config => {
            // This is the temporary directory where we'll assemble the config docker image.
            // By design it's not automatically deleted with a defer, because the caller of
            // the script might want to debug the docker compose setup locally.
            const image_dir = try shell.create_tmp_dir();

            try shell.pushd(image_dir);
            defer shell.popd();

            // TODO(owickstrom): remove the need for .env file by rendering docker-compose.yaml
            // with the correct tag?
            const env_file = ".env";
            shell.echo(
                \\{ansi-red}
                \\ {s}/{s}
                \\{ansi-reset}
            , .{ image_dir, env_file });

            const env_file_contents = try shell.fmt("TAG={s}", .{tag});
            _ = try shell.file_ensure_content(env_file, env_file_contents);

            const docker_compose_file = "docker-compose.yaml";
            _ = try shell.file_ensure_content(docker_compose_file, docker_compose_contents);

            try shell.exec("mkdir -p volumes/database", .{});

            try shell.exec_options(.{
                .echo = true,
                .stdin_slice = @field(dockerfiles, @tagName(image)),
            },
                \\docker build 
                \\  --file - .
                \\  --build-arg TAG={tag}
                \\  --tag={image}:{tag}
            , .{ .image = @tagName(image), .tag = tag });

            shell.echo(
                \\{ansi-red}
                \\To debug the docker compose config locally, run:
                \\
                \\    cd {s} && TAG={s} docker compose up
                \\{ansi-reset}
            , .{ image_dir, tag });
        },
        else => {
            try shell.exec_options(.{
                .echo = true,
                .stdin_slice = @field(dockerfiles, @tagName(image)),
            },
                \\docker build 
                \\  --file - .
                \\  --build-arg TAG={tag}
                \\  --tag={image}:{tag}
            , .{ .image = @tagName(image), .tag = tag });
        },
    }
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
    \\FROM scratch
    \\
    \\ADD docker-compose.yaml docker-compose.yaml
    \\ADD .env .env
    \\ADD volumes/database /volumes/database
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

const docker_compose_contents =
    \\ version: "3.0"
    \\ 
    \\ services:
    \\   replica0:
    \\     container_name: replica0
    \\     hostname: replica0
    \\     image: "replica:${TAG}"
    \\     environment:
    \\       - CLUSTER=1
    \\       - ADDRESSES=10.20.20.10:3000,10.20.20.11:3000,10.20.20.12:3000
    \\       - REPLICA_COUNT=3
    \\       - REPLICA=0
    \\     volumes:
    \\       - ./volumes/database:/var/data
    \\     networks:
    \\       antithesis-net:
    \\         ipv4_address: 10.20.20.10
    \\   replica1:
    \\     container_name: replica1
    \\     hostname: replica1
    \\     image: "replica:${TAG}"
    \\     environment:
    \\       - CLUSTER=1
    \\       - ADDRESSES=10.20.20.10:3000,10.20.20.11:3000,10.20.20.12:3000
    \\       - REPLICA_COUNT=3
    \\       - REPLICA=1
    \\     volumes:
    \\       - ./volumes/database:/var/data
    \\     networks:
    \\       antithesis-net:
    \\         ipv4_address: 10.20.20.11
    \\   replica2:
    \\     container_name: replica2
    \\     hostname: replica2
    \\     image: "replica:${TAG}"
    \\     environment:
    \\       - CLUSTER=1
    \\       - ADDRESSES=10.20.20.10:3000,10.20.20.11:3000,10.20.20.12:3000
    \\       - REPLICA_COUNT=3
    \\       - REPLICA=2
    \\     volumes:
    \\       - ./volumes/database:/var/data
    \\     networks:
    \\       antithesis-net:
    \\         ipv4_address: 10.20.20.12
    \\ 
    \\   workload:
    \\     container_name: workload
    \\     hostname: workload
    \\     image: "workload:${TAG}"
    \\     environment:
    \\       - CLUSTER=1
    \\       - REPLICAS=10.20.20.10:3000,10.20.20.11:3000,10.20.20.12:3000
    \\     networks:
    \\       antithesis-net:
    \\         ipv4_address: 10.20.20.100
    \\ 
    \\ # The subnet provided here is an example
    \\ # An alternate /24 can be used
    \\ networks:
    \\   antithesis-net:
    \\     driver: bridge
    \\     ipam:
    \\       config:
    \\         - subnet: 10.20.20.0/24
;
