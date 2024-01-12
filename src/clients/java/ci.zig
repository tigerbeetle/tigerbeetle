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

    inline for (.{ "basic", "two-phase", "two-phase-many", "walkthrough" }) |sample| {
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

pub fn validate_release(shell: *Shell, gpa: std.mem.Allocator, options: struct {
    version: []const u8,
    tigerbeetle: []const u8,
}) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());

    try shell.cwd.writeFile("pom.xml", try shell.print(
        \\<project>
        \\  <modelVersion>4.0.0</modelVersion>
        \\  <groupId>com.tigerbeetle</groupId>
        \\  <artifactId>samples</artifactId>
        \\  <version>1.0-SNAPSHOT</version>
        \\
        \\  <properties>
        \\    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        \\    <maven.compiler.source>11</maven.compiler.source>
        \\    <maven.compiler.target>11</maven.compiler.target>
        \\  </properties>
        \\
        \\  <build>
        \\    <plugins>
        \\      <plugin>
        \\        <groupId>org.apache.maven.plugins</groupId>
        \\        <artifactId>maven-compiler-plugin</artifactId>
        \\        <version>3.8.1</version>
        \\        <configuration>
        \\          <compilerArgs>
        \\            <arg>-Xlint:all,-options,-path</arg>
        \\          </compilerArgs>
        \\        </configuration>
        \\      </plugin>
        \\
        \\      <plugin>
        \\        <groupId>org.codehaus.mojo</groupId>
        \\        <artifactId>exec-maven-plugin</artifactId>
        \\        <version>1.6.0</version>
        \\        <configuration>
        \\          <mainClass>com.tigerbeetle.samples.Main</mainClass>
        \\        </configuration>
        \\      </plugin>
        \\    </plugins>
        \\  </build>
        \\
        \\  <dependencies>
        \\    <dependency>
        \\      <groupId>com.tigerbeetle</groupId>
        \\      <artifactId>tigerbeetle-java</artifactId>
        \\      <version>{s}</version>
        \\    </dependency>
        \\  </dependencies>
        \\</project>
    , .{options.version}));

    try Shell.copy_path(
        shell.project_root,
        "src/clients/java/samples/basic/src/main/java/Main.java",
        shell.cwd,
        "src/main/java/Main.java",
    );

    // According to the docs, java package might not be immediately available:
    //
    //  > Upon release, your component will be published to Central: this typically occurs within 30
    //  > minutes, though updates to search can take up to four hours.
    //
    // <https://central.sonatype.org/publish/publish-guide/#releasing-to-central>
    //
    // Retry the download for 45 minutes, passing `--update-snapshots` to thwart local negative
    // caching.
    for (0..9) |_| {
        if (try mvn_update(shell) == .ok) break;
        log.warn("waiting for 5 minutes for the {s} version to appear in maven cental", .{
            options.version,
        });
        std.time.sleep(5 * std.time.ns_per_min);
    } else {
        switch (try mvn_update(shell)) {
            .ok => {},
            .retry => |err| {
                log.err("package is not available in maven central", .{});
                return err;
            },
        }
    }

    try shell.exec("mvn exec:java", .{});
}

fn mvn_update(shell: *Shell) !union(enum) { ok, retry: anyerror } {
    if (shell.exec("mvn package --update-snapshots", .{})) {
        return .ok;
    } else |err| {
        // Re-run immediately to capture stdout (sic) to check if the failure is indeed due to
        // the package missing from the registry.
        const exec_result = try shell.exec_raw("mvn package --update-snapshots", .{});
        switch (exec_result.term) {
            .Exited => |code| if (code == 0) return .ok,
            else => {},
        }

        const package_missing = std.mem.indexOf(
            u8,
            exec_result.stdout,
            "Could not resolve dependencies",
        ) != null;
        if (package_missing) return .{ .retry = err };

        return err;
    }
}
