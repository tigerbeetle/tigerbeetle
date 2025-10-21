const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    assert(shell.file_exists("pom.xml"));

    try shell.exec_zig("build clients:java -Drelease", .{});
    try shell.exec_zig("build -Drelease", .{});

    try shell.exec_zig("build test:jni", .{});
    // Java's maven doesn't support a separate test command, or a way to add dependency on a
    // project (as opposed to a compiled jar file).
    //
    // For this reason, we do all our testing in one go, imperatively building a client jar and
    // installing it into the local maven repository.
    try shell.exec("mvn --batch-mode --file pom.xml --quiet formatter:validate", .{});
    try shell.exec("mvn --batch-mode --file pom.xml --quiet install", .{});

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
        .development = true,
        .prebuilt = options.tigerbeetle,
    });
    defer tmp_beetle.deinit(gpa);
    errdefer tmp_beetle.log_stderr();

    try shell.env.put("TB_ADDRESS", tmp_beetle.port_str);

    try shell.cwd.writeFile(.{ .sub_path = "pom.xml", .data = try shell.fmt(
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
    , .{options.version}) });

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

pub fn release_published_latest(shell: *Shell) ![]const u8 {
    const url = "https://central.sonatype.com/api/internal/browse/component/versions?" ++
        "sortField=normalizedVersion&sortDirection=desc&page=0&size=1&" ++
        "filter=namespace%3Acom.tigerbeetle%2Cname%3Atigerbeetle-java";

    const response_body = try shell.http_get(url, .{});

    const MavenSearch = struct {
        const Component = struct {
            namespace: []const u8,
            name: []const u8,
            version: []const u8,
        };
        components: []Component,
    };

    const maven_search_results = try std.json.parseFromSliceLeaky(
        MavenSearch,
        shell.arena.allocator(),
        response_body,
        .{ .ignore_unknown_fields = true },
    );

    assert(maven_search_results.components.len == 1);

    assert(std.mem.eql(u8, maven_search_results.components[0].namespace, "com.tigerbeetle"));
    assert(std.mem.eql(u8, maven_search_results.components[0].name, "tigerbeetle-java"));

    return maven_search_results.components[0].version;
}
