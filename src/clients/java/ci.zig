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

    try shell.exec("mvn package", .{});
    try shell.exec("mvn exec:java", .{});
}
