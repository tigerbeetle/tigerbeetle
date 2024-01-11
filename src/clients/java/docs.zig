const builtin = @import("builtin");
const std = @import("std");

const Docs = @import("../docs_types.zig").Docs;

pub const JavaDocs = Docs{
    .directory = "java",

    .markdown_name = "java",
    .extension = "java",
    .proper_name = "Java",

    .test_source_path = "src/main/java/",

    .name = "tigerbeetle-java",
    .description =
    \\The TigerBeetle client for Java.
    \\
    \\[![javadoc](https://javadoc.io/badge2/com.tigerbeetle/tigerbeetle-java/javadoc.svg)](https://javadoc.io/doc/com.tigerbeetle/tigerbeetle-java)
    \\
    \\[![maven-central](https://img.shields.io/maven-central/v/com.tigerbeetle/tigerbeetle-java)](https://central.sonatype.com/namespace/com.tigerbeetle)
    ,

    .prerequisites =
    \\* Java >= 11
    \\* Maven >= 3.6 (not strictly necessary but it's what our guides assume)
    ,

    .project_file_name = "pom.xml",
    .project_file =
    \\<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    \\         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    \\  <modelVersion>4.0.0</modelVersion>
    \\
    \\  <groupId>com.tigerbeetle</groupId>
    \\  <artifactId>samples</artifactId>
    \\  <version>1.0-SNAPSHOT</version>
    \\
    \\  <properties>
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
    \\      <!-- Grab the latest commit from: https://repo1.maven.org/maven2/com/tigerbeetle/tigerbeetle-java/maven-metadata.xml -->
    \\      <version>0.0.1-3431</version>
    \\    </dependency>
    \\  </dependencies>
    \\</project>
    ,

    .test_file_name = "Main",

    .install_commands = "mvn install",
    .run_commands = "mvn exec:java",

    .examples = "",

    .client_object_documentation = "",

    .create_accounts_documentation =
    \\The 128-bit fields like `id` and `user_data_128` have a few
    \\overrides to make it easier to integrate. You can either
    \\pass in a long, a pair of longs (least and most
    \\significant bits), or a `byte[]`.
    \\
    \\There is also a `com.tigerbeetle.UInt128` helper with static
    \\methods for converting 128-bit little-endian unsigned integers
    \\between instances of `long`, `java.util.UUID`, `java.math.BigInteger` and `byte[]`.
    \\
    \\The fields for transfer amounts and account balances are also 128-bit,
    \\but they are always represented as a `java.math.BigInteger`.
    ,

    .account_flags_documentation =
    \\To toggle behavior for an account, combine enum values stored in the
    \\`AccountFlags` object with bitwise-or:
    \\
    \\* `AccountFlags.LINKED`
    \\* `AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS`
    \\* `AccountFlags.CREDITS_MUST_NOT_EXCEED_CREDITS`
    ,

    .create_accounts_errors_documentation = "",

    .create_transfers_documentation = "",

    .create_transfers_errors_documentation = "",

    .transfer_flags_documentation =
    \\To toggle behavior for an account, combine enum values stored in the
    \\`TransferFlags` object with bitwise-or:
    \\
    \\* `TransferFlags.NONE`
    \\* `TransferFlags.LINKED`
    \\* `TransferFlags.PENDING`
    \\* `TransferFlags.POST_PENDING_TRANSFER`
    \\* `TransferFlags.VOID_PENDING_TRANSFER`
    ,
};
