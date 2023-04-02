const std = @import("std");
const builtin = @import("builtin");

const run = @import("./docs_generate.zig").run;
const run_with_env = @import("./docs_generate.zig").run_with_env;
const TmpDir = @import("./docs_generate.zig").TmpDir;
const git_root = @import("./docs_generate.zig").git_root;
const run_with_tb = @import("./run_with_tb.zig").run_with_tb;
const file_or_directory_exists = @import("./run_with_tb.zig").file_or_directory_exists;
const script_filename = @import("./run_with_tb.zig").script_filename;
const binary_filename = @import("./run_with_tb.zig").binary_filename;

// Caller is responsible for resetting to a good cwd after this completes.
fn find_tigerbeetle_client_jar(arena: *std.heap.ArenaAllocator) ![]const u8 {
    const root = try git_root(arena);
    try std.os.chdir(root);

    var i: usize = 2;
    var java_target_path: []const u8 = "";
    while (i > 0) {
        if (std.fs.cwd().realpathAlloc(arena.allocator(), "src/clients/java/target")) |path| {
            java_target_path = path;
            break;
        } else |err| switch (err) {
            else => {
                // target directory doesn't exist, let's try building the Java client.
                try std.os.chdir("src/clients/java");
                try run(arena, &[_][]const u8{
                    try script_filename(arena, &[_][]const u8{ "scripts", "install" }),
                });

                // Retry opening the directory now that we've run the install script.
                try std.os.chdir(root);
                i -= 1;
            },
        }
    }

    var dir = try std.fs.cwd().openDir(java_target_path, .{ .iterate = true });
    defer dir.close();

    var walker = try dir.walk(arena.allocator());
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (std.mem.endsWith(u8, entry.path, "-SNAPSHOT.jar")) {
            return std.fmt.allocPrint(
                arena.allocator(),
                "{s}/{s}",
                .{ java_target_path, entry.path },
            );
        }
    }

    std.debug.print("Could not find src/clients/java/target/**/*-SNAPSHOT.jar, run: cd src/clients/java && ./scripts/install.sh)\n", .{});
    return error.JarFileNotfound;
}

fn prepare_java_sample_integration_test(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
    cmds: *std.ArrayList([]const u8),
) !void {
    const jar_file = try find_tigerbeetle_client_jar(arena);
    try std.os.chdir(sample_dir);

    try run(arena, &[_][]const u8{
        "mvn",
        "deploy:deploy-file",
        try std.fmt.allocPrint(arena.allocator(), "-Durl=file://{s}", .{sample_dir}),
        try std.fmt.allocPrint(arena.allocator(), "-Dfile={s}", .{jar_file}),
        "-DgroupId=com.tigerbeetle",
        "-DartifactId=tigerbeetle-java",
        "-Dpackaging=jar",
        "-Dversion=0.0.1-3431",
    });

    // Write out local settings
    const local_settings_xml_path = try std.fmt.allocPrint(
        arena.allocator(),
        "{s}/local-settings.xml",
        .{sample_dir},
    );
    var local_settings_xml = try std.fs.cwd().createFile(
        local_settings_xml_path,
        .{ .truncate = true },
    );
    defer local_settings_xml.close();
    _ = try local_settings_xml.write(
        try std.fmt.allocPrint(
            arena.allocator(),
            \\<settings
            \\  xmlns="http://maven.apache.org/SETTINGS/1.0.0"
            \\  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            \\  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd"
            \\>
            \\  <localRepository>
            \\    {s}
            \\  </localRepository>
            \\</settings>
        ,
            .{sample_dir},
        ),
    );

    // Run mvn referencing local settings.xml and local JAR
    const cmdSep = if (builtin.os.tag == .windows) ";" else "&&";
    _ = try cmds.appendSlice(&[_][]const u8{
        if (builtin.os.tag == .windows) "powershell" else "sh",
        "-c",
        "mvn -s local-settings.xml install " ++ cmdSep ++
            " mvn -s local-settings.xml exec:java",
    });
}

fn prepare_go_sample_integration_test(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
    cmds: *std.ArrayList([]const u8),
) !void {
    try std.os.chdir(sample_dir);
    const root = try git_root(arena);

    const go_mod = try std.fs.cwd().openFile(
        "go.mod",
        .{ .write = true, .read = true },
    );
    defer go_mod.close();

    const file_size = try go_mod.getEndPos();
    var go_mod_contents = try arena.allocator().alloc(u8, file_size);

    _ = try go_mod.read(go_mod_contents);

    go_mod_contents = try std.mem.replaceOwned(
        u8,
        arena.allocator(),
        go_mod_contents,
        "require",
        try std.fmt.allocPrint(
            arena.allocator(),
            "replace github.com/tigerbeetledb/tigerbeetle-go => {s}/src/clients/go\n\nrequire",
            .{root},
        ),
    );

    // First truncate.
    try go_mod.setEndPos(0);
    // Reset cursor.
    try go_mod.seekTo(0);
    try go_mod.writeAll(go_mod_contents);

    try run(arena, &[_][]const u8{
        "go",
        "mod",
        "tidy",
    });

    _ = try cmds.appendSlice(&[_][]const u8{ "go", "run", "main.go" });
}

fn prepare_node_sample_integration_test(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
    cmds: *std.ArrayList([]const u8),
) !void {
    _ = arena;
    _ = sample_dir;
    _ = cmds;
    @panic("Unsupported");
}

fn copy_into_tmp_dir(
    arena: *std.heap.ArenaAllocator,
    sample_dir: []const u8,
) !TmpDir {
    var t = try TmpDir.init(arena);

    try run(arena, &[_][]const u8{
        if (builtin.os.tag == .windows) "powershell" else "sh",
        "-c",
        // Should actually work on Windows as well!
        try std.fmt.allocPrint(arena.allocator(), "cp -r {s}/* {s}/", .{ sample_dir, t.path }),
    });

    return t;
}

fn error_main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const root = try git_root(&arena);

    std.debug.print("Moved to git root: {s}\n", .{root});
    try std.os.chdir(root);

    var args = std.process.args();
    _ = args.next(allocator);
    var language: enum { none, java, go, node } = .none;
    var sample: []const u8 = "";
    var keep_tmp = false;
    while (args.next(allocator)) |arg_or_err| {
        const arg = arg_or_err catch {
            std.debug.print("Could not parse all arguments.\n", .{});
            return error.CouldNotParseArguments;
        };

        if (std.mem.eql(u8, arg, "--language")) {
            const next = try args.next(allocator) orelse "";

            if (std.mem.eql(u8, next, "java")) {
                language = .java;
            } else if (std.mem.eql(u8, next, "node")) {
                language = .node;
            } else if (std.mem.eql(u8, next, "go")) {
                language = .go;
            } else {
                std.debug.print("Unknown language: {s}.\n", .{next});
                return error.UnknownLanguage;
            }
        }

        if (std.mem.eql(u8, arg, "--keep-tmp")) {
            keep_tmp = true;
        }

        if (std.mem.eql(u8, arg, "--sample")) {
            const next = try args.next(allocator) orelse "";

            sample = next;
        }
    }

    if (sample.len == 0) {
        std.debug.print("--sample not set.\n", .{});
        return error.SampleNotSet;
    }

    if (language == .none) {
        std.debug.print("--language not set.\n", .{});
        return error.LanguageNotSet;
    }

    // Copy the sample into a temporary directory.
    const sample_dir = try std.fmt.allocPrint(
        allocator,
        "{s}/src/clients/{s}/samples/{s}",
        .{
            root,
            @tagName(language),
            sample,
        },
    );
    var tmp_copy = try copy_into_tmp_dir(&arena, sample_dir);
    defer {
        if (!keep_tmp) {
            tmp_copy.deinit();
        }
    }

    // Build up commands to pass to the runner, depends on the sample and the language
    var cmds = std.ArrayList([]const u8).init(allocator);

    switch (language) {
        .java => try prepare_java_sample_integration_test(&arena, tmp_copy.path, &cmds),
        .go => try prepare_go_sample_integration_test(&arena, tmp_copy.path, &cmds),
        .node => try prepare_node_sample_integration_test(&arena, tmp_copy.path, &cmds),
        .none => unreachable, // proven previously
    }

    try run_with_tb(&arena, cmds.items, tmp_copy.path);
}

// Returning errors in main produces useless traces, at least for some
// known errors. But using errors allows defers to run. So wrap the
// main but don't pass the error back to main. Just exit(1) on
// failure.
pub fn main() !void {
    if (error_main()) {
        // fine
    } else |err| switch (err) {
        error.RunCommandFailed => std.os.exit(1),
        else => return err,
    }
}
