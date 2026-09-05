//! Custom build step to align the members of a Mach-O static library to 8 bytes.
//!
//! Zig's MachO linker starts archive members on 2-byte boundaries, while Apple's linker requires
//! 64-bit Mach-O members to start on 8-byte boundaries and rejects the archive otherwise:
//!
//!     ld: 64-bit mach-o member 'libtb_client.a.o' not 8-byte aligned in 'libtb_client.a'
//!
//! Apple's `ar` pads the `#1/N` name to a multiple of 8 plus 4 bytes and the data to a multiple of
//! 8, counting both in the size field. The archive is rewritten with that layout: member order,
//! names, metadata and bytes are kept, and the member offsets in `__.SYMDEF` are updated.

const std = @import("std");
const assert = std.debug.assert;
const stdx = @import("stdx");

const magic = "!<arch>\n";
const header_size = 60;
const header_end = "`\n";
const name_prefix = "#1/";

const member_alignment = 8;
const member_count_max = 32;
const archive_size_max = 64 * stdx.MiB;

/// The name is followed by the data, so the name padding compensates for the header size.
const name_padding_min = member_alignment - header_size % member_alignment;

comptime {
    assert(magic.len % member_alignment == 0);
    assert(name_padding_min == 4);
}

const CLIArgs = struct {
    input: []const u8,
    output: []const u8,
};

pub fn main() !void {
    var allocator: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer {
        if (allocator.deinit() != .ok) {
            @panic("memory leaked");
        }
    }

    const gpa = allocator.allocator();

    var flags = stdx.Flags.init(gpa);
    defer flags.deinit(gpa);

    const cli_args = flags.parse(CLIArgs);

    const archive = try std.fs.cwd().readFileAlloc(gpa, cli_args.input, archive_size_max);
    defer gpa.free(archive);

    const archive_aligned = try align_members(gpa, archive);
    defer gpa.free(archive_aligned);

    try std.fs.cwd().writeFile(.{ .sub_path = cli_args.output, .data = archive_aligned });
}

const Member = struct {
    /// The `ar_hdr` of the member. Only the name and size fields are rewritten.
    header: *const [header_size]u8,
    /// Offset of the header from the start of the archive.
    offset: u32,
    /// The member name, without the zero bytes that pad it.
    name: []const u8,
    /// Size of the name with its padding. Counted in the size field of the header.
    name_size: u32,
    /// The bytes after the name: an object file, or the symbol table for `__.SYMDEF`.
    data: []const u8,

    fn data_offset(member: Member) u32 {
        return member.offset + header_size + member.name_size;
    }
};

const Members = stdx.BoundedArrayType(Member, member_count_max);

/// `__.SYMDEF` stores symbol name and member offsets as u32, `__.SYMDEF_64` as u64.
const SymtabFormat = enum { p32, p64 };

fn symtab_format(name: []const u8) ?SymtabFormat {
    const symtab_names = .{
        .{ "__.SYMDEF", .p32 },
        .{ "__.SYMDEF SORTED", .p32 },
        .{ "__.SYMDEF_64", .p64 },
        .{ "__.SYMDEF_64 SORTED", .p64 },
    };
    inline for (symtab_names) |symtab_name| {
        const text, const format = symtab_name;
        if (std.mem.eql(u8, name, text)) return format;
    }
    return null;
}

pub fn align_members(gpa: std.mem.Allocator, archive: []const u8) ![]u8 {
    assert(archive.len <= archive_size_max);

    var members: Members = .{};
    try parse(archive, &members);

    const format = symtab_format(members.get(0).name) orelse return error.SymtabMissing;
    for (members.const_slice()[1..]) |member| {
        if (symtab_format(member.name) != null) return error.SymtabDuplicate;
    }

    var offsets_aligned: [member_count_max]u32 = undefined;
    var size_aligned: u32 = magic.len;
    for (members.const_slice(), 0..) |member, index| {
        assert(size_aligned % member_alignment == 0);
        offsets_aligned[index] = size_aligned;
        size_aligned += header_size;
        size_aligned += name_size_aligned(member.name);
        size_aligned += data_size_aligned(member.data);
    }

    const archive_aligned = try gpa.alloc(u8, size_aligned);
    errdefer gpa.free(archive_aligned);

    try write(archive_aligned, members.const_slice(), offsets_aligned[0..members.count()], format);

    // Parse the result to check the layout the way a consumer sees it.
    var members_aligned: Members = .{};
    try parse(archive_aligned, &members_aligned);
    assert(members_aligned.count() == members.count());
    for (members_aligned.const_slice(), members.const_slice(), 0..) |aligned, member, index| {
        assert(aligned.offset % member_alignment == 0);
        assert(aligned.data_offset() % member_alignment == 0);
        assert(aligned.data.len == data_size_aligned(member.data));
        assert(std.mem.eql(u8, aligned.name, member.name));
        // The symbol table is rewritten with the new member offsets, the objects are copied.
        if (index > 0) assert(std.mem.startsWith(u8, aligned.data, member.data));
    }

    return archive_aligned;
}

fn parse(archive: []const u8, members: *Members) !void {
    assert(members.empty());
    assert(archive.len <= archive_size_max);

    if (archive.len < magic.len) return error.ArchiveTruncated;
    if (!std.mem.eql(u8, archive[0..magic.len], magic)) return error.MagicInvalid;

    var offset: u32 = magic.len;
    for (0..member_count_max) |_| {
        if (offset == archive.len) break;
        // Members start on even offsets. The pad byte after a member of odd size is not counted
        // in its size field.
        if (offset % 2 == 1) {
            offset += 1;
            if (offset == archive.len) break;
        }
        if (archive.len - offset < header_size) return error.HeaderTruncated;

        const header = archive[offset..][0..header_size];
        if (!std.mem.eql(u8, header[58..], header_end)) return error.HeaderInvalid;
        const name_field = stdx.cut_prefix(header[0..16], name_prefix) orelse {
            return error.NameInvalid;
        };
        const name_size = try field_parse(name_field);
        const size = try field_parse(header[48..58]);
        if (size > archive.len - offset - header_size) return error.MemberTruncated;
        if (name_size > size) return error.NameInvalid;

        const name_padded = archive[offset + header_size ..][0..name_size];
        const name = std.mem.trimRight(u8, name_padded, "\x00");
        if (name.len == 0) return error.NameInvalid;

        members.push(.{
            .header = header,
            .offset = offset,
            .name = name,
            .name_size = name_size,
            .data = archive[offset + header_size + name_size ..][0 .. size - name_size],
        });
        offset += header_size + size;
    } else {
        return error.MemberCountExceeded;
    }

    if (members.empty()) return error.ArchiveEmpty;
}

/// Header fields hold decimal text, padded on the right with spaces.
fn field_parse(field: []const u8) !u32 {
    const text = std.mem.trimRight(u8, field, " ");
    return stdx.parse_int(u32, text, .{}) catch return error.HeaderInvalid;
}

/// Apple's `ar` pads the name to a multiple of 8 plus 4 bytes: after the 60-byte header, the data
/// then starts on an 8-byte boundary, and at least 4 zero bytes terminate the name.
fn name_size_aligned(name: []const u8) u32 {
    assert(name.len > 0);
    const name_size = std.mem.alignForward(u32, @intCast(name.len), member_alignment) +
        name_padding_min;
    assert(name_size > name.len);
    assert((header_size + name_size) % member_alignment == 0);
    return name_size;
}

fn data_size_aligned(data: []const u8) u32 {
    const data_size = std.mem.alignForward(u32, @intCast(data.len), member_alignment);
    assert(data_size >= data.len);
    assert(data_size < data.len + member_alignment);
    return data_size;
}

fn write(
    archive: []u8,
    members: []const Member,
    offsets: []const u32,
    format: SymtabFormat,
) !void {
    assert(members.len > 0);
    assert(members.len == offsets.len);
    assert(symtab_format(members[0].name) == format);

    stdx.copy_disjoint(.exact, u8, archive[0..magic.len], magic);
    for (members, offsets, 0..) |member, offset, index| {
        assert(offset % member_alignment == 0);
        const name_size = name_size_aligned(member.name);
        const data_size = data_size_aligned(member.data);

        header_write(archive[offset..][0..header_size], member.header, .{
            .name_size = name_size,
            .size = name_size + data_size,
        });

        const name = archive[offset + header_size ..][0..name_size];
        @memset(name, 0);
        stdx.copy_disjoint(.inexact, u8, name, member.name);

        const data_offset = offset + header_size + name_size;
        assert(data_offset % member_alignment == 0);
        const data = archive[data_offset..][0..data_size];
        @memset(data[member.data.len..], 0);
        stdx.copy_disjoint(.inexact, u8, data, member.data);
        if (index == 0) {
            switch (format) {
                .p32 => try symtab_relocate(u32, data[0..member.data.len], members, offsets),
                .p64 => try symtab_relocate(u64, data[0..member.data.len], members, offsets),
            }
        }

        const offset_next = data_offset + data_size;
        assert(offset_next % member_alignment == 0);
        if (index + 1 < members.len) {
            assert(offset_next == offsets[index + 1]);
        } else {
            assert(offset_next == archive.len);
        }
    }
}

fn header_write(
    target: *[header_size]u8,
    source: *const [header_size]u8,
    options: struct { name_size: u32, size: u32 },
) void {
    assert(std.mem.eql(u8, source[58..], header_end));
    assert(options.name_size <= options.size);

    target.* = source.*;
    @memset(target[0..16], ' ');
    _ = std.fmt.bufPrint(target[0..16], "{s}{d}", .{ name_prefix, options.name_size }) catch
        unreachable;
    @memset(target[48..58], ' ');
    _ = std.fmt.bufPrint(target[48..58], "{d}", .{options.size}) catch unreachable;
}

/// The symbol table is `entries_size: Int, entries: [entries_size / 16]{ name_offset: Int,
/// member_offset: Int }, strtab_size: Int, strtab: [strtab_size]u8`, little-endian, where
/// `member_offset` is the offset of the member's header. Points the entries at the new offsets.
fn symtab_relocate(
    comptime Int: type,
    symtab: []u8,
    members: []const Member,
    offsets: []const u32,
) !void {
    assert(members.len == offsets.len);
    const int_size = @sizeOf(Int);
    const entry_size = 2 * int_size;

    if (symtab.len < int_size) return error.SymtabTruncated;
    const entries_size = std.mem.readInt(Int, symtab[0..int_size], .little);
    if (entries_size % entry_size != 0) return error.SymtabInvalid;
    if (entries_size > symtab.len - int_size) return error.SymtabTruncated;
    const entries = symtab[int_size..][0..@intCast(entries_size)];

    const strtab_size_offset = int_size + entries.len;
    if (symtab.len - strtab_size_offset < int_size) return error.SymtabTruncated;
    const strtab_size = std.mem.readInt(Int, symtab[strtab_size_offset..][0..int_size], .little);
    if (strtab_size > symtab.len - strtab_size_offset - int_size) return error.SymtabTruncated;

    for (0..entries.len / entry_size) |index| {
        const member_offset = entries[index * entry_size + int_size ..][0..int_size];
        const offset_old = std.mem.readInt(Int, member_offset, .little);
        // The symbol table itself has no symbols, so its own offset is not a valid target.
        const offset_new = for (members[1..], offsets[1..]) |member, offset| {
            if (member.offset == offset_old) break offset;
        } else return error.SymtabOffsetInvalid;
        std.mem.writeInt(Int, member_offset, offset_new, .little);
    }
}

const TestObject = struct { name: []const u8, data: []const u8 };

/// Header fields as Apple's `ar` writes them, to check that the tool copies them unchanged.
const test_header = "                " ++ "1788586078  " ++ "501   " ++ "20    " ++ "100644  " ++
    "          " ++ header_end;

comptime {
    assert(test_header.len == header_size);
}

/// Writes an archive the way Zig 0.14.1 does: `#1/N` names padded with zero bytes to the symbol
/// table's integer size (always with a terminating zero), members on 2-byte boundaries, and a
/// symbol table with one symbol per object, named after the object.
fn test_archive_zig(
    gpa: std.mem.Allocator,
    format: SymtabFormat,
    objects: []const TestObject,
) ![]u8 {
    const int_size: u32 = switch (format) {
        .p32 => 4,
        .p64 => 8,
    };
    const symtab_name: []const u8 = switch (format) {
        .p32 => "__.SYMDEF",
        .p64 => "__.SYMDEF_64",
    };
    const symtab_name_size = test_name_size_zig(symtab_name, int_size);

    var strtab_size: u32 = 0;
    for (objects) |object| strtab_size += @intCast(object.name.len + 1);
    strtab_size = std.mem.alignForward(u32, strtab_size, int_size);
    const entries_size: u32 = @intCast(objects.len * 2 * int_size);
    const symtab_size = int_size + entries_size + int_size + strtab_size;

    const offsets = try gpa.alloc(u32, objects.len);
    defer gpa.free(offsets);

    var size: u32 = magic.len;
    size += header_size + symtab_name_size + symtab_size;
    for (objects, offsets) |object, *offset| {
        size = std.mem.alignForward(u32, size, 2);
        offset.* = size;
        size += header_size + test_name_size_zig(object.name, int_size);
        size += @intCast(object.data.len);
    }

    const archive = try gpa.alloc(u8, size);
    errdefer gpa.free(archive);

    @memset(archive, '\n'); // The pad byte between members.
    stdx.copy_disjoint(.exact, u8, archive[0..magic.len], magic);

    header_write(archive[magic.len..][0..header_size], test_header, .{
        .name_size = symtab_name_size,
        .size = symtab_name_size + symtab_size,
    });
    test_name_write(archive[magic.len + header_size ..][0..symtab_name_size], symtab_name);
    const symtab = archive[magic.len + header_size + symtab_name_size ..][0..symtab_size];
    switch (format) {
        .p32 => test_symtab_write(u32, symtab, objects, offsets),
        .p64 => test_symtab_write(u64, symtab, objects, offsets),
    }

    for (objects, offsets) |object, offset| {
        const name_size = test_name_size_zig(object.name, int_size);
        header_write(archive[offset..][0..header_size], test_header, .{
            .name_size = name_size,
            .size = name_size + @as(u32, @intCast(object.data.len)),
        });
        test_name_write(archive[offset + header_size ..][0..name_size], object.name);
        const data = archive[offset + header_size + name_size ..][0..object.data.len];
        stdx.copy_disjoint(.exact, u8, data, object.data);
    }
    return archive;
}

fn test_name_size_zig(name: []const u8, int_size: u32) u32 {
    return std.mem.alignForward(u32, @intCast(name.len + 1), int_size);
}

fn test_name_write(target: []u8, name: []const u8) void {
    assert(target.len > name.len);
    @memset(target, 0);
    stdx.copy_disjoint(.inexact, u8, target, name);
}

fn test_symtab_write(
    comptime Int: type,
    symtab: []u8,
    objects: []const TestObject,
    offsets: []const u32,
) void {
    const int_size = @sizeOf(Int);
    const entries_size = objects.len * 2 * int_size;
    const strtab = symtab[int_size + entries_size + int_size ..];

    @memset(symtab, 0);
    std.mem.writeInt(Int, symtab[0..int_size], @intCast(entries_size), .little);
    var strtab_offset: usize = 0;
    for (objects, offsets, 0..) |object, offset, index| {
        const entry = symtab[int_size + index * 2 * int_size ..][0 .. 2 * int_size];
        std.mem.writeInt(Int, entry[0..int_size], @intCast(strtab_offset), .little);
        std.mem.writeInt(Int, entry[int_size..][0..int_size], offset, .little);
        stdx.copy_disjoint(.inexact, u8, strtab[strtab_offset..], object.name);
        strtab_offset += object.name.len + 1;
    }
    const strtab_size = symtab[int_size + entries_size ..][0..int_size];
    std.mem.writeInt(Int, strtab_size, @intCast(strtab.len), .little);
}

/// Aligns the archive and checks that the members only gained padding, that the symbol table
/// still points at the same members, and that aligning the result changes nothing.
fn test_align_members(gpa: std.mem.Allocator, archive: []const u8) !void {
    var members: Members = .{};
    try parse(archive, &members);

    const archive_aligned = try align_members(gpa, archive);
    defer gpa.free(archive_aligned);

    var members_aligned: Members = .{};
    try parse(archive_aligned, &members_aligned);

    try std.testing.expectEqual(members.count(), members_aligned.count());
    for (members.const_slice(), members_aligned.const_slice(), 0..) |member, aligned, index| {
        try std.testing.expectEqual(0, aligned.offset % member_alignment);
        try std.testing.expectEqual(0, aligned.data_offset() % member_alignment);
        try std.testing.expectEqual(0, aligned.data.len % member_alignment);
        try std.testing.expectEqual(name_padding_min, aligned.name_size % member_alignment);
        try std.testing.expectEqualSlices(u8, member.name, aligned.name);
        try std.testing.expectEqualSlices(u8, member.header[16..48], aligned.header[16..48]);
        try std.testing.expectEqual(data_size_aligned(member.data), aligned.data.len);
        if (index > 0) try std.testing.expect(std.mem.startsWith(u8, aligned.data, member.data));
        try std.testing.expect(stdx.zeroed(aligned.data[member.data.len..]));
        const name_padded = archive_aligned[aligned.offset + header_size ..][0..aligned.name_size];
        try std.testing.expect(stdx.zeroed(name_padded[member.name.len..]));
    }

    switch (symtab_format(members.get(0).name).?) {
        .p32 => try test_symtab_check(u32, members.const_slice(), members_aligned.const_slice()),
        .p64 => try test_symtab_check(u64, members.const_slice(), members_aligned.const_slice()),
    }

    const archive_aligned_again = try align_members(gpa, archive_aligned);
    defer gpa.free(archive_aligned_again);

    try std.testing.expectEqualSlices(u8, archive_aligned, archive_aligned_again);
}

fn test_symtab_check(
    comptime Int: type,
    members: []const Member,
    members_aligned: []const Member,
) !void {
    const int_size = @sizeOf(Int);
    const symtab = members[0].data;
    const symtab_aligned = members_aligned[0].data[0..symtab.len];

    const entries_size: usize = @intCast(std.mem.readInt(Int, symtab[0..int_size], .little));
    try std.testing.expectEqualSlices(u8, symtab[0..int_size], symtab_aligned[0..int_size]);
    for (0..entries_size / (2 * int_size)) |index| {
        const entry = symtab[int_size + index * 2 * int_size ..][0 .. 2 * int_size];
        const entry_aligned = symtab_aligned[int_size + index * 2 * int_size ..][0 .. 2 * int_size];
        try std.testing.expectEqualSlices(u8, entry[0..int_size], entry_aligned[0..int_size]);

        const offset = std.mem.readInt(Int, entry[int_size..][0..int_size], .little);
        const offset_aligned = std.mem.readInt(
            Int,
            entry_aligned[int_size..][0..int_size],
            .little,
        );
        const member_index = for (members, 0..) |member, i| {
            if (member.offset == offset) break i;
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqual(members_aligned[member_index].offset, offset_aligned);
    }
    try std.testing.expectEqualSlices(
        u8,
        symtab[int_size + entries_size ..],
        symtab_aligned[int_size + entries_size ..],
    );
}

test "archive_align: members start on 8-byte boundaries and are otherwise unchanged" {
    const gpa = std.testing.allocator;
    var prng = stdx.PRNG.from_seed_testing();

    var data: [32]u8 = undefined;
    prng.fill(&data);
    var names: [2][24]u8 = undefined;
    for (&names, [_]u8{ 'a', 'A' }) |*name, first| {
        for (name, 0..) |*byte, index| byte.* = first + @as(u8, @intCast(index));
    }

    // Every combination of name and data lengths, so that the symbol table and the members take
    // every size modulo 8, and members of the input start on 2, 4 and 8-byte boundaries.
    var misaligned_count: u32 = 0;
    for (1..names[0].len + 1) |name_len| {
        for (0..18) |data_len| {
            for ([_]SymtabFormat{ .p32, .p64 }) |format| {
                const objects = [_]TestObject{
                    .{ .name = names[0][0..name_len], .data = data[0..data_len] },
                    .{
                        .name = names[1][0..name_len],
                        .data = data[data_len..][0 .. (data_len * 5) % 15],
                    },
                };
                const archive = try test_archive_zig(gpa, format, &objects);
                defer gpa.free(archive);

                var members: Members = .{};
                try parse(archive, &members);
                for (members.const_slice()) |member| {
                    if (member.data_offset() % member_alignment != 0) misaligned_count += 1;
                }

                try test_align_members(gpa, archive);
            }
        }
    }
    try std.testing.expect(misaligned_count > 0);
}

test "archive_align: layout" {
    const gpa = std.testing.allocator;
    const objects = [_]TestObject{
        .{ .name = "libtb_client.a.o", .data = "0123456789" ** 10 ++ "01" },
        .{ .name = "compiler_rt.o", .data = "0123456789" ** 3 ++ "012" },
    };
    const archive = try test_archive_zig(gpa, .p32, &objects);
    defer gpa.free(archive);

    try test_layout_check(archive, snap(@src(),
        \\#1/12 __.SYMDEF        header=8   data=80  size=56
        \\#1/20 libtb_client.a.o header=136 data=216 size=102
        \\#1/16 compiler_rt.o    header=318 data=394 size=33
        \\
    ));

    const archive_aligned = try align_members(gpa, archive);
    defer gpa.free(archive_aligned);

    try test_layout_check(archive_aligned, snap(@src(),
        \\#1/20 __.SYMDEF        header=8   data=88  size=56
        \\#1/20 libtb_client.a.o header=144 data=224 size=104
        \\#1/20 compiler_rt.o    header=328 data=408 size=40
        \\
    ));
}

const Snap = stdx.Snap;
const snap = Snap.snap_fn("src");

fn test_layout_check(archive: []const u8, want: Snap) !void {
    var members: Members = .{};
    try parse(archive, &members);

    var layout: std.ArrayListUnmanaged(u8) = .empty;
    defer layout.deinit(std.testing.allocator);

    for (members.const_slice()) |member| {
        try layout.writer(std.testing.allocator).print(
            "#1/{d:<2} {s:<16} header={d:<3} data={d:<3} size={d}\n",
            .{
                member.name_size,
                member.name,
                member.offset,
                member.data_offset(),
                member.data.len,
            },
        );
    }
    try want.diff(layout.items);
}

test "archive_align: invalid archives are rejected" {
    const gpa = std.testing.allocator;
    const objects = [_]TestObject{
        .{ .name = "a.o", .data = "object a" },
        .{ .name = "b.o", .data = "object b!" },
    };
    const archive = try test_archive_zig(gpa, .p32, &objects);
    defer gpa.free(archive);

    // Every object has a symbol, so a truncated archive leaves a symbol without its member.
    for (0..archive.len) |len| {
        if (align_members(gpa, archive[0..len])) |archive_aligned| {
            gpa.free(archive_aligned);
            return error.TestUnexpectedResult;
        } else |_| {}
    }

    // Any single corrupted byte is either rejected or leaves a valid archive to align.
    const corrupted = try gpa.dupe(u8, archive);
    defer gpa.free(corrupted);

    for (0..archive.len) |index| {
        for ([_]u8{ 0, 1, '/', 0xFF }) |byte| {
            corrupted[index] = byte;
            if (align_members(gpa, corrupted)) |archive_aligned| {
                gpa.free(archive_aligned);
            } else |_| {}
        }
        corrupted[index] = archive[index];
    }

    try std.testing.expectError(error.MagicInvalid, align_members(gpa, "!<arch>\r"));
    try std.testing.expectError(error.ArchiveEmpty, align_members(gpa, "!<arch>\n"));

    var members: Members = .{};
    try parse(archive, &members);
    const object = members.get(1);

    stdx.copy_disjoint(.inexact, u8, corrupted[object.offset..], "a.o/");
    try std.testing.expectError(error.NameInvalid, align_members(gpa, corrupted));
    stdx.copy_disjoint(.inexact, u8, corrupted[object.offset..], archive[object.offset..][0..4]);

    stdx.copy_disjoint(.inexact, u8, corrupted[magic.len + header_size ..], "__.SYMDEX");
    try std.testing.expectError(error.SymtabMissing, align_members(gpa, corrupted));
    stdx.copy_disjoint(.inexact, u8, corrupted[magic.len + header_size ..], "__.SYMDEF");

    const symtab = corrupted[members.get(0).data_offset()..][0..members.get(0).data.len];
    std.mem.writeInt(u32, symtab[8..12], object.offset + 2, .little);
    try std.testing.expectError(error.SymtabOffsetInvalid, align_members(gpa, corrupted));
    std.mem.writeInt(u32, symtab[8..12], object.offset, .little);
    try std.testing.expectEqualSlices(u8, archive, corrupted);

    const objects_duplicate = [_]TestObject{
        .{ .name = "a.o", .data = "object a" },
        .{ .name = "__.SYMDEF", .data = "object b!" },
    };
    const archive_duplicate = try test_archive_zig(gpa, .p32, &objects_duplicate);
    defer gpa.free(archive_duplicate);

    try std.testing.expectError(error.SymtabDuplicate, align_members(gpa, archive_duplicate));

    const objects_many = [_]TestObject{.{ .name = "a.o", .data = "object a" }} ** member_count_max;
    const archive_many = try test_archive_zig(gpa, .p32, &objects_many);
    defer gpa.free(archive_many);

    try std.testing.expectError(error.MemberCountExceeded, align_members(gpa, archive_many));
}
