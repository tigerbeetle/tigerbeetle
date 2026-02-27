//! Minimal native objcopy implementation used by multiversion builds.
//! Supports only the subset of flags used in this repository.

const std = @import("std");
const elf = std.elf;
const stdx = @import("stdx");
const assert = std.debug.assert;

const Section = enum {
    tb_mvb,
    tb_mvh,

    fn name(section: Section) []const u8 {
        return switch (section) {
            .tb_mvb => ".tb_mvb",
            .tb_mvh => ".tb_mvh",
        };
    }
};

const AddSection = struct {
    section: Section,
    path: []const u8,
    bytes: []const u8 = "",
};

const CLI = struct {
    source: []const u8 = "",
    output: []const u8 = "",
    add_sections: [2]?AddSection = .{ null, null },
    remove_sections: [2]bool = .{ false, false },
};

pub fn main() !void {
    var gpa_state: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer if (gpa_state.deinit() != .ok) @panic("memory leaked");

    const gpa = gpa_state.allocator();

    var arguments = try std.process.argsWithAllocator(gpa);
    defer arguments.deinit();

    var cli = try parse_cli_args(&arguments);
    const source_file_mode = (try std.fs.cwd().statFile(cli.source)).mode;

    for (cli.add_sections, 0..) |maybe_add, i| {
        if (maybe_add == null) continue;
        var add = maybe_add.?;
        add.bytes = try std.fs.cwd().readFileAlloc(gpa, add.path, std.math.maxInt(usize));
        cli.add_sections[i] = add;
    }
    defer for (cli.add_sections) |maybe_add| {
        if (maybe_add) |add| gpa.free(add.bytes);
    };

    const input = try std.fs.cwd().readFileAlloc(gpa, cli.source, std.math.maxInt(usize));
    defer gpa.free(input);

    const has_section_changes = cli_has_section_changes(&cli);
    if (!has_section_changes and is_pe(input)) {
        try std.fs.cwd().writeFile(.{
            .sub_path = cli.output,
            .data = input,
            .flags = .{ .mode = source_file_mode },
        });
        return;
    }

    const output = if (is_elf(input))
        try transform_elf(gpa, input, &cli)
    else if (is_pe(input))
        try transform_pe(gpa, input, &cli)
    else
        return error.UnsupportedFormat;
    defer gpa.free(output);

    try std.fs.cwd().writeFile(.{
        .sub_path = cli.output,
        .data = output,
        .flags = .{ .mode = source_file_mode },
    });
}

// Parse the supported objcopy CLI subset and normalize in-place output mode.
fn parse_cli_args(arguments: *std.process.ArgIterator) !CLI {
    _ = arguments.next() orelse return error.InvalidArguments;

    var cli: CLI = .{};
    var positional_argument_count: usize = 0;
    while (arguments.next()) |argument| {
        if (std.mem.eql(u8, argument, "--enable-deterministic-archives") or
            std.mem.eql(u8, argument, "--keep-undefined"))
        {
            continue;
        }

        if (try parse_option_value(arguments, argument, "--add-section")) |value| {
            try parse_add_section(&cli, value);
            continue;
        }

        if (try parse_option_value(arguments, argument, "--remove-section")) |value| {
            try parse_remove_section(&cli, value);
            continue;
        }

        if (try parse_option_value(arguments, argument, "--set-section-flags")) |value| {
            try validate_section_flags(value);
            continue;
        }

        if (std.mem.startsWith(u8, argument, "-")) return error.InvalidArguments;

        positional_argument_count += 1;
        switch (positional_argument_count) {
            1 => cli.source = argument,
            2 => cli.output = argument,
            else => return error.InvalidArguments,
        }
    }

    if (positional_argument_count == 0) return error.InvalidArguments;
    if (positional_argument_count == 1) cli.output = cli.source;
    return cli;
}

// Parse `--option value` and `--option=value` forms into a single value.
fn parse_option_value(
    arguments: *std.process.ArgIterator,
    argument: []const u8,
    comptime option_name: []const u8,
) !?[]const u8 {
    if (std.mem.eql(u8, argument, option_name)) {
        return arguments.next() orelse error.InvalidArguments;
    }
    return stdx.cut_prefix(argument, option_name ++ "=");
}

// Return whether any section add/remove operation was requested.
fn cli_has_section_changes(cli: *const CLI) bool {
    for (cli.add_sections) |add| if (add != null) return true;
    for (cli.remove_sections) |remove| if (remove) return true;
    return false;
}

// Parse one `--add-section` entry and store it by logical section id.
fn parse_add_section(cli: *CLI, value: []const u8) !void {
    const section_name, const path = stdx.cut(value, "=") orelse return error.InvalidArguments;
    const section = section_parse(section_name) orelse return error.InvalidArguments;
    if (path.len == 0) return error.InvalidArguments;

    const index = @intFromEnum(section);
    cli.add_sections[index] = .{ .section = section, .path = path };
}

// Parse one `--remove-section` entry.
fn parse_remove_section(cli: *CLI, value: []const u8) !void {
    const section = section_parse(value) orelse return error.InvalidArguments;
    cli.remove_sections[@intFromEnum(section)] = true;
}

// Accept only the exact section flags combination used by TigerBeetle builds.
fn validate_section_flags(value: []const u8) !void {
    const section_name, const flags = stdx.cut(value, "=") orelse return error.InvalidArguments;
    const section = section_parse(section_name) orelse return error.InvalidArguments;
    _ = section;
    if (!std.mem.eql(u8, flags, "contents,noload,readonly")) return error.InvalidArguments;
}

// Map known section names into the internal section enum.
fn section_parse(name: []const u8) ?Section {
    if (std.mem.eql(u8, name, ".tb_mvb")) return .tb_mvb;
    if (std.mem.eql(u8, name, ".tb_mvh")) return .tb_mvh;
    return null;
}

// Check ELF magic prefix.
fn is_elf(bytes: []const u8) bool {
    return bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], "\x7fELF");
}

// Check DOS/PE magic prefix.
fn is_pe(bytes: []const u8) bool {
    return bytes.len >= 2 and std.mem.eql(u8, bytes[0..2], "MZ");
}

const ElfSection = struct {
    header: elf.Elf64_Shdr,
    name: []const u8,
    data: []const u8,
    data_owned: bool,
    old_offset: usize,
    old_size: usize,
    added: bool,
};

// Apply the requested section edits to an ELF input and emit rewritten bytes.
fn transform_elf(gpa: std.mem.Allocator, input: []const u8, cli: *const CLI) ![]u8 {
    if (input.len < @sizeOf(elf.Elf64_Ehdr)) return error.InvalidELF;

    var elf_header = std.mem.bytesAsValue(elf.Elf64_Ehdr, input[0..@sizeOf(elf.Elf64_Ehdr)]).*;
    if (!std.mem.eql(u8, elf_header.e_ident[0..4], elf.MAGIC)) return error.InvalidELF;
    if (elf_header.e_ident[elf.EI_CLASS] != elf.ELFCLASS64) return error.UnsupportedELF;
    if (elf_header.e_ident[elf.EI_DATA] != elf.ELFDATA2LSB) return error.UnsupportedELF;
    if (elf_header.e_shentsize != @sizeOf(elf.Elf64_Shdr)) return error.InvalidELF;

    const section_headers_offset: usize = @intCast(elf_header.e_shoff);
    const section_headers_count: usize = elf_header.e_shnum;
    if (section_headers_offset + section_headers_count * @sizeOf(elf.Elf64_Shdr) > input.len) {
        return error.InvalidELF;
    }

    if (elf_header.e_shstrndx >= section_headers_count) return error.InvalidELF;
    const section_name_table_header = std.mem.bytesAsValue(
        elf.Elf64_Shdr,
        input[section_headers_offset + elf_header.e_shstrndx * @sizeOf(elf.Elf64_Shdr) ..][0..@sizeOf(
            elf.Elf64_Shdr,
        )],
    ).*;
    const section_name_table_offset: usize = @intCast(section_name_table_header.sh_offset);
    const section_name_table_size: usize = @intCast(section_name_table_header.sh_size);
    if (section_name_table_offset + section_name_table_size > input.len) return error.InvalidELF;
    const section_name_table = input[section_name_table_offset..][0..section_name_table_size];

    var sections, const section_data_end_max_original = try elf_collect_sections(gpa, input, cli, .{
        .section_headers_offset = section_headers_offset,
        .section_headers_count = section_headers_count,
        .section_name_table = section_name_table,
    });
    defer sections.deinit();
    defer for (sections.items) |section| {
        if (section.name.len > 0) gpa.free(section.name);
        if (section.data_owned) gpa.free(section.data);
    };

    try elf_apply_add_sections(gpa, &sections, cli, section_data_end_max_original);
    const section_name_table_index = try elf_apply_string_table(gpa, &sections);

    // Keep existing section data offsets as stable as possible and shift only the tail.
    const section_data_end_max = elf_relayout_sections(sections.items);

    const section_headers_offset_new = std.mem.alignForward(usize, section_data_end_max, 8);
    elf_header.e_shoff = section_headers_offset_new;
    elf_header.e_shnum = @intCast(sections.items.len);
    elf_header.e_shstrndx = @intCast(section_name_table_index);

    return try elf_write_output(gpa, input, elf_header, sections.items, section_data_end_max);
}

// Read ELF section headers/data, filtering removed sections.
fn elf_collect_sections(
    gpa: std.mem.Allocator,
    input: []const u8,
    cli: *const CLI,
    options: struct {
        section_headers_offset: usize,
        section_headers_count: usize,
        section_name_table: []const u8,
    },
) !struct { std.ArrayList(ElfSection), usize } {
    var sections = std.ArrayList(ElfSection).init(gpa);
    var data_end_max: usize = 0;

    for (0..options.section_headers_count) |i| {
        const shdr = std.mem.bytesAsValue(
            elf.Elf64_Shdr,
            input[options.section_headers_offset + i * @sizeOf(elf.Elf64_Shdr) ..][0..@sizeOf(
                elf.Elf64_Shdr,
            )],
        ).*;
        if (shdr.sh_name >= options.section_name_table.len) return error.InvalidELF;
        const name = std.mem.sliceTo(
            @as([*:0]const u8, @ptrCast(options.section_name_table.ptr + shdr.sh_name)),
            0,
        );
        if (section_parse(name)) |section| {
            if (cli.remove_sections[@intFromEnum(section)]) continue;
        }

        const data = blk: {
            if (shdr.sh_type == elf.SHT_NOBITS or shdr.sh_size == 0) break :blk "";
            const offset: usize = @intCast(shdr.sh_offset);
            const size: usize = @intCast(shdr.sh_size);
            if (offset + size > input.len) return error.InvalidELF;
            break :blk input[offset..][0..size];
        };
        const name_owned = if (name.len > 0) try gpa.dupe(u8, name) else "";
        try sections.append(.{
            .header = shdr,
            .name = name_owned,
            .data = data,
            .data_owned = false,
            .old_offset = @intCast(shdr.sh_offset),
            .old_size = data.len,
            .added = false,
        });
        if (shdr.sh_type != elf.SHT_NOBITS and data.len > 0) {
            data_end_max = @max(data_end_max, @as(usize, @intCast(shdr.sh_offset)) + data.len);
        }
    }

    return .{ sections, data_end_max };
}

// Append requested ELF sections, replacing same-name sections when present.
fn elf_apply_add_sections(
    gpa: std.mem.Allocator,
    sections: *std.ArrayList(ElfSection),
    cli: *const CLI,
    data_end_max: usize,
) !void {
    for (cli.add_sections) |maybe_add| {
        if (maybe_add == null) continue;
        const add = maybe_add.?;
        // Replace existing section (if any) with same name to match `remove + add` behavior.
        var existing_index: ?usize = null;
        for (sections.items, 0..) |section, i| {
            if (std.mem.eql(u8, section.name, add.section.name())) {
                existing_index = i;
                break;
            }
        }
        if (existing_index) |index| {
            const removed = sections.orderedRemove(index);
            if (removed.name.len > 0) gpa.free(removed.name);
            if (removed.data_owned) gpa.free(removed.data);
        }

        try sections.append(.{
            .header = .{
                .sh_name = 0,
                .sh_type = elf.SHT_PROGBITS,
                .sh_flags = 0,
                .sh_addr = 0,
                .sh_offset = 0,
                .sh_size = @intCast(add.bytes.len),
                .sh_link = 0,
                .sh_info = 0,
                .sh_addralign = 1,
                .sh_entsize = 0,
            },
            .name = try gpa.dupe(u8, add.section.name()),
            .data = add.bytes,
            .data_owned = false,
            .old_offset = data_end_max,
            .old_size = 0,
            .added = true,
        });
    }
}

// Rebuild `.shstrtab` and patch each section header `sh_name`.
fn elf_apply_string_table(
    gpa: std.mem.Allocator,
    sections: *std.ArrayList(ElfSection),
) !usize {
    var shstr_index: ?usize = null;
    for (sections.items, 0..) |section, i| {
        if (std.mem.eql(u8, section.name, ".shstrtab")) {
            shstr_index = i;
            break;
        }
    }
    if (shstr_index == null) return error.InvalidELF;

    // LLVM sorts and suffix-deduplicates names before materializing `.shstrtab`.
    const name_offsets, const shstr_bytes = try elf_build_string_table(gpa, sections.items);
    defer gpa.free(name_offsets);

    for (sections.items, 0..) |*section, i| {
        section.header.sh_name = name_offsets[i];
    }
    // Ownership is transferred to the section list and used by the final writer.
    sections.items[shstr_index.?].data = shstr_bytes;
    sections.items[shstr_index.?].data_owned = true;
    sections.items[shstr_index.?].header.sh_size = shstr_bytes.len;

    return shstr_index.?;
}

// Recompute section file offsets after add/remove/replace operations.
fn elf_relayout_sections(sections: []ElfSection) usize {
    var delta: i64 = 0;
    var max_data_end: usize = 0;
    for (sections) |*section| {
        if (section.header.sh_type == elf.SHT_NOBITS or section.header.sh_size == 0) continue;
        const section_align = if (section.header.sh_addralign == 0)
            1
        else
            section.header.sh_addralign;
        const base: i64 = @as(i64, @intCast(section.old_offset)) + delta;
        const aligned: usize = std.mem.alignForward(
            usize,
            @intCast(base),
            @intCast(section_align),
        );
        const aligned_delta: i64 = @as(i64, @intCast(aligned)) - base;
        delta += aligned_delta;
        section.header.sh_offset = aligned;

        const old_size: i64 = @intCast(section.old_size);
        const new_size: i64 = @intCast(section.data.len);
        delta += new_size - old_size;
        section.old_offset = aligned;
        max_data_end = @max(max_data_end, aligned + section.data.len);
    }
    return max_data_end;
}

// Materialize final ELF bytes including data payloads and the section table.
fn elf_write_output(
    gpa: std.mem.Allocator,
    input: []const u8,
    elf_header: elf.Elf64_Ehdr,
    sections: []const ElfSection,
    max_data_end: usize,
) ![]u8 {
    const section_headers_offset_new: usize = @intCast(elf_header.e_shoff);

    const output_size = section_headers_offset_new + sections.len * @sizeOf(elf.Elf64_Shdr);
    var output = try gpa.alloc(u8, output_size);
    @memset(output, 0);

    stdx.copy_disjoint(
        .exact,
        u8,
        output[0..@min(output.len, input.len)],
        input[0..@min(output.len, input.len)],
    );
    stdx.copy_disjoint(
        .exact,
        u8,
        output[0..@sizeOf(elf.Elf64_Ehdr)],
        std.mem.asBytes(&elf_header),
    );

    for (sections) |section| {
        if (section.header.sh_type == elf.SHT_NOBITS or section.header.sh_size == 0) continue;
        const section_data_offset: usize = @intCast(section.header.sh_offset);
        if (section_data_offset + section.data.len > output.len) return error.InvalidELF;
        stdx.copy_disjoint(
            .exact,
            u8,
            output[section_data_offset..][0..section.data.len],
            section.data,
        );
    }
    if (max_data_end < section_headers_offset_new) {
        @memset(output[max_data_end..section_headers_offset_new], 0);
    }

    for (sections, 0..) |section, i| {
        const section_header_offset = section_headers_offset_new + i * @sizeOf(elf.Elf64_Shdr);
        stdx.copy_disjoint(
            .exact,
            u8,
            output[section_header_offset..][0..@sizeOf(elf.Elf64_Shdr)],
            std.mem.asBytes(&section.header),
        );
    }

    return output;
}

// Build LLVM-compatible `.shstrtab` bytes and per-section name offsets.
fn elf_build_string_table(
    gpa: std.mem.Allocator,
    sections: []const ElfSection,
) !struct { []u32, []u8 } {
    var unique_names = std.ArrayList([]const u8).init(gpa);
    defer unique_names.deinit();

    var offset_by_name = std.StringHashMap(u32).init(gpa);
    defer offset_by_name.deinit();

    try unique_names.append("");
    try offset_by_name.put("", 0);
    for (sections) |section| {
        if (offset_by_name.get(section.name) == null) {
            try unique_names.append(section.name);
            try offset_by_name.put(section.name, 0);
        }
    }

    std.sort.heap([]const u8, unique_names.items, {}, struct {
        fn char_tail_at(s: []const u8, pos: usize) i16 {
            if (pos >= s.len) return -1;
            return @intCast(s[s.len - 1 - pos]);
        }

        fn less_than(_: void, a: []const u8, b: []const u8) bool {
            var pos: usize = 0;
            while (true) : (pos += 1) {
                const ca = char_tail_at(a, pos);
                const cb = char_tail_at(b, pos);
                if (ca == cb) {
                    if (ca == -1) return false;
                    continue;
                }
                // Same ordering as LLVM's multikeySort partition `C > Pivot`.
                return ca > cb;
            }
        }
    }.less_than);

    var size: usize = 1;
    var previous: []const u8 = "";
    for (unique_names.items) |name| {
        if (std.mem.endsWith(u8, previous, name)) {
            const pos = size - name.len - 1;
            try offset_by_name.put(name, @intCast(pos));
            continue;
        }
        const offset: u32 = @intCast(size);
        try offset_by_name.put(name, offset);
        size += name.len + 1;
        previous = name;
    }
    try offset_by_name.put("", 0);

    var table = try gpa.alloc(u8, size);
    @memset(table, 0);
    var it = offset_by_name.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        const offset = entry.value_ptr.*;
        if (name.len == 0) continue;
        stdx.copy_disjoint(.exact, u8, table[offset..][0..name.len], name);
    }

    var offsets = try gpa.alloc(u32, sections.len);
    for (sections, 0..) |section, i| {
        offsets[i] = offset_by_name.get(section.name).?;
    }

    return .{ offsets, table };
}

const PESection = struct {
    name: [8]u8,
    virtual_size: u32,
    virtual_address: u32,
    size_of_raw_data: u32,
    pointer_to_raw_data: u32,
    pointer_to_relocations: u32,
    pointer_to_linenumbers: u32,
    number_of_relocations: u16,
    number_of_linenumbers: u16,
    characteristics: u32,
    data: []const u8,
    added: bool,
};

// Apply the requested section edits to a PE/COFF input and emit rewritten bytes.
fn transform_pe(gpa: std.mem.Allocator, input: []const u8, cli: *const CLI) ![]u8 {
    if (input.len < 0x40) return error.InvalidPE;
    const pe_header_offset = std.mem.readInt(u32, input[0x3c..][0..4], .little);
    if (pe_header_offset + 4 + 20 > input.len) return error.InvalidPE;
    if (!std.mem.eql(u8, input[pe_header_offset..][0..4], "PE\x00\x00")) return error.InvalidPE;

    const coff_offset = pe_header_offset + 4;
    const section_count = std.mem.readInt(u16, input[coff_offset + 2 ..][0..2], .little);
    const optional_header_size = std.mem.readInt(u16, input[coff_offset + 16 ..][0..2], .little);
    const optional_header_offset = coff_offset + 20;
    const section_table_offset = optional_header_offset + optional_header_size;
    const section_header_size = 40;
    if (section_table_offset + section_count * section_header_size > input.len) {
        return error.InvalidPE;
    }

    const section_alignment = std.mem.readInt(
        u32,
        input[optional_header_offset + 32 ..][0..4],
        .little,
    );
    const file_alignment = std.mem.readInt(
        u32,
        input[optional_header_offset + 36 ..][0..4],
        .little,
    );
    const size_of_headers = std.mem.readInt(
        u32,
        input[optional_header_offset + 60 ..][0..4],
        .little,
    );
    if (section_alignment == 0 or file_alignment == 0) return error.InvalidPE;

    var sections = try pe_collect_sections(gpa, input, cli, .{
        .section_table_offset = section_table_offset,
        .section_header_size = section_header_size,
        .section_count = section_count,
    });
    defer sections.deinit();

    try pe_apply_add_sections(&sections, cli);
    const next_raw, const next_va = pe_layout_sections(
        sections.items,
        file_alignment,
        section_alignment,
    );

    const new_section_count: u16 = @intCast(sections.items.len);
    const new_size_of_image = std.mem.alignForward(u32, next_va, section_alignment);
    const output_size: usize = @intCast(next_raw);

    var output = try gpa.alloc(u8, output_size);
    @memset(output, 0);
    // LLVM only preserves the PE header region, not stale data from removed section payloads.
    const headers_copy_len: usize = @min(@min(output.len, input.len), size_of_headers);
    stdx.copy_disjoint(.exact, u8, output[0..headers_copy_len], input[0..headers_copy_len]);

    std.mem.writeInt(u16, output[coff_offset + 2 ..][0..2], new_section_count, .little);
    std.mem.writeInt(u32, output[optional_header_offset + 56 ..][0..4], new_size_of_image, .little);

    for (sections.items, 0..) |section, i| {
        const section_header_offset = section_table_offset + i * section_header_size;
        if (section_header_offset + section_header_size > output.len) return error.InvalidPE;
        stdx.copy_disjoint(.exact, u8, output[section_header_offset..][0..8], &section.name);

        inline for ([_]struct { field_offset: usize, field_value: u32 }{
            .{ .field_offset = 8, .field_value = section.virtual_size },
            .{ .field_offset = 12, .field_value = section.virtual_address },
            .{ .field_offset = 16, .field_value = section.size_of_raw_data },
            .{ .field_offset = 20, .field_value = section.pointer_to_raw_data },
            .{ .field_offset = 24, .field_value = section.pointer_to_relocations },
            .{ .field_offset = 28, .field_value = section.pointer_to_linenumbers },
            .{ .field_offset = 36, .field_value = section.characteristics },
        }) |field| {
            std.mem.writeInt(
                u32,
                output[section_header_offset + field.field_offset ..][0..4],
                field.field_value,
                .little,
            );
        }

        inline for ([_]struct { field_offset: usize, field_value: u16 }{
            .{ .field_offset = 32, .field_value = section.number_of_relocations },
            .{ .field_offset = 34, .field_value = section.number_of_linenumbers },
        }) |field| {
            std.mem.writeInt(
                u16,
                output[section_header_offset + field.field_offset ..][0..2],
                field.field_value,
                .little,
            );
        }
    }
    const old_section_table_end = section_table_offset + section_count * section_header_size;
    const new_section_table_end = section_table_offset + sections.items.len * section_header_size;
    if (new_section_table_end < old_section_table_end and old_section_table_end <= output.len) {
        @memset(output[new_section_table_end..old_section_table_end], 0);
    }

    for (sections.items) |section| {
        if (section.size_of_raw_data == 0) continue;
        const raw_ptr: usize = @intCast(section.pointer_to_raw_data);
        if (raw_ptr + section.data.len > output.len) return error.InvalidPE;
        stdx.copy_disjoint(.exact, u8, output[raw_ptr..][0..section.data.len], section.data);
    }

    assert(output.len == output_size);
    return output;
}

// Read PE section headers/data, filtering removed sections.
fn pe_collect_sections(
    gpa: std.mem.Allocator,
    input: []const u8,
    cli: *const CLI,
    options: struct {
        section_table_offset: usize,
        section_header_size: usize,
        section_count: usize,
    },
) !std.ArrayList(PESection) {
    var sections = std.ArrayList(PESection).init(gpa);

    for (0..options.section_count) |i| {
        const section_header_offset = options.section_table_offset + i * options.section_header_size;
        var name: [8]u8 = undefined;
        stdx.copy_disjoint(.exact, u8, &name, input[section_header_offset..][0..8]);

        const section_name = std.mem.sliceTo(&name, 0);
        if (section_parse(section_name)) |section| {
            if (cli.remove_sections[@intFromEnum(section)]) continue;
        }

        const raw_size = std.mem.readInt(u32, input[section_header_offset + 16 ..][0..4], .little);
        const raw_ptr = std.mem.readInt(u32, input[section_header_offset + 20 ..][0..4], .little);
        const data = blk: {
            if (raw_size == 0) break :blk "";
            if (raw_ptr + raw_size > input.len) return error.InvalidPE;
            break :blk input[raw_ptr..][0..raw_size];
        };

        var section: PESection = .{
            .name = name,
            .virtual_size = 0,
            .virtual_address = 0,
            .size_of_raw_data = raw_size,
            .pointer_to_raw_data = raw_ptr,
            .pointer_to_relocations = 0,
            .pointer_to_linenumbers = 0,
            .number_of_relocations = 0,
            .number_of_linenumbers = 0,
            .characteristics = 0,
            .data = data,
            .added = false,
        };
        inline for ([_]struct { field_name: []const u8, field_offset: usize }{
            .{ .field_name = "virtual_size", .field_offset = 8 },
            .{ .field_name = "virtual_address", .field_offset = 12 },
            .{ .field_name = "pointer_to_relocations", .field_offset = 24 },
            .{ .field_name = "pointer_to_linenumbers", .field_offset = 28 },
            .{ .field_name = "characteristics", .field_offset = 36 },
        }) |field| {
            @field(section, field.field_name) = std.mem.readInt(
                u32,
                input[section_header_offset + field.field_offset ..][0..4],
                .little,
            );
        }
        inline for ([_]struct { field_name: []const u8, field_offset: usize }{
            .{ .field_name = "number_of_relocations", .field_offset = 32 },
            .{ .field_name = "number_of_linenumbers", .field_offset = 34 },
        }) |field| {
            @field(section, field.field_name) = std.mem.readInt(
                u16,
                input[section_header_offset + field.field_offset ..][0..2],
                .little,
            );
        }
        try sections.append(section);
    }
    return sections;
}

// Append requested PE sections with TigerBeetle's expected section attributes.
fn pe_apply_add_sections(sections: *std.ArrayList(PESection), cli: *const CLI) !void {
    for (cli.add_sections) |maybe_add| {
        if (maybe_add == null) continue;
        const add = maybe_add.?;
        var name: [8]u8 = @splat(0);
        stdx.copy_disjoint(.exact, u8, name[0..add.section.name().len], add.section.name());
        try sections.append(.{
            .name = name,
            .virtual_size = @intCast(add.bytes.len),
            .virtual_address = 0,
            .size_of_raw_data = 0,
            .pointer_to_raw_data = 0,
            .pointer_to_relocations = 0,
            .pointer_to_linenumbers = 0,
            .number_of_relocations = 0,
            .number_of_linenumbers = 0,
            .characteristics = 0x40000800, // MEM_READ | LNK_REMOVE
            .data = add.bytes,
            .added = true,
        });
    }
}

// Compute raw/file and virtual layout for all PE sections.
fn pe_layout_sections(
    sections: []PESection,
    file_alignment: u32,
    section_alignment: u32,
) struct { u32, u32 } {
    var next_raw: u32 = 0;
    var next_va: u32 = 0;
    for (sections) |*section| {
        if (section.added) {
            section.size_of_raw_data = @intCast(std.mem.alignForward(
                usize,
                section.data.len,
                file_alignment,
            ));
            section.pointer_to_raw_data = std.mem.alignForward(u32, next_raw, file_alignment);
            section.virtual_address = std.mem.alignForward(u32, next_va, section_alignment);
        }
        next_raw = section.pointer_to_raw_data + section.size_of_raw_data;
        next_va = section.virtual_address + std.mem.alignForward(
            u32,
            @max(section.virtual_size, section.size_of_raw_data),
            section_alignment,
        );
    }
    return .{ next_raw, next_va };
}
