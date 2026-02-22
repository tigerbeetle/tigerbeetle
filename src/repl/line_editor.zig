const std = @import("std");
const stdx = @import("../vsr.zig").stdx;

const ReplBufferBoundedArray = stdx.BoundedArrayType(u8, 511);
const buffer_capacity = 511;

pub const LineEditor = struct {
    buffer: ReplBufferBoundedArray = .{},
    cursor: u32 = 0,

    pub fn clear(self: *LineEditor) void {
        self.buffer.clear();
        self.cursor = 0;
    }

    pub fn set_content(self: *LineEditor, content: []const u8) !void {
        if (content.len > buffer_capacity) return error.BufferOverflow;
        self.buffer.clear();
        self.buffer.push_slice(content);
        self.cursor = @intCast(try std.unicode.utf8CountCodepoints(content));
    }

    pub fn const_slice(self: *const LineEditor) []const u8 {
        return self.buffer.const_slice();
    }

    pub fn get_cursor_byte_offset(self: *const LineEditor) usize {
        return self.codepoint_index_to_byte_offset(self.cursor);
    }

    pub fn insert(self: *LineEditor, char: u8) void {
        if (self.buffer.count() >= buffer_capacity) return;
        const byte_offset = self.get_cursor_byte_offset();
        self.buffer.insert_at(byte_offset, char);
        self.cursor += 1;
    }

    pub fn backspace(self: *LineEditor) void {
        if (self.cursor == 0) return;
        const end = self.codepoint_index_to_byte_offset(self.cursor);
        const start = self.codepoint_index_to_byte_offset(self.cursor - 1);
        var i: usize = 0;
        while (i < end - start) : (i += 1) _ = self.buffer.ordered_remove(start);
        self.cursor -= 1;
    }

    pub fn delete(self: *LineEditor) void {
        const count = std.unicode.utf8CountCodepoints(self.buffer.const_slice()) catch 0;
        if (self.cursor >= count) return;
        const start = self.codepoint_index_to_byte_offset(self.cursor);
        const end = self.codepoint_index_to_byte_offset(self.cursor + 1);
        var i: usize = 0;
        while (i < end - start) : (i += 1) _ = self.buffer.ordered_remove(start);
    }

    pub fn move_left(self: *LineEditor) void {
        if (self.cursor > 0) self.cursor -= 1;
    }

    pub fn move_right(self: *LineEditor) void {
        const count = std.unicode.utf8CountCodepoints(self.buffer.const_slice()) catch 0;
        if (self.cursor < count) self.cursor += 1;
    }

    pub fn move_home(self: *LineEditor) void {
        self.cursor = 0;
    }

    pub fn move_end(self: *LineEditor) void {
        self.cursor = @intCast((std.unicode.utf8CountCodepoints(self.buffer.const_slice()) catch 0));
    }

    pub fn move_backward_by_word(self: *LineEditor) void {
        const buffer = self.buffer.const_slice();
        var position = self.get_cursor_byte_offset();
        while (position > 0 and std.ascii.isWhitespace(buffer[position - 1])) position -= 1;
        while (position > 0 and !std.ascii.isWhitespace(buffer[position - 1])) position -= 1;
        self.cursor = self.byte_offset_to_codepoint_index(position);
    }

    pub fn move_forward_by_word(self: *LineEditor) void {
        const buffer = self.buffer.const_slice();
        var position = self.get_cursor_byte_offset();
        while (position < buffer.len and !std.ascii.isWhitespace(buffer[position])) position += 1;
        while (position < buffer.len and std.ascii.isWhitespace(buffer[position])) position += 1;
        self.cursor = self.byte_offset_to_codepoint_index(position);
    }

    fn codepoint_index_to_byte_offset(self: *const LineEditor, index: u32) usize {
        var byte_offset: usize = 0;
        var count: u32 = 0;
        var iterator = std.unicode.Utf8Iterator{ .bytes = self.buffer.const_slice(), .i = 0 };
        while (iterator.nextCodepointSlice()) |_| {
            if (count == index) break;
            byte_offset = iterator.i;
            count += 1;
        }
        return byte_offset;
    }

    fn byte_offset_to_codepoint_index(self: *const LineEditor, byte_offset: usize) u32 {
        var iterator = std.unicode.Utf8Iterator{ .bytes = self.buffer.const_slice(), .i = 0 };
        var count: u32 = 0;
        while (iterator.nextCodepointSlice()) |_| {
            if (iterator.i > byte_offset) break;
            count += 1;
        }
        return count;
    }
};

test "line_editor: insert and cursor movement" {
    var editor: LineEditor = .{};
    editor.insert('a');
    try std.testing.expectEqualStrings("a", editor.const_slice());
    try std.testing.expectEqual(@as(u32, 1), editor.cursor);

    editor.insert('b');
    try std.testing.expectEqualStrings("ab", editor.const_slice());
    try std.testing.expectEqual(@as(u32, 2), editor.cursor);

    editor.move_left();
    try std.testing.expectEqual(@as(u32, 1), editor.cursor);

    editor.insert('c');
    try std.testing.expectEqualStrings("acb", editor.const_slice());
    try std.testing.expectEqual(@as(u32, 2), editor.cursor);

    editor.backspace();
    try std.testing.expectEqualStrings("ab", editor.const_slice());
    try std.testing.expectEqual(@as(u32, 1), editor.cursor);

    editor.delete();
    try std.testing.expectEqualStrings("a", editor.const_slice());
    try std.testing.expectEqual(@as(u32, 1), editor.cursor);
}

test "line_editor: UTF-8 multibyte" {
    var editor: LineEditor = .{};
    try editor.set_content("a");
    try std.testing.expectEqual(@as(u32, 1), editor.cursor);

    try editor.set_content("α");
    try std.testing.expectEqualStrings("α", editor.const_slice());
    try std.testing.expectEqual(@as(u32, 1), editor.cursor);

    editor.move_left();
    try std.testing.expectEqual(@as(u32, 0), editor.cursor);

    editor.move_right();
    try std.testing.expectEqual(@as(u32, 1), editor.cursor);
}

test "line_editor: history set content" {
    var editor: LineEditor = .{};
    try editor.set_content("hello world");
    try std.testing.expectEqualStrings("hello world", editor.const_slice());
    try std.testing.expectEqual(@as(u32, 11), editor.cursor);

    editor.move_home();
    try std.testing.expectEqual(@as(u32, 0), editor.cursor);

    editor.move_end();
    try std.testing.expectEqual(@as(u32, 11), editor.cursor);
}
