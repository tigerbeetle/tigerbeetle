// MIT License

// Copyright (c) 2020 Felix Queißner

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

const std = @import("std");
const network = @import("net_echo_server_blocking_network.zig");

// Simple TCP echo server:
// Accepts a single incoming connection and will echo any received data back to the
// client. Increasing the buffer size might improve throughput.

// using 1000 here yields roughly 54 MBit/s
// using 100_00 yields 150 MB/s
const buffer_size = 1000;

pub fn main() !void {
    try network.init();
    defer network.deinit();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = &gpa.allocator;

    var args_iter = std.process.args();
    const exe_name = try (args_iter.next(allocator) orelse return error.MissingArgument);
    defer allocator.free(exe_name);

    const port_name = try (args_iter.next(allocator) orelse return error.MissingArgument);
    defer allocator.free(port_name);

    const port_number = try std.fmt.parseInt(u16, port_name, 10);

    var sock = try network.Socket.create(.ipv4, .tcp);
    defer sock.close();

    try sock.enablePortReuse(true);
    try sock.bindToPort(port_number);
    try sock.listen();

    while (true) {
        var client = try sock.accept();
        defer client.close();

        std.debug.print("Client connected from {}.\n", .{
            try client.getLocalEndPoint(),
        });

        runEchoClient(client) catch |err| {
            std.debug.print("Client disconnected with msg {}.\n", .{
                @errorName(err),
            });
            continue;
        };
        std.debug.print("Client disconnected.\n", .{});
    }
}

fn runEchoClient(client: network.Socket) !void {
    while (true) {
        var buffer: [buffer_size]u8 = undefined;

        const len = try client.receive(&buffer);
        if (len == 0)
            break;
        // we ignore the amount of data sent.
        _ = try client.send(buffer[0..len]);
    }
}
