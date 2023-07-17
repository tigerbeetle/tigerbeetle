const Big = struct {
    ballast: [4096]u8 = undefined,
    small: Small,
};

const Small = struct {
    ballast: [128]u8 = undefined,
};

export fn foo(xs_ptr: [*]const Big, xs_len: usize) callconv(.C) void {
    const xs: []const Big = xs_ptr[0..xs_len];
    for (xs) |x| {
        bar(&x.small);
    }
}

noinline fn bar(x: *const Small) void {
    _ = x;
}
