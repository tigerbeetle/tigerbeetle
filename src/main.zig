const Foo = struct {
    fn foo(me: Foo) void {
        _ = me;
    }
    const bar = struct {
        fn f(me: Foo) void {
            _ = me;
        }
    }.f;
};

pub fn main() void {
    const foo: Foo = .{};
    foo.foo();
    foo.bar();
}
