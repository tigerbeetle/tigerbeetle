---
sidebar_position: 6
---

## Why Is Tigerbeetle Written In Zig?

Coming from C, it's the language we've always wanted—OOM safety, bounds checking, rich choice of
allocators (and test allocators), checked arithmetic, explicit control flow, explicit control over
memory layout and alignment, no hidden allocations, no macros, first class C ABI interoperability,
and an insanely good compiler. Comptime is a game changer.

We realized that our roadmap would coincide with Zig's in terms of stability. We wanted to invest
for the next 20 years and didn't want to be stuck with C/C++ or compiler/language complexity and pay
a tax for the lifetime of the project.
