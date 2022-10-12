# `@bitCast` deserialization demo

This is a simple demo to get you comfortable building TigerBeetle and show how
low-level code can be easier than high-level code.

## Preliminaries

```shell
git clone https://github.com/coilhq/tigerbeetle
cd tigerbeetle
scripts/install_zig.sh
```

Now, add the binary at `zig/zig` to your path.

## Compile and run decode.zig vs decode.js

```shell
cd demos/bitcast
zig build-exe decode.zig -O ReleaseSafe
time ./decode # Do a throwaway run in case macOS adds SIP latency.
time ./decode
time node decode.js
```

## Compare decode.js and decode.zig

Both files do the same thing, with almost the same syntax, except one is faster
and safer with less code, thanks to first-class pointers, packed structs, types,
and an explicit bit cast.

The JavaScript version has to deserialize each and every field of the struct
with multiple function calls, instantiate a JavaScript object (with associated
GC) and copy all field values. Lots of little memory copies. Death by a thousand
cuts. We want to process a few million events a second and you can see the
unnecessary overhead that JavaScript is forcing simply for lack of language
features.

**The Zig version simply does a bit cast, in one line of code, to point a struct
to existing memory, without any deserialization or function calls.** This is
also better than JSON. Imagine you could point your JSON parser to a piece of
memory and auto-hydrate your JSON object without doing any parsing.

This bit cast in itself is special, and one wouldn't normally feel comfortable
doing the same thing in C.

In C it's "mostly safe"
([as Miracle Max would say](https://www.youtube.com/watch?v=d4ftmOI5NnI)) to
hydrate structs from network bytes by casting a buffer to a struct
(no deserialization overhead!)... but you have to pack structs carefully by hand
to control alignment, field ordering and padding, otherwise the compiler may
reorder your struct fields or add padding and your struct may end up larger than
your network bytes and corrupt data. See
[The Lost Art of Structure Packing](http://www.catb.org/esr/structure-packing)
for a great explanation.

However, in Zig our struct is not only carefully ordered to work even in C, but
it is also explicitly annotated as "packed". It's not just that this Zig
struct behaves the same way as an eccentric C struct, like you can do in other
languages, but we can further guarantee that the cast from 128 bytes to our
struct is going to be safe on a little-endian machine, because we know that's
exactly how our 128 byte struct is laid out in memory. (You can do the same in C
with arcane compiler-specific attributes)

The `@bitCast()` is also going to help us more than a C pointer cast would... by
asserting that the size of the two operands is the same, which it does at
compile time with no runtime overhead... try changing
`var bytes: [@sizeOf(Transfer)]u8` to `var bytes: [64]u8` and then re-compile.

Coming from C, this is fun.

Coming from JavaScript, this is less code.

## What's the performance impact?

Deserializing **16,384 transfers** on each language:

Roughly, deserializing 16,384 transfers takes 5ms to 20ms in JavaScript and 0.03ms in
Zig:

|Language| Avg time(ms) |
|--------|--------------|
|Zig     |          0.03|
|Go      |          0.03|
|C#      |          0.05|
|Java    |          0.85|
|JS      |          5.00|

To put this in perspective, the cost of deserializing a batch of transfers in
JavaScript can be more than the fsync latency for writing the same data to an
HDD, let alone an SSD or NVMe SSD.

### Running

Before running, create a mass of data used by all benchmarks:

```shell
node bench-encode.js
```

**Zig:**

```shell
zig build-exe bench-decode.zig -O ReleaseSafe && ./bench-decode
```

**Go:**

```shell
go run bench-decode.go
```

**C#**

```shell
dotnet run --project bench-decode.csproj -c Release
```

**Java:**

```shell
java BenchDecode.java
```

**JavaScript:**

```shell
node bench-decode.js
```

