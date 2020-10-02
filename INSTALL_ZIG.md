# Install Zig

First, [download the Zig master binary](https://ziglang.org/download/) for your platform.

Then:

```bash
tar -xf zig-*.tar.xz
rm zig-*.tar.xz
sudo rm -rf /usr/local/lib/zig
sudo mv zig-* /usr/local/lib/zig
sudo ln -s --force /usr/local/lib/zig/zig /usr/local/bin/zig
zig version
```
