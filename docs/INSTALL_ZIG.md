# Install Zig

First, [download the Zig master binary](https://ziglang.org/download/) for your platform.

Then:

```bash
rm -rf zig-cache
tar -xf zig-*.tar.xz
rm zig-*.tar.xz
sudo rm -rf /usr/local/lib/zig
sudo mv zig-* /usr/local/lib/zig
sudo ln -s --force /usr/local/lib/zig/zig /usr/local/bin/zig
zig version
```

You can also re-run the same steps above to update your Zig to latest master. Zig has a rapid release cadence at present and we are currently tracking master to keep pace.
