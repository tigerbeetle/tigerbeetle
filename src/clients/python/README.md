Still extremely early days...

```
(cd ../../.. && zig build c_client)
python client_build.py
LD_LIBRARY_PATH="../c/lib/x86_64-linux-gnu/" python client.py
```