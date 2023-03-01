from cffi import FFI
ffibuilder = FFI()

# For the cdef, we cheat by just pulling in our C header and removing the ifdefs and includes :)
c_header = [line for line in open("../c/tb_client.h").readlines() if not line.startswith("#")]

# Also, replace the definition for __uint128_t because cffi doesn't support it yet.
t_index = c_header.index("typedef __uint128_t tb_uint128_t;\n")
c_header[t_index] = "typedef struct tb_uint128_t {uint64_t high; uint64_t low;} tb_uint128_t;\n"

# Also, append the callback handler, only for the cdef though
ffibuilder.cdef("".join(c_header + ['extern "Python" void on_completion_fn(uintptr_t, tb_client_t, tb_packet_t*, const uint8_t*, uint32_t);\n']))

ffibuilder.set_source("_tb_client", "".join(c_header), libraries=["tb_client"], library_dirs=["../c/lib/x86_64-linux-gnu/"])

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)