from cffi import FFI
ffibuilder = FFI()

ffibuilder.cdef(open("../c/tb_client.h").read() +"\n" + 'extern "Python" void on_completion_fn(uintptr_t, tb_client_t, tb_packet_t*, const uint8_t*, uint32_t);')

ffibuilder.set_source("_tb_client",
r"""
#include "../c/tb_client.h"
""", libraries=["tb_client"], library_dirs=["../c/lib/x86_64-linux-gnu/"])

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)