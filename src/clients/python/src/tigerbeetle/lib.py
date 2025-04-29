import ctypes
import dataclasses
import platform
import sys
import os

from pathlib import Path


class NativeError(Exception):
    pass


class IntegerOverflowError(ValueError):
    pass


def _python_tbclient_prefix():
    arch = ""
    system = ""
    linux_libc = ""

    platform_machine = platform.machine().lower()

    if platform_machine == "x86_64" or platform_machine == "amd64":
        arch = "x86_64"
    elif platform_machine == "aarch64" or platform_machine == "arm64":
        arch = "aarch64"
    else:
        raise NativeError("Unsupported machine: " + platform.machine())

    if platform.system() == "Linux":
        system = "linux"
        libc = platform.libc_ver()[0]
        if libc == "glibc":
            linux_libc = "-gnu.2.27"
        elif libc == "musl":
            linux_libc = "-musl"
        else:
            raise NativeError("Unsupported libc: " + libc)
    elif platform.system() == "Darwin":
        system = "macos"
    elif platform.system() == "Windows":
        system = "windows"
    else:
        raise NativeError("Unsupported system: " + platform.system())

    source_path = Path(__file__)
    source_dir = source_path.parent
    library_path = source_dir / "lib" / f"{arch}-{system}{linux_libc}"

    print("Importing from:", library_path)
    print(os.listdir(library_path))

    return str(library_path)


class IntegerOverflowError(ValueError):
    pass


def validate_uint(*, bits: int, name: str, number: int):
    if number > 2**bits - 1:
        raise IntegerOverflowError(f"{name}=={number} is too large to fit in {bits} bits")
    if number < 0:
        raise IntegerOverflowError(f"{name}=={number} cannot be negative")


class c_uint128(ctypes.Structure): # noqa: N801
    _fields_ = [("_low", ctypes.c_uint64), ("_high", ctypes.c_uint64)] # noqa: RUF012

    @classmethod
    def from_param(cls, obj):
        return cls(_high=obj >> 64, _low=obj & 0xffffffffffffffff)

    def to_python(self):
        return self._high << 64 | self._low


# Use slots=True if the version of Python is new enough (3.10+) to support it.
try:
    dataclass = dataclasses.dataclass(slots=True)
except TypeError:
    dataclass = dataclasses.dataclass()

def tb_assert(value):
    """
    Python's built-in assert can be silently disabled if Python is run with -O.
    """
    if not value:
        raise AssertionError()

sys.path.insert(0, _python_tbclient_prefix())

if platform.system() == "Windows":
    import importlib.util
    spec = importlib.util.spec_from_file_location("libtb_pythonclient", Path(_python_tbclient_prefix()) / "libtb_pythonclient.abi3.pyd")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

try:
    print("trying import from")
    print(sys.path)
    import libtb_pythonclient
finally:
    sys.path.pop(0)


# This is a little bit unorthodox: the same shared library is used both as a CPython extension,
# imported directly, _and_ via ctypes.
tbclient = ctypes.CDLL(libtb_pythonclient.__file__)

encode = libtb_pythonclient.encode
decode = libtb_pythonclient.decode
id = libtb_pythonclient.id