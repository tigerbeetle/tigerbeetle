import ctypes
import itertools

import tigerbeetle as tb
tb.configure_logging(debug=True)

cluster_ids = [
    2**8 - 1,
    2**16 - 1,
    2**32 - 1,
    2**64 - 1,
    2**128 - 1,
    71274155903562890452255960078140154531,
]

addresses = [
    "1.1.1.1",
    "1.1.1.1:3000",
    "1.1.1.1:40000",
    "127.127.127.127:12712",
    "[0000:0000:0000:0000:0000:ffff:c0a8:64e4]:65535",
    "[0000:0000:0000:0000:0000:ffff:c0a8:64e4]:65534",
]

def test_init_parameters():
    addresses_comma = []
    for address in addresses:
        addresses_comma.append(",".join(itertools.repeat(address, 6)))
    addresses_comma.append(",".join(addresses))

    for cluster_id in cluster_ids:
        for address_comma in addresses_comma:
            client = tb.ClientSync(cluster_id=cluster_id, replica_addresses=address_comma)

            cluster_id_out = tb.c_uint128()
            client_id_out = tb.c_uint128()
            addresses_ptr_out = ctypes.c_char_p()
            addresses_len_out = ctypes.c_uint64()

            tb.bindings.tb_client_init_parameters(
                client._client,
                ctypes.cast(
                    ctypes.byref(cluster_id_out), ctypes.POINTER(ctypes.c_uint8 * 16)
                ),
                ctypes.cast(
                    ctypes.byref(client_id_out), ctypes.POINTER(ctypes.c_uint8 * 16)
                ),
                ctypes.byref(addresses_ptr_out),
                ctypes.byref(addresses_len_out)
            )

            addresses_out = addresses_ptr_out.value[0:addresses_len_out.value].decode("ascii")

            assert cluster_id_out.to_python() == cluster_id
            assert addresses_out == address_comma

            client.close()
