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
    address_permutations = []
    for address in addresses:
        address_permutations.append(",".join(itertools.repeat(address, 6)))
    address_permutations.append(",".join(addresses))

    for cluster_id in cluster_ids:
        for address_permutation in address_permutations:
            client = tb.ClientSync(cluster_id=cluster_id, replica_addresses=address_permutation)
            init_parameters_out = tb.InitParameters()

            status = tb.bindings.tb_client_init_parameters(
                client._client,
                ctypes.byref(init_parameters_out),
            )

            assert status == tb.ClientStatus.OK

            addresses_out_slice = ctypes.cast(init_parameters_out.addresses_ptr,
                ctypes.POINTER(ctypes.c_char * init_parameters_out.addresses_len))
            addresses_out = bytes(addresses_out_slice.contents).decode("ascii")

            assert init_parameters_out.client_id.to_python() != 0
            assert init_parameters_out.cluster_id.to_python() == cluster_id
            assert addresses_out == address_permutation

            client.close()
