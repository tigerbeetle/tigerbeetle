import ctypes
import threading
import uuid

from typing import List
from queue import Queue
import _tb_client

ffi = _tb_client.ffi
lib = _tb_client.lib

def generate_enums():
    enum_prefixes = ["TB_ACCOUNT", "TB_PACKET", "TB_OPERATION", "TB_CREATE_ACCOUNT", "TB_CREATE_TRANSFER", "TB_TRANSFER"]
    enums = {}
    enum_values = dir(lib)
    for prefix in enum_prefixes:
        if prefix not in enums:
            enums[prefix] = {}
        for val in enum_values:
            if val.startswith(prefix):
                enums[prefix][getattr(lib, val)] = val
    return enums

ENUMS = generate_enums()

@ffi.def_extern()
def on_completion_fn(context, client, packet, result_ptr, result_len):
    """
    Simple statically registered extern "Python" fn. This gets
    called for any callbacks, looks up the respective client from our
    global mapping, and forwards on the callback.

    NB: This runs in the Zig's client thread.
    """
    client = Client.completion_mapping[client]
    client._on_completion_fn(context, client, packet, result_ptr, result_len)

class TigerBeetleInitException(Exception):
    pass

class Client:
    completion_mapping = {}

    def __init__(self, cluster_id: int, replica_addresses: List[str]):
        self.tb_client = ffi.new("tb_client_t *")

        self.out_packets = ffi.new("tb_packet_list_t *")
        self.out_packets.head = ffi.new("tb_packet_t *")
        self.out_packets.tail = ffi.new("tb_packet_t *")

        status = _tb_client.lib.tb_client_init(
                self.tb_client,
                self.out_packets,
                cluster_id,
                b"3001",
                4,
                4096,
                0,
                lib.on_completion_fn
            )
        print(self.tb_client[0])

        if status != 0:
            raise TigerBeetleInitException()

        self.completion_mapping[self.tb_client[0]] = self
        self.inflight = {}

    def _acquire_packet(self):
        # TODO: Thread safety
        packet = self.out_packets.head

        if packet is None:
            raise Exception("Too many concurrent requests")

        self.out_packets.head = packet.next
        packet.next = None

        if self.out_packets.head is None:
            self.out_packets.tail = None

        return packet

    def _release_packet(self, packet):
        # TODO: Thread safety
        if self.out_packets.head is None:
            self.out_packets.head = packet
            self.out_packets.tail = packet
        else:
            self.out_packets.tail.next = packet
            self.out_packets.tail = packet

    @staticmethod
    def _get_event_size(op):
        if op == lib.TB_OPERATION_CREATE_ACCOUNTS:
            return ffi.sizeof("tb_account_t")
        elif op == TB_OPERATION_CREATE_TRANSFERS:
            return ffi.sizeof("tb_transfer_t")
        elif op == TB_OPERATION_LOOKUP_ACCOUNTS:
            return ffi.sizeof("tb_uint128_t")
        elif op == TB_OPERATION_LOOKUP_TRANSFERS:
            return ffi.sizeof("tb_uint128_t")
        else:
            return 0

    @staticmethod
    def _get_result_size(op):
        if op == lib.TB_OPERATION_CREATE_ACCOUNTS:
            return ffi.sizeof("tb_create_accounts_result_t")
        elif op == TB_OPERATION_CREATE_TRANSFERS:
            return ffi.sizeof("tb_create_transfers_result_t")
        elif op == TB_OPERATION_LOOKUP_ACCOUNTS:
            return ffi.sizeof("tb_account_t")
        elif op == TB_OPERATION_LOOKUP_TRANSFERS:
            return ffi.sizeof("tb_transfer_t")
        else:
            return 0

    def _request(self, op, count, data):
        # GIL Protects us (maybe???)
        request_id = len(self.inflight)

        packet = self._acquire_packet()
        packets = ffi.new("tb_packet_list_t *")
        packets.head = packet
        packets.tail = packet

        packet.next = ffi.NULL
        packet.user_data = ffi.cast("void *", request_id)
        packet.operation = lib.TB_OPERATION_CREATE_ACCOUNTS
        packet.status = lib.TB_PACKET_OK
        packet.data_size = self._get_event_size(op) * count
        packet.data = data

        self.inflight[request_id] = [threading.Event(), None]
        _tb_client.lib.tb_client_submit(self.tb_client[0], packets)

        return self.inflight[request_id]

    def _on_completion_fn(self, context, client, packet, result_ptr, result_len):
        # TODO: Needs to be bubbled outside of this function
        # # Handle packet error
        # if packet[0].status != lib.TB_PACKET_OK:
        #     raise Exception(ENUMS["TB_PACKET"][packet[0].status])

        request_id = int(ffi.cast("int", packet[0].user_data))

        self.inflight[request_id][1] = (result_ptr, result_len)
        self.inflight[request_id][0].set()

    def create_accounts(self, batch):
        count = len(batch)
        c_batch = ffi.new(f"tb_account_t[{count}]")
        for idx, item in enumerate(batch):
            c_batch[idx].id = item["id"]
            c_batch[idx].user_data = item["user_data"]
            c_batch[idx].ledger = item["ledger"]
            c_batch[idx].code = item["code"]
            c_batch[idx].flags = item["flags"]

        req = client._request(lib.TB_OPERATION_CREATE_ACCOUNTS, count, c_batch)
        req[0].wait()

        result_ptr, result_len = req[1]
        count = result_len // self._get_result_size(lib.TB_OPERATION_CREATE_ACCOUNTS)
        result = ffi.cast(f"tb_create_accounts_result_t[{count}]", result_ptr)

        results = []
        for res in result:
            results.append({"index": res.index, "result": res.result})

        return results

    def create_transfers(self, batch):
        pass

    def lookup_accounts(self):
        pass

    def lookup_transfers(self):
        pass

    def destroy(self):
        _tb_client.lib.tb_client_deinit(self.tb_client[0])
        del self.completion_mapping[self.tb_client[0]]

import time
client = Client(0, ["3001"])
r = client.create_accounts([{
    "id": (123, 1291),
    "user_data": (0, 0),
    "code": 1,
    "ledger": 1,
    "flags": 0
}, {
    "id": (123, 138),
    "user_data": (0, 0),
    "code": 1,
    "ledger": 1,
    "flags": 0
}])
print(r)
client.destroy()
