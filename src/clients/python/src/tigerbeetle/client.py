from __future__ import annotations

import asyncio
import ctypes
import logging
import os
import threading
import time
from collections.abc import Callable  # noqa: TCH003
from dataclasses import dataclass
from typing import Any

from . import bindings
from .lib import tb_assert, c_uint128, decode, encode, id

logger = logging.getLogger("tigerbeetle")


class AtomicInteger:
    def __init__(self, value=0):
        self._value = value
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self._value += 1
            return self._value


@dataclass
class CompletionContextSync:
    event: threading.Event


@dataclass
class CompletionContextAsync:
    loop: asyncio.AbstractEventLoop
    event: asyncio.Event


@dataclass
class InflightPacket:
    packet: bindings.CPacket
    response: Any
    operation: bindings.Operation
    result_type: Any
    on_completion: Callable | None
    on_completion_context: CompletionContextSync | CompletionContextAsync | None
    buffer: bytes


amount_max = (2 ** 128) - 1


class InitError(Exception):
    pass

class ClientClosedError(Exception):
    pass

class PacketError(Exception):
    pass


class Client:
    _clients: dict[int, Any] = {}
    _counter = AtomicInteger()

    def __init__(self, cluster_id: int, replica_addresses: str):
        self._client_key = Client._counter.increment()
        self._client = bindings.CClient()

        self._inflight_packets: dict[int, InflightPacket] = {}

        # ctypes needs a reference to keep this alive through the FFI call. Having it as a temporary
        # within the call _does not_ work.
        cluster_id_u128 = c_uint128.from_param(cluster_id)
        init_status = bindings.tb_client_init(
            ctypes.byref(self._client),
            ctypes.cast(
                ctypes.byref(cluster_id_u128), ctypes.POINTER(ctypes.c_uint8 * 16)
            ),
            replica_addresses.encode("ascii"),
            len(replica_addresses),
            self._client_key,
            self._c_on_completion
        )
        if init_status != bindings.InitStatus.SUCCESS:
            raise InitError(init_status)

        Client._clients[self._client_key] = self


    def _acquire_packet(self, operation: bindings.Operation, operations: Any,
                        result_type: Any) -> InflightPacket:
        packet = bindings.CPacket()
        packet.next = None
        packet.user_data = Client._counter.increment()
        packet.user_tag = 0
        packet.operation = operation
        packet.status = bindings.PacketStatus.OK

        buffer = encode(operation, operations)
        packet.data_size = len(buffer)
        packet.data = ctypes.cast(buffer, ctypes.c_void_p)

        return InflightPacket(
            packet=packet,
            response=None,
            on_completion=None,
            on_completion_context=None,
            operation=operation,
            result_type=result_type,
            buffer=buffer)

    def close(self):
        bindings.tb_client_deinit(ctypes.byref(self._client))
        tb_assert(self._client is not None)
        tb_assert(len(self._inflight_packets) == 0)
        del Client._clients[self._client_key]

    @staticmethod
    @bindings.OnCompletion
    def _c_on_completion(completion_ctx, packet, timestamp, bytes_ptr, len_):
        """
        Invoked in a separate thread
        """
        self: Client = Client._clients[completion_ctx]

        packet = ctypes.cast(packet, ctypes.POINTER(bindings.CPacket))
        inflight_packet = self._inflight_packets[packet[0].user_data]

        if packet[0].status != bindings.PacketStatus.OK.value:
            inflight_packet.response = PacketError(repr(bindings.PacketStatus(packet[0].status)))
            tb_assert(inflight_packet.on_completion is not None)
            inflight_packet.on_completion(inflight_packet)
            return

        result_type = inflight_packet.result_type

        # The memory referenced in bytes_ptr is only valid for the duration of this callback.
        # decode() copies this and returns a Python managed list.
        results_slice = ctypes.string_at(bytes_ptr, len_)
        inflight_packet.response = decode(inflight_packet.operation, result_type, results_slice)

        tb_assert(inflight_packet.on_completion is not None)
        inflight_packet.on_completion(inflight_packet)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ClientSync(Client, bindings.StateMachineMixin):
    def _on_completion(self, inflight_packet):
        inflight_packet.on_completion_context.event.set()

    def _submit(self, operation: bindings.Operation, operations: list[Any],
                result_type: Any):
        inflight_packet = self._acquire_packet(operation, operations, result_type)
        self._inflight_packets[inflight_packet.packet.user_data] = inflight_packet

        inflight_packet.on_completion = self._on_completion
        inflight_packet.on_completion_context = CompletionContextSync(event=threading.Event())

        client_state = bindings.tb_client_submit(ctypes.byref(self._client), ctypes.byref(inflight_packet.packet))
        if client_state == bindings.ClientStatus.OK:
            inflight_packet.on_completion_context.event.wait()

        del self._inflight_packets[inflight_packet.packet.user_data]

        if client_state == bindings.ClientStatus.INVALID:
            raise ClientClosedError()

        if isinstance(inflight_packet.response, Exception):
            raise inflight_packet.response

        return inflight_packet.response


class ClientAsync(Client, bindings.AsyncStateMachineMixin):
    def _on_completion(self, inflight_packet):
        """
        Called by Client._c_on_completion, which itself is called from a different thread. Use
        `call_soon_threadsafe` to return to the thread of the event loop the request was invoked
        from, so _trigger_event() can trigger the async event and allow the client to progress.
        """
        inflight_packet.on_completion_context.loop.call_soon_threadsafe(
            self._trigger_event,
            inflight_packet
        )

    def _trigger_event(self, inflight_packet):
        inflight_packet.on_completion_context.event.set()

    async def _submit(self, operation: bindings.Operation, operations: Any,
                      result_type: Any):
        inflight_packet = self._acquire_packet(operation, operations, result_type)
        self._inflight_packets[inflight_packet.packet.user_data] = inflight_packet

        inflight_packet.on_completion = self._on_completion
        inflight_packet.on_completion_context = CompletionContextAsync(
            loop=asyncio.get_event_loop(),
            event=asyncio.Event()
        )

        client_state = bindings.tb_client_submit(ctypes.byref(self._client), ctypes.byref(inflight_packet.packet))
        if client_state == bindings.ClientStatus.OK:
            await inflight_packet.on_completion_context.event.wait()

        del self._inflight_packets[inflight_packet.packet.user_data]

        if client_state == bindings.ClientStatus.INVALID:
            raise ClientClosedError()

        if isinstance(inflight_packet.response, Exception):
            raise inflight_packet.response

        return inflight_packet.response


@bindings.LogHandler
def log_handler(level_zig, message_ptr, message_len):
    level_python = {
        bindings.LogLevel.ERR: logging.ERROR,
        bindings.LogLevel.WARN: logging.WARNING,
        bindings.LogLevel.INFO: logging.INFO,
        bindings.LogLevel.DEBUG: logging.DEBUG,
    }[level_zig]
    logger.log(level_python, ctypes.string_at(message_ptr, message_len).decode("utf-8"))

tb_assert(bindings.tb_client_register_log_callback(log_handler, True) ==
    bindings.RegisterLogCallbackStatus.SUCCESS)

def configure_logging(*, debug, log_handler=log_handler):
    # First disable the existing log handler, before enabling the new one.
    tb_assert(bindings.tb_client_register_log_callback(None, debug) ==
        bindings.RegisterLogCallbackStatus.SUCCESS)

    tb_assert(bindings.tb_client_register_log_callback(log_handler, debug) ==
        bindings.RegisterLogCallbackStatus.SUCCESS)
