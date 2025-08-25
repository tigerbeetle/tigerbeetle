import asyncio
import ctypes
import itertools
import socket
import threading
import time

import pytest

import tigerbeetle as tb
tb.configure_logging(debug=True)

def _blocking_lookup(client, result):
    assert isinstance(result, list)
    try:
        result = client.lookup_accounts([1])
        raise AssertionError("lookup_accounts didn't throw an exception")
    except Exception as e:
        result.append(e)

def test_close_sync():
    # Bind a socket to a free port to get a socket that's definitely not TigerBeetle.
    not_tigerbeetle = socket.socket()
    not_tigerbeetle.bind(('127.0.0.1', 0))
    not_tigerbeetle_port = not_tigerbeetle.getsockname()[1]

    client = tb.ClientSync(cluster_id=1234, replica_addresses=f"127.0.0.1:{not_tigerbeetle_port}")

    # Submit a request, which would normally block.
    thread_result = []
    thread = threading.Thread(target=_blocking_lookup, args=(client, thread_result))
    thread.start()

    # Wait until the request is actually in-flight.
    for i in range(0, 10):
        if len(client._inflight_packets) == 1:
            break
        time.sleep(0.01)
    else:
        raise AssertionError("thread didn't create a request in time")

    assert len(client._inflight_packets) == 1

    client.close()
    thread.join()

    assert len(thread_result) == 1
    with pytest.raises(tb.PacketError, match='CLIENT_SHUTDOWN'):
        raise thread_result[0]

    # Closing the client should have resulted in the request being terminated.
    assert len(client._inflight_packets) == 0
    assert client._client_key not in tb.ClientSync._clients

def test_close_async():
    # Saves having an extra dependency like pytest-asyncio!
    asyncio.run(_test_close_async())

async def _test_close_async():
    # Bind a socket to a free port to get a socket that's definitely not TigerBeetle.
    not_tigerbeetle = socket.socket()
    not_tigerbeetle.bind(('127.0.0.1', 0))
    not_tigerbeetle_port = not_tigerbeetle.getsockname()[1]

    client = tb.ClientAsync(cluster_id=1234, replica_addresses=f"127.0.0.1:{not_tigerbeetle_port}")

    # Submit a request, as a task.
    lookup_task = asyncio.create_task(client.lookup_accounts([1]))

    # Wait until the request is actually in-flight.
    for i in range(0, 10):
        if len(client._inflight_packets) == 1:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("thread didn't create a request in time")

    assert len(client._inflight_packets) == 1

    await client.close()

    with pytest.raises(tb.PacketError, match='CLIENT_SHUTDOWN'):
        lookup_result = await lookup_task

    # Closing the client should have resulted in the request being terminated.
    assert len(client._inflight_packets) == 0
    assert client._client_key not in tb.ClientSync._clients
