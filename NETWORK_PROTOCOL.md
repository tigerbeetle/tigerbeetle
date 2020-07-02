# Network Protocol

The TCP wire protocol is simple:

* a fixed-size header that can be used for requests or responses,
* followed by variable-length data.

```
HEADER (64 bytes)
16 bytes CHECKSUM META (remaining HEADER)
16 bytes CHECKSUM DATA
16 bytes ID (to match responses to requests and enable multiplexing in future)
 8 bytes MAGIC (for protocol versioning)
 4 bytes COMMAND
 4 bytes SIZE (of DATA if any)
DATA (multiples of 64 bytes)
................................................................................
................................................................................
................................................................................
```

The header is a multiple of 64 bytes because we want to keep the subsequent data
aligned to 64-byte cache line boundaries. We don't want any structure to
straddle multiple cache lines unnecessarily.

We order the header struct as we do to keep any future C implementations
padding-free.

The ID means we can switch to any reliable, unordered protocol in future. This
is useful where you want to multiplex messages with different priorites. For
example, huge batches would cause head-of-line blocking on a TCP connection,
blocking critical control-plane messages.

The MAGIC means we can do backwards-incompatible upgrades incrementally, without
shutting down the whole TigerBeetle system. The MAGIC also lets us discard any
obviously bad traffic without doing any checksum calculations.

We use SHA256 as our checksum for the prototype. We will switch to BLAKE3 after
the prototype for more than double the throughput.

The reason we use two checksums instead of only a single checksum across header
and data is that we need a reliable way to know the size of the data to expect,
before we start receiving the data.

Here is an example showing the risk of a single checksum for the recipient:

1. We receive a header with a single checksum protecting both header and data.
2. We extract the SIZE of the data from the header (4 GB in this case).
3. We cannot tell if this SIZE value is corrupt until we receive the data.
4. We wait for 4 GB of data to arrive before calculating/comparing checksums.
5. Except the SIZE was corrupted in transit from 16 MB to 4 GB (2 bit flips).
6. We never detect the corruption, the connection times out and we miss our SLA.

Any problems with this protocol that you can spot?
