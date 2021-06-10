const std = @import("std");
const assert = std.debug.assert;

pub const Header = extern struct {
    comptime {
        assert(@sizeOf(Header) == 128);
    }
    /// A checksum covering only the rest of this header starting from `checksum_body`.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    /// This checksum is enough to uniquely identify a network message or journal entry.
    checksum: u128 = 0,

    /// A checksum covering only the associated body after this header.
    checksum_body: u128 = 0,

    /// A backpointer to the checksum of the previous prepare if this header is for a prepare.
    hash_chain: u128 = 0,

    /// The checksum of the message to which this message refers, or a unique recovery nonce:
    /// We use this nonce in various ways, for example:
    /// * A prepare sets nonce to the checksum of the request.
    /// * A reply sets nonce to the checksum of the prepare so client can assert against the request.
    /// * A prepare_ok sets nonce to the checksum of the prepare it wants to ack.
    /// * A commit sets nonce to the checksum of the latest committed op.
    /// This enables cryptographic assertions beyond the request, op, and commit numbers, which may
    /// otherwise easily collide in the presence of any correctness bugs.
    nonce: u128 = 0,

    /// Each client records its own client ID and a current request number. A client is allowed to
    /// have just one outstanding request at a time.
    client: u128 = 0,

    /// Each request is given a number by the client and later requests must have larger numbers
    /// than earlier ones. The request number is used by the replicas to avoid running requests more
    /// than once; it is also used by the client to discard duplicate responses to its requests.
    request: u32 = 0,
    
    /// The cluster number binds intention into the header, so that a client or replica can indicate
    /// which cluster it thinks it's speaking to, instead of accidentally talking to the wrong
    /// cluster (for example, staging vs production).
    cluster: u32,

    /// The cluster reconfiguration epoch number (for future use):
    epoch: u32 = 0,

    /// Every message sent from one replica to another contains the sending replica's current view.
    /// A u32 allows for a minimum lifetime of 136 years at a rate of one view change per second.
    view: u32 = 0,

    /// The op number.
    op: u64 = 0,

    /// The commit number.
    commit: u64 = 0,

    /// The journal offset to which this message relates.
    /// This enables direct access to a prepare, without requiring previous variable-length entries.
    /// While we use fixed-size data structures, a batch will contain a variable amount of them.
    offset: u64 = 0,

    /// The size of this message header and any associated body.
    /// This must be 0 for an empty header with command == .reserved.
    size: u32 = @sizeOf(Header),

    /// The index of the replica in the cluster configuration array that originated this message.
    /// This identifies the ultimate author because messages may be forwarded amongst replicas.
    replica: u8 = 0,

    /// The Viewstamped Replication protocol command for this message.
    command: u8,

    /// The state machine operation to apply.
    operation: u8 = 0,
    
    /// The version of the protocol implementation that originated this message.
    version: u8 = 0,
};

test "size" {
    std.debug.print("\nHeader={}\n", .{@sizeOf(Header)});
}
