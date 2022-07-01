const assert = require('assert');

const PORT = process.env.PORT || 30000;

const Node = {
  child: require('child_process'),
  crypto: require('crypto'),
  fs: require('fs'),
  net: require('net'),
  process: process
};

const HashTable = require('@ronomon/hash-table');
const Queue = require('@ronomon/queue');

// You need to create and preallocate an empty journal using:
// dd < /dev/zero bs=1048576 count=256 > journal

// 10,000 Foot Overview:
//
// 1. Receive a batch of transfers from the network.
// 2. Write these sequentially to disk and fsync.
// 3. Apply business logic and apply to in-memory state.

// See DATA_STRUCTURES.md:

const TRANSFER                         = 64;
const TRANSFER_ID                      = 16;
const TRANSFER_PAYER                   = 8;
const TRANSFER_PAYEE                   = 8;
const TRANSFER_AMOUNT                  = 8;
const TRANSFER_EXPIRE_TIMESTAMP        = 6;
const TRANSFER_CREATE_TIMESTAMP        = 6;
const TRANSFER_COMMIT_TIMESTAMP        = 6;
const TRANSFER_USERDATA                = 6;

const TRANSFER_ID_OFFSET               = 0;
const TRANSFER_PAYER_OFFSET            = 0 + 16;
const TRANSFER_PAYEE_OFFSET            = 0 + 16 + 8;
const TRANSFER_AMOUNT_OFFSET           = 0 + 16 + 8 + 8;
const TRANSFER_EXPIRE_TIMESTAMP_OFFSET = 0 + 16 + 8 + 8 + 8;
const TRANSFER_CREATE_TIMESTAMP_OFFSET = 0 + 16 + 8 + 8 + 8 + 6;
const TRANSFER_COMMIT_TIMESTAMP_OFFSET = 0 + 16 + 8 + 8 + 8 + 6 + 6;
const TRANSFER_USERDATA_OFFSET         = 0 + 16 + 8 + 8 + 8 + 6 + 6 + 6;

const COMMIT_TRANSFER                  = 48;
const COMMIT_TRANSFER_ID               = 16;
const COMMIT_TRANSFER_PREIMAGE         = 32;

const COMMIT_TRANSFER_ID_OFFSET        = 0;
const COMMIT_TRANSFER_PREIMAGE_OFFSET  = 0 + 16;

const PARTICIPANT                      = 24;
const PARTICIPANT_ID                   = 8;
const PARTICIPANT_NET_DEBIT_CAP        = 8;
const PARTICIPANT_POSITION             = 8;

const PARTICIPANT_ID_OFFSET            = 0;
const PARTICIPANT_NET_DEBIT_CAP_OFFSET = 0 + 8;
const PARTICIPANT_POSITION_OFFSET      = 0 + 8 + 8;

const COMMAND_CREATE_TRANSFERS = 1;
const COMMAND_ACCEPT_TRANSFERS = 2;
const COMMAND_REJECT_TRANSFERS = 3;
const COMMAND_ACK = 4;

// See NETWORK_PROTOCOL.md:

const MAGIC = Buffer.from('0a5ca1ab1ebee11e', 'hex'); // A scalable beetle...

const NETWORK_HEADER                      = 64;
const NETWORK_HEADER_CHECKSUM_META        = 16;
const NETWORK_HEADER_CHECKSUM_DATA        = 16;
const NETWORK_HEADER_ID                   = 16;
const NETWORK_HEADER_MAGIC                = 8;
const NETWORK_HEADER_COMMAND              = 4;
const NETWORK_HEADER_SIZE                 = 4;

const NETWORK_HEADER_CHECKSUM_META_OFFSET = 0;
const NETWORK_HEADER_CHECKSUM_DATA_OFFSET = 0 + 16;
const NETWORK_HEADER_ID_OFFSET            = 0 + 16 + 16;
const NETWORK_HEADER_MAGIC_OFFSET         = 0 + 16 + 16 + 16;
const NETWORK_HEADER_COMMAND_OFFSET       = 0 + 16 + 16 + 16 + 8;
const NETWORK_HEADER_SIZE_OFFSET          = 0 + 16 + 16 + 16 + 8 + 4;

const CHECKSUM = Buffer.alloc(32);

const Result = require('./result.js');

const Journal = require('./journal.js');
Journal.open('./journal');

// We pre-allocate buffers outside of the critical path.
// We reuse these when working with hash table values.
const BUF_PAYER = Buffer.alloc(PARTICIPANT);
const BUF_PAYEE = Buffer.alloc(PARTICIPANT);
const BUF_TRANSFER = Buffer.alloc(TRANSFER);

const Logic = {};

// Compare two buffers for equality without creating slices and associated GC:
const Equal = function(a, aOffset, b, bOffset, size) {
  // Node's buffer.equals() accepts no offsets, costing 100,000 TPS!!
  while (size--) if (a[aOffset++] !== b[bOffset++]) return false;
  return true;
};

function Hash(algorithm, source, sourceOffset, sourceSize, target, targetOffset) {
  var hash = Node.crypto.createHash(algorithm);
  hash.update(source.slice(sourceOffset, sourceOffset + sourceSize));
  return hash.digest().copy(target, targetOffset);
}

Logic.adjustParticipantPosition = function(amount, buffer, offset) {
  assert(Number.isSafeInteger(amount));
  assert(Number.isSafeInteger(offset));
  assert(offset >= 0 && offset + PARTICIPANT <= buffer.length);
  const position = buffer.readUIntBE(
    offset + PARTICIPANT_POSITION_OFFSET,
    6 // TODO 64-bit
  );
  const net_debit_cap = buffer.readUIntBE(
    offset + PARTICIPANT_NET_DEBIT_CAP_OFFSET,
    6 // TODO 64-bit
  );
  assert(position + amount >= 0);
  assert(position + amount <= net_debit_cap);
  buffer.writeUIntBE(
    position + amount,
    offset + PARTICIPANT_POSITION_OFFSET,
    6 // TODO 64-bit
  );
};

Logic.acceptTransfer = function(state, buffer, offset) {
  assert(Number.isSafeInteger(offset));
  assert(offset >= 0 && offset + COMMIT_TRANSFER <= buffer.length);
  if (state.transfers.exist(buffer, offset + COMMIT_TRANSFER_ID_OFFSET) !== 1) {
    return Result.TransferDoesNotExist;
  }

  // TODO Check if transfer is committed.
  // TODO Check if transfer is expired.
  // TODO Update balances.
  
  return Result.OK;
};

Logic.createTransfer = function(state, buffer, offset) {
  assert(Number.isSafeInteger(offset));
  assert(offset >= 0 && offset + TRANSFER <= buffer.length);

  if (state.transfers.exist(buffer, offset + TRANSFER_ID_OFFSET) === 1) {
    // TODO Currently allowing duplicate results to make benchmarking easier.
    // return Result.TransferExists;
  }

  if (
    state.participants.get(buffer, offset + TRANSFER_PAYER_OFFSET, BUF_PAYER, 0)
    !== 1
  ) {
    return Result.PayerDoesNotExist;
  }
  if (
    state.participants.get(buffer, offset + TRANSFER_PAYEE_OFFSET, BUF_PAYEE, 0)
    !== 1
  ) {
    return Result.PayeeDoesNotExist;
  }

  if (
    Equal(
      BUF_PAYER,
      PARTICIPANT_ID_OFFSET,
      BUF_PAYEE,
      PARTICIPANT_ID_OFFSET,
      PARTICIPANT_ID
    )
  ) {
    return Result.ParticipantsAreTheSame;
  }

  const amount = buffer.readUIntBE(
    offset + TRANSFER_AMOUNT_OFFSET,
    6 // TODO 64-bit
  );
  if (amount === 0) return Result.TransferAmountZero;
  const payer_net_debit_cap = BUF_PAYER.readUIntBE(
    PARTICIPANT_NET_DEBIT_CAP_OFFSET,
    6 // TODO 64-bit
  );
  const payer_position = BUF_PAYER.readUIntBE(
    PARTICIPANT_POSITION_OFFSET,
    6 // TODO 64-bit
  );
  // TODO Check if Mojaloop is correct in failing if position + amount >= NDC.
  if (payer_position + amount > payer_net_debit_cap) {
    return Result.NetDebitCap;
  }

  const expire_timestamp = buffer.readUIntBE(
    offset + TRANSFER_EXPIRE_TIMESTAMP_OFFSET,
    TRANSFER_EXPIRE_TIMESTAMP
  );
  const create_timestamp = buffer.readUIntBE(
    offset + TRANSFER_CREATE_TIMESTAMP_OFFSET,
    TRANSFER_CREATE_TIMESTAMP
  );
  const commit_timestamp = buffer.readUIntBE(
    offset + TRANSFER_COMMIT_TIMESTAMP_OFFSET,
    TRANSFER_COMMIT_TIMESTAMP
  );
  // Date.now() is expensive.
  // TODO Cache result of call to Date.now() and set before inserting batch.
  if (expire_timestamp < Date.now()) return Result.TransferExpired;
  if (create_timestamp !== 0) return Result.TransferCreateTimestampReserved;
  if (commit_timestamp !== 0) return Result.TransferCommitTimestampReserved;

  const userdata = buffer.readUIntBE(
    offset + TRANSFER_USERDATA_OFFSET,
    TRANSFER_USERDATA
  );
  assert(userdata === 0); // Not yet supported.
  
  // TODO Double-check that all validations are done (see BUSINESS_LOGIC.json).
  
  // Set reserved create timestamp to indicate that transfer has been prepared:
  buffer.writeUIntBE(
    Date.now(), // TODO Use batch timestamp to amortize calls to Date.now().
    offset + TRANSFER_CREATE_TIMESTAMP_OFFSET,
    TRANSFER_CREATE_TIMESTAMP
  );

  // Adjust payer position:
  Logic.adjustParticipantPosition(amount, BUF_PAYER, 0);
  state.participants.set(BUF_PAYER, 0, BUF_PAYER, 0);

  // TODO Assert insert is unique.
  state.transfers.set(buffer, offset + 0, buffer, offset + 0);
  return Result.OK;
};

const State = {};

// TODO Set elementsMin and elementsMax according to deployment config.
// TODO Iterate across hash table buffers and initialize to preempt page faults.
State.participants = new HashTable(8, 24, 1000, 10000);
State.transfers = new HashTable(16, 64, 10000000, 256000000);

// Add two participants for testing:
(function() {
  const payer_id = Buffer.alloc(PARTICIPANT_ID, 1);
  const payee_id = Buffer.alloc(PARTICIPANT_ID, 2);
  const net_debit_cap = Buffer.alloc(PARTICIPANT_NET_DEBIT_CAP);
  net_debit_cap.writeUIntBE(10000000, 0, 6);
  const position = Buffer.alloc(PARTICIPANT_POSITION);
  position.writeUIntBE(2000000, 0, 6);

  assert(
    State.participants.set(
      payer_id,
      0,
      Buffer.concat([
        payer_id,
        net_debit_cap,
        position
      ]),
      0
    ) === 0
  );

  assert(
    State.participants.set(
      payee_id,
      0,
      Buffer.concat([
        payee_id,
        net_debit_cap,
        position
      ]),
      0
    ) === 0
  );
})();

const ProcessRequest = function(header, data) {
  const command = header.readUInt32BE(NETWORK_HEADER_COMMAND_OFFSET);
  assert(
    command === COMMAND_CREATE_TRANSFERS ||
    command === COMMAND_ACCEPT_TRANSFERS
  );
  const size = header.readUInt32BE(NETWORK_HEADER_SIZE_OFFSET);
  assert(data.length === size);
  Journal.write(data);
  var method;
  var struct_size;
  if (command === COMMAND_CREATE_TRANSFERS) {
    method = Logic.createTransfer;
    struct_size = TRANSFER;
  } else if (command === COMMAND_ACCEPT_TRANSFERS) {
    method = Logic.acceptTransfer;
    struct_size = COMMIT_TRANSFER;
  } else {
    throw new Error('unsupported command');
  }
  var offset = 0;
  while (offset < data.length) {
    assert(method(State, data, offset) === Result.OK);
    offset += struct_size;
  }
  assert(offset === data.length);
  // TODO Error buffer
};

const AckHeader = function(request, header, data) {
  // Copy request ID to response ID to match responses to requests:
  request.copy(
    header,
    NETWORK_HEADER_ID_OFFSET,
    NETWORK_HEADER_ID_OFFSET,
    NETWORK_HEADER_ID_OFFSET + NETWORK_HEADER_ID
  );
  MAGIC.copy(header, NETWORK_HEADER_MAGIC_OFFSET);
  header.writeUInt32BE(COMMAND_ACK, NETWORK_HEADER_COMMAND_OFFSET);
  header.writeUInt32BE(data.length, NETWORK_HEADER_SIZE_OFFSET);
  Node.crypto.createHash('sha256').update(data).digest();
  assert(
    Hash(
      'sha256',
      data,
      0,
      data.length,
      CHECKSUM,
      0
    ) === 32
  );
  assert(
    CHECKSUM.copy(
      header,
      NETWORK_HEADER_CHECKSUM_DATA_OFFSET,
      0,
      NETWORK_HEADER_CHECKSUM_DATA
    ) === 16
  );
  assert(
    Hash(
      'sha256',
      header,
      NETWORK_HEADER_CHECKSUM_DATA_OFFSET,
      NETWORK_HEADER - NETWORK_HEADER_CHECKSUM_META,
      CHECKSUM,
      0
    ) === 32
  );
  assert(
    CHECKSUM.copy(
      header,
      NETWORK_HEADER_CHECKSUM_META_OFFSET,
      0,
      NETWORK_HEADER_CHECKSUM_META
    ) === 16
  );
};

const Server = Node.net.createServer(
  function(socket) {
    console.log('Client connected...');
    // We truncate 256-bit checksums to 128-bit:
    const checksum_meta = Buffer.alloc(32);
    const checksum_data = Buffer.alloc(32);
    assert(checksum_meta.length > NETWORK_HEADER_CHECKSUM_META);
    assert(checksum_data.length > NETWORK_HEADER_CHECKSUM_DATA);
    var header;
    var headerBuffers = [];
    var headerRemaining = NETWORK_HEADER;
    var data;
    var dataBuffers = [];
    var dataRemaining = 0;
    socket.on('data',
      function(buffer) {
        while (buffer.length) {
          if (headerRemaining) {
            // We are expecting a header.
            if (headerRemaining > buffer.length) {
              // ...but buffer contains only a partial header buffer.
              console.log(`Received a partial header buffer ${buffer.length} bytes`);
              headerBuffers.push(buffer);
              headerRemaining -= buffer.length;
              return;
            }
            headerBuffers.push(buffer.slice(0, headerRemaining));
            header = Buffer.concat(headerBuffers);
            buffer = buffer.slice(headerRemaining);
            headerRemaining = 0;
            // Verify header MAGIC:
            if (
              !Equal(
                header,
                NETWORK_HEADER_MAGIC_OFFSET,
                MAGIC,
                0,
                NETWORK_HEADER_MAGIC
              )
            ) {
              // We don't speak the same language or something is really wrong.
              console.log('Wrong network protocol magic!');
              return socket.destroy(); // Prevent parsing more data events.
            }
            // Verify header CHECKSUM_META:
            assert(
              Hash(
                'sha256',
                header,
                NETWORK_HEADER_CHECKSUM_DATA_OFFSET,
                NETWORK_HEADER - NETWORK_HEADER_CHECKSUM_META,
                checksum_meta,
                0
              ) === 32
            );
            if (
              !Equal(
                header,
                NETWORK_HEADER_CHECKSUM_META_OFFSET,
                checksum_meta,
                0,
                NETWORK_HEADER_CHECKSUM_META
              )
            ) {
              // TODO We are treating corruption the same as a network error?
              console.log('Corrupt network protocol header!');
              return socket.destroy(); // Prevent parsing more data events.
            }
            dataBuffers = [];
            dataRemaining = header.readUInt32BE(NETWORK_HEADER_SIZE_OFFSET);
            // We are done receiving the header... onwards!
            if (buffer.length === 0) return;
          }

          // We are expecting data.
          assert(dataRemaining >= 0);
          if (dataRemaining > buffer.length) {
            // ... but buffer contains only a partial data buffer.
            dataBuffers.push(buffer);
            dataRemaining -= buffer.length;
            return;
          }
          dataBuffers.push(buffer.slice(0, dataRemaining));
          data = Buffer.concat(dataBuffers);
          buffer = buffer.slice(dataRemaining);
          
          // TODO Parser needs to work with async calls.
          ProcessRequest(header, data);
          const reply_header = Buffer.alloc(NETWORK_HEADER);
          const reply_data = Buffer.alloc(0); // TODO
          AckHeader(header, reply_header, reply_data);
          socket.write(reply_header);
          if (reply_data.length) socket.write(reply_data);

          header = undefined;
          headerBuffers = [];
          headerRemaining = NETWORK_HEADER;
          data = undefined;
          dataBuffers = [];
          dataRemaining = 0;
        }
      }
    );
    socket.on('end',
      function() {
        console.log('Client disconnected.');
      }
    );
  }
);

Server.on('error', function(error) { console.error(error); });

Server.listen(Number(PORT),
  function() {
    console.log(`Listening on ${PORT}...`);
  }
);
