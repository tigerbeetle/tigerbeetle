const assert = require('assert');

const Node = {
  crypto: require('crypto'),
  fs: require('fs'),
  net: require('net'),
  process: process
};

const HOST = '127.0.0.1';
const PORT = 30000;

const TRANSFER = 64;
const BATCH_SIZE = TRANSFER * 10000;

const COMMAND_CREATE_TRANSFERS = 1;
const COMMAND_ACCEPT_TRANSFERS = 2;

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

const COMMIT_TRANSFER                  = 48;
const COMMIT_TRANSFER_ID               = 16;
const COMMIT_TRANSFER_PREIMAGE         = 32;

const COMMIT_TRANSFER_ID_OFFSET        = 0;
const COMMIT_TRANSFER_PREIMAGE_OFFSET  = 0 + 16;

const CHECKSUM = Buffer.alloc(32); // We truncate 256-bit checksums to 128-bit.

const transfers = Node.fs.readFileSync('transfers');
assert(transfers.length % TRANSFER === 0);

function Hash(algorithm, source, sourceOffset, sourceSize, target, targetOffset) {
  var hash = Node.crypto.createHash(algorithm);
  hash.update(source.slice(sourceOffset, sourceOffset + sourceSize));
  return hash.digest().copy(target, targetOffset);
}

const start = Date.now();

var id = 0;
var transfersOffset = 0;
var pendingAccept;
function sendBatch(socket) {
  if (transfersOffset === transfers.length) {
    const ms = Date.now() - start;
    const count = transfers.length / TRANSFER;
    const tps = Math.floor(count / (ms / 1000));
    console.log(new Array(30).join('='));
    console.log(`${tps.toString().padStart(7, ' ')} transfers per second`);
    return Node.process.exit();
  }

  var data = transfers.slice(transfersOffset, transfersOffset + BATCH_SIZE);
  // console.log(`creating ${data.length / TRANSFER} transfers...`);
  transfersOffset += data.length;

  var header = Buffer.alloc(NETWORK_HEADER);
  header.writeUIntBE(id++, NETWORK_HEADER_ID_OFFSET, 6);
  MAGIC.copy(header, NETWORK_HEADER_MAGIC_OFFSET);
  header.writeUInt32BE(COMMAND_CREATE_TRANSFERS, NETWORK_HEADER_COMMAND_OFFSET);
  header.writeUInt32BE(data.length, NETWORK_HEADER_SIZE_OFFSET);
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
  pendingAccept = data;
  socket.write(header);
  socket.write(data);
  
  var sent = transfersOffset / TRANSFER;
  if (sent % 100000 === 0) {
    console.log(`${sent.toString().padStart(7, ' ')} transfers...`);
  }
}

function acceptTransfers(source, socket) {
  const data = Buffer.alloc(source.length / TRANSFER * COMMIT_TRANSFER);
  // console.log(`accepting ${data.length / COMMIT_TRANSFER} transfers...`);
  // Copy transfer ID (but do nothing for preimage at this stage):
  var sourceOffset = 0;
  var targetOffset = 0;
  while (sourceOffset < source.length) {
    source.copy(data, targetOffset, sourceOffset + 0, sourceOffset + 16);
    sourceOffset += TRANSFER;
    targetOffset += COMMIT_TRANSFER;
  }
  const header = Buffer.alloc(NETWORK_HEADER);
  header.writeUIntBE(id++, NETWORK_HEADER_ID_OFFSET, 6);
  MAGIC.copy(header, NETWORK_HEADER_MAGIC_OFFSET);
  header.writeUInt32BE(COMMAND_ACCEPT_TRANSFERS, NETWORK_HEADER_COMMAND_OFFSET);
  header.writeUInt32BE(data.length, NETWORK_HEADER_SIZE_OFFSET);
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
  socket.write(header);
  socket.write(data);
}

console.log(`Connecting to ${HOST}:${PORT}...`);

const socket = Node.net.createConnection(PORT, HOST,
  function() {
    console.log('Connected to server');
    socket.on('data',
      function(buffer) {
        // TODO Handle any server results for the batch.
        assert(buffer.length === 64);
        if (pendingAccept) {
          acceptTransfers(pendingAccept, socket);
          pendingAccept = undefined;
        } else {
          sendBatch(socket);
        }
      }
    );
    socket.on('end',
      function() {
        throw new Error('server disconnected.');
      }
    );
    sendBatch(socket);
  }
);
