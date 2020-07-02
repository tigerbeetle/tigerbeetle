const assert = require('assert');

const Node = {
  fs: require('fs'),
  net: require('net'),
  process: process
};

const Crypto = require('@ronomon/crypto-async');

const HOST = 'tb.perf.openafrica.network';
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

const transfers = Node.fs.readFileSync('transfers');
assert(transfers.length % TRANSFER === 0);

const start = Date.now();

const CHECKSUM = Buffer.alloc(32); // We truncate 256-bit checksums to 128-bit.

var id = 0;
var transfersOffset = 0;
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
  transfersOffset += data.length;

  var header = Buffer.alloc(NETWORK_HEADER);
  header.writeUIntBE(id++, NETWORK_HEADER_ID_OFFSET, 6);
  MAGIC.copy(header, NETWORK_HEADER_MAGIC_OFFSET);
  header.writeUInt32BE(COMMAND_CREATE_TRANSFERS, NETWORK_HEADER_COMMAND_OFFSET);
  header.writeUInt32BE(data.length, NETWORK_HEADER_SIZE_OFFSET);
  assert(
    Crypto.hash(
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
    Crypto.hash(
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
  
  var sent = transfersOffset / TRANSFER;
  if (sent % 100000 === 0) {
    console.log(`${sent.toString().padStart(7, ' ')} transfers...`);
  }
}

console.log(`connecting to ${HOST}:${PORT}...`);

const socket = Node.net.createConnection(PORT, HOST,
  function() {
    console.log('connected to server');
    socket.on('data',
      function(buffer) {
        // TODO Handle any server results for the batch.
        sendBatch(socket);
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
