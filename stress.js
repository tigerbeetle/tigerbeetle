const assert = require('assert');

const Node = {
  fs: require('fs'),
  net: require('net'),
  process: process
};

const HOST = '127.0.0.1';
const PORT = 30000;

const TRANSFER = 64;
const BATCH_SIZE = TRANSFER * 10000;

const transfers = Node.fs.readFileSync('transfers');
assert(transfers.length % TRANSFER === 0);

const start = Date.now();

var transfersOffset = 0;
function sendBatch(socket) {
  if (transfersOffset === transfers.length) {
    const ms = Date.now() - start;
    const count = transfers.length / TRANSFER;
    const tps = Math.floor(count / (ms / 1000));
    console.log(new Array(30).join('='));
    console.log(`${tps.toString().padStart(7, ' ')} transfers per second`);
    socket.write(Buffer.alloc(4)); // kill server
    return Node.process.exit();
  }
  var batch = transfers.slice(transfersOffset, transfersOffset + BATCH_SIZE);
  var batchSize = Buffer.alloc(4);
  batchSize.writeUInt32BE(batch.length, 0);
  transfersOffset += batch.length;
  socket.write(batchSize);
  socket.write(batch);
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
