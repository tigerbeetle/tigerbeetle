const assert = require('assert');

const PORT = 30000;

const Node = {
  child: require('child_process'),
  fs: require('fs'),
  net: require('net'),
  process: process
};

const Crypto = require('@ronomon/crypto-async');
const HashTable = require('@ronomon/hash-table');
const Queue = require('@ronomon/queue');

// You need to create and preallocate an empty journal using:
// dd < /dev/zero bs=1048576 count=256 > journal

// 10 Foot Overview:
// 1. Receive a batch of transfers from the network.
// 2. Write these sequentially to disk and fsync.
// 3. Apply business logic and apply to in-memory state.

// transfer {
//                 id: 16 bytes (128-bit)
//              payer:  8 bytes ( 64-bit)
//              payee:  8 bytes ( 64-bit)
//             amount:  8 bytes ( 64-bit)
//   expire_timestamp:  6 bytes ( 48-bit)
//   create_timestamp:  6 bytes ( 48-bit) [reserved]
//   commit_timestamp:  6 bytes ( 48-bit) [reserved]
//           userdata:  6 bytes ( 48-bit) [reserved]
// }

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

const PARTICIPANT                      = 24;
const PARTICIPANT_ID                   = 8;
const PARTICIPANT_NET_DEBIT_CAP        = 8;
const PARTICIPANT_POSITION             = 8;

const PARTICIPANT_ID_OFFSET            = 0;
const PARTICIPANT_NET_DEBIT_CAP_OFFSET = 0 + 8;
const PARTICIPANT_POSITION_OFFSET      = 0 + 8 + 8;

const Result = require('./result.js');

const Journal = require('./journal.js');
Journal.open('./journal');

const PAYER = Buffer.alloc(PARTICIPANT);
const PAYEE = Buffer.alloc(PARTICIPANT);

const Logic = {};

// Compare two buffers for equality without creating slices and associated GC:
Logic.equal = function(a, aOffset, b, bOffset, size) {
  // Node's buffer.equals() accepts no offsets, costing 100,000 TPS!!
  while (size--) if (a[aOffset++] !== b[bOffset++]) return false;
  return true;
};

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

Logic.commitTransfer = function(state, buffer, offset) {
  // 1587309924771 "insert into `transferFulfilment` (`completedDate`, `createdDate`, `ilpFulfilment`, `isValid`, `settlementWindowId`, `transferId`)"
  // 1587309924775 "UPDATE participantPosition SET value = (value + -100), changedDate = '2020-04-19 15:25:24.769' WHERE participantPositionId = 5 "
  // 1587309924776 "INSERT INTO participantPositionChange (participantPositionId, transferStateChangeId, value, reservedValue, createdDate) SELECT 5, 1254, value, reservedValue, '2020-04-19 15:25:24.769' FROM participantPosition WHERE participantPositionId = 5"
};

Logic.createTransfer = function(state, buffer, offset) {
  assert(Number.isSafeInteger(offset));
  assert(offset >= 0 && offset + TRANSFER <= buffer.length);
  assert(offset % TRANSFER === 0);

  if (state.transfers.exist(buffer, offset + TRANSFER_ID_OFFSET) === 1) {
    // TODO Currently allowing duplicate results to make benchmarking easier.
    // return Result.TransferExists;
  }

  if (
    state.participants.get(buffer, offset + TRANSFER_PAYER_OFFSET, PAYER, 0)
    !== 1
  ) {
    return Result.PayerDoesNotExist;
  }
  if (
    state.participants.get(buffer, offset + TRANSFER_PAYEE_OFFSET, PAYEE, 0)
    !== 1
  ) {
    return Result.PayeeDoesNotExist;
  }

  if (
    Logic.equal(
      PAYER,
      PARTICIPANT_ID_OFFSET,
      PAYEE,
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
  const payer_net_debit_cap = PAYER.readUIntBE(
    PARTICIPANT_NET_DEBIT_CAP_OFFSET,
    6 // TODO 64-bit
  );
  const payer_position = PAYER.readUIntBE(
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
  Logic.adjustParticipantPosition(amount, PAYER, 0);
  state.participants.set(PAYER, 0, PAYER, 0);

  // TODO Assert insert is unique.
  state.transfers.set(buffer, offset + 0, buffer, offset + 0);
  return Result.OK;
};

const State = {};

// TODO Set elementsMin and elementsMax according to deployment config.
// TODO Iterate across hash table buffers and initialize to avoid page faults.
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

const ProcessBatch = function(buffer) {
  Journal.write(buffer);
  var offset = 0;
  while (offset < buffer.length) {
    assert(Logic.createTransfer(State, buffer, offset) === Result.OK);
    offset += TRANSFER;
  }
  assert(offset === buffer.length);
};

const Server = Node.net.createServer(
  function(socket) {
    console.log('client connected...');
    var batchSize = 0;
    var batchReceived = 0;
    var batch = [];
    socket.on('data',
      function(buffer) {
        // Quick and dirty parser:
        while (buffer.length) {
          if (batchSize === 0) {
            // TODO We hack and assume buffer always has at least 4 bytes.
            // This is not always true.
            batchSize = buffer.readUInt32BE(0);
            // console.log(`batchSize=${batchSize}`);
            if (batchSize === 0) {
              socket.end();
              Node.process.exit();
              return;
            }
            buffer = buffer.slice(4);
            // console.log(`buffer has ${buffer.length} bytes remaining`);
            if (buffer.length === 0) break;
          }
          var chunk = buffer.slice(0, batchSize - batchReceived);
          // console.log(`received chunk of ${chunk.length} bytes`);
          batch.push(chunk);
          batchReceived += chunk.length;
          buffer = buffer.slice(chunk.length);
          // console.log(`buffer has ${buffer.length} bytes remaining`);
          if (batchReceived === batchSize) {
            var received = batch.length === 1 ? batch[0] : Buffer.concat(batch);
            ProcessBatch(received);
            // console.log(`batch receive completed`);
            // console.log(`resetting receive state...`);
            socket.write('1');
            batchSize = 0;
            batchReceived = 0;
            batch = [];
          }
        }
      }
    );
    socket.on('end',
      function() {
        console.log('client disconnected.');
      }
    );

  }
);

Server.on('error', function(error) { console.error(error); });

Server.listen(PORT,
  function() {
    console.log(`listening on ${PORT}...`);
    if (!Node.process.argv[2]) {
      const child = Node.child.fork('stress.js');
    }
  }
);
