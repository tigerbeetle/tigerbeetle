const assert = require('assert');

const Node = {
  fs: require('fs'),
  net: require('net')
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

Logic.createTransfer = function(state, buffer, offset) {
  assert(Number.isSafeInteger(offset));
  assert(offset >= 0 && offset % TRANSFER === 0);
  assert(offset + TRANSFER <= buffer.length);

  if (state.transfers.exist(buffer, offset + TRANSFER_ID_OFFSET) === 1) {
    return Result.TransferExists;
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

  // TODO Assert payer and payee ids are different (byte-for-byte only).
  const amount = buffer.readUIntBE(
    offset + TRANSFER_AMOUNT_OFFSET,
    6 // TODO We have a 64-bit field but only use 48-bits because of JS limits.
  );
  if (amount === 0) return Result.TransferAmountZero;
  const payer_net_debit_cap = PAYER.readUIntBE(
    PARTICIPANT_NET_DEBIT_CAP_OFFSET,
    6 // TODO We have a 64-bit field but only use 48-bits because of JS limits.
  );
  const payer_position = PAYER.readUIntBE(
    PARTICIPANT_POSITION_OFFSET,
    6 // TODO We have a 64-bit field but only use 48-bits because of JS limits.
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

  assert(state.transfers.set(buffer, offset + 0, buffer, offset + 0) === 0);
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

const transfers = Node.fs.readFileSync('transfers');
assert(transfers.length % TRANSFER === 0);
const count = transfers.length / TRANSFER;
const batch_size = TRANSFER * 1000;

const start = Date.now();
const queue = new Queue(1);
queue.onData = function(buffer, end) {
  Journal.write(buffer,
    function(error) {
      assert(!error);
      var offset = 0;
      while (offset < buffer.length) {
        assert(Logic.createTransfer(State, buffer, offset) === Result.OK);
        offset += TRANSFER;
      }
      assert(offset === buffer.length);
      end();
    }
  );
};
queue.onEnd = function(error) {
  assert(!error);
  const ms = Date.now() - start;
  console.log(`persisted and prepared ${count} transfers in ${ms}ms`);
  console.log(`${Math.floor(count / (ms / 1000))} transfers per second`);
};

var offset = 0;
while (offset < transfers.length) {
  var buffer = transfers.slice(offset, offset + batch_size);
  queue.push(buffer);
  offset += buffer.length;
}
queue.end();
