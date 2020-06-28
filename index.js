const assert = require('assert');

const Node = {
  fs: require('fs')
};

const Crypto = require('@ronomon/crypto-async');
const HashTable = require('@ronomon/hash-table');
const Queue = require('@ronomon/queue');

// You need to create and preallocate an empty journal using:
// dd < /dev/zero bs=1048576 count=256 > journal

// 10,000 foot overview:
// 1. Receive a batch of transfers from the network.
// 2. Write these sequentially to disk and fsync.
// 3. Apply business logic and apply to in-memory state.

const Journal = {};

Journal.fd = Node.fs.openSync('journal', 'r+');
Journal.position = 0;

Journal.queue = new Queue(1);
Journal.queue.onData = function(batch, end) {
  // TODO: Batch Records
  // TODO: Checksums
  // TODO: Snapshot
  // TODO: Wrap around journal file
  const self = Journal;
  const buffer = batch.buffer;
  // Simulate CPU cost of checkum:
  Crypto.hash('sha256', buffer);
  assert(buffer.length > 0);
  assert(buffer.length % 64 === 0);
  Node.fs.write(self.fd, buffer, 0, buffer.length, self.position,
    function(error, bytesWritten) {
      assert(!error);
      assert(bytesWritten === buffer.length);
      Node.fs.fsync(self.fd,
        function(error) {
          assert(!error);
          self.position += buffer.length;
          // TODO: See if we can make a single function call here:
          end();
          batch.end();
        }
      );
    }
  );
};

Journal.push = function(batch) {
  const self = this;
  self.queue.push(batch);
};

const Participant = {};

Participant.table = new HashTable(8, 24, 1000, 10000);

(function() {
  // TODO: We want to be using constants instead of magic numbers.
  const payer_id = Buffer.alloc(8, 1);
  const payee_id = Buffer.alloc(8, 2);
  const net_debit_cap = Buffer.alloc(8);
  net_debit_cap.writeUIntBE(10000000, 0, 6);
  const position = Buffer.alloc(8);
  position.writeUIntBE(2000000, 0, 6);

  assert(
    Participant.table.set(
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
    Participant.table.set(
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

const Transfer = {};

Transfer.table = new HashTable(16, 64, 10000000, 256000000);
// TODO: Iterate across hash table buffers and initialize to avoid page faults.

Transfer.create = function(buffer, offset) {
  // transfer {
  //                 id: 16 bytes (128-bit)
  //           payer_id:  8 bytes ( 64-bit)
  //           payee_id:  8 bytes ( 64-bit)
  //             amount:  8 bytes ( 64-bit)
  //   expire timestamp:  6 bytes ( 48-bit)
  //   create timestamp:  6 bytes ( 48-bit) [reserved]
  //   commit timestamp:  6 bytes ( 48-bit) [reserved]
  //        userdata_id:  6 bytes ( 48-bit) [reserved]
  // }
  assert(Number.isSafeInteger(offset));
  assert(offset >= 0 && offset % 64 === 0);
  assert(offset + 64 <= buffer.length);

  assert(Transfer.table.exist(buffer, offset + 0) === 0);
  assert(Participant.table.exist(buffer, offset + 16) === 1);
  assert(Participant.table.exist(buffer, offset + 24) === 1);
  // TODO: More validations (see BUSINESS_LOGIC.json).

  assert(Transfer.table.set(buffer, offset + 0, buffer, offset + 0) === 0);
};

const transfers = Node.fs.readFileSync('transfers');
assert(transfers.length % 64 === 0);
const count = transfers.length / 64;
const batch_size = 64 * 1000;

const start = Date.now();
const queue = new Queue(1);
queue.onData = function(buffer, end) {
  // TODO: Improve this interface.
  Journal.push({
    buffer: buffer,
    end: function() {
      var offset = 0;
      while (offset < buffer.length) {
        Transfer.create(buffer, offset);
        offset += 64;
      }
      assert(offset === buffer.length);
      end();
    }
  });
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
