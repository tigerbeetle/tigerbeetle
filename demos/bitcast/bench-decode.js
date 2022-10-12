const assert = require('assert');
const fs = require('fs');

const TRANSFER                  = 128;
const TRANSFER_ID               = 16;
const TRANSFER_DEBIT_ID         = 16;
const TRANSFER_CREDIT_ID        = 16;
const TRANSFER_USER_DATA        = 16;
const TRANSFER_RESERVED         = 16;
const TRANSFER_PENDING_ID       = 16;
const TRANSFER_TIMEOUT          = 8;
const TRANSFER_LEDGER           = 4;
const TRANSFER_CODE             = 2;
const TRANSFER_FLAGS            = 2;
const TRANSFER_AMOUNT           = 8;
const TRANSFER_TIMESTAMP        = 8;

const TRANSFER_ID_OFFSET         = 0;
const TRANSFER_DEBIT_ID_OFFSET   = 0 + 16;
const TRANSFER_CREDIT_ID_OFFSET  = 0 + 16 + 16;
const TRANSFER_USER_DATA_OFFSET  = 0 + 16 + 16 + 16;
const TRANSFER_RESERVED_OFFSET   = 0 + 16 + 16 + 16 + 16;
const TRANSFER_PENDING_ID_OFFSET = 0 + 16 + 16 + 16 + 16 + 16;
const TRANSFER_TIMEOUT_OFFSET    = 0 + 16 + 16 + 16 + 16 + 16 + 16;
const TRANSFER_LEDGER_OFFSET     = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8;
const TRANSFER_CODE_OFFSET       = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 4;
const TRANSFER_FLAGS_OFFSET      = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 4 + 2;
const TRANSFER_AMOUNT_OFFSET     = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 4 + 2 + 2;
const TRANSFER_TIMESTAMP_OFFSET  = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 4 + 2 + 2 + 8;

assert(TRANSFER_TIMESTAMP_OFFSET + TRANSFER_TIMESTAMP === TRANSFER);

const bytes = fs.readFileSync('transfers');

function id(bytes, offset, size) {
  return bytes.slice(offset, offset + size);
}

console.log('do this a few times to let v8 optimize...');

var loops = 10;
while (loops--) {
  var ms = Date.now();
  var sum = 0;
  var offset = 0;
  while (offset < bytes.length) {
    var slice = bytes.slice(offset, offset + 128);
    var transfer = {
      id: id(slice, TRANSFER_ID_OFFSET, TRANSFER_ID),
      debit_id: id(slice, TRANSFER_DEBIT_ID_OFFSET, TRANSFER_DEBIT_ID),
      credit_id: id(slice, TRANSFER_CREDIT_ID_OFFSET, TRANSFER_CREDIT_ID),
      user_data: id(slice, TRANSFER_USER_DATA_OFFSET, TRANSFER_USER_DATA),
      reserved: id(slice, TRANSFER_RESERVED_OFFSET, TRANSFER_RESERVED),
      pending_id: id(slice, TRANSFER_PENDING_ID_OFFSET, TRANSFER_PENDING_ID),
      timeout: slice.readUint32LE(TRANSFER_TIMEOUT_OFFSET),
      ledger: slice.readUInt32LE(TRANSFER_LEDGER_OFFSET),
      code: slice.readUInt16LE(TRANSFER_CODE_OFFSET),
      flags: slice.readUInt16LE(TRANSFER_FLAGS_OFFSET),
      amount: slice.readUInt32LE(TRANSFER_AMOUNT_OFFSET),
      timestamp: slice.readUInt32LE(TRANSFER_TIMESTAMP_OFFSET)
    };
    sum += transfer.amount;
    offset += 128;
  }
  console.log(`  js: sum of transfer amounts=${sum} ms=${Date.now() - ms}`);
}
