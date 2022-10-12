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

const bytes = fs.readFileSync('transfer');

function hex(bytes, offset, size) {
  return bytes.toString('hex', offset, offset + size);
}

// We use 32-bit integers here for demo purposes to avoid the hassle of 64-bits.
var transfer = {
  id: hex(bytes, TRANSFER_ID_OFFSET, TRANSFER_ID),
  debit_id: hex(bytes, TRANSFER_DEBIT_ID_OFFSET, TRANSFER_DEBIT_ID),
  credit_id: hex(bytes, TRANSFER_CREDIT_ID_OFFSET, TRANSFER_CREDIT_ID),
  user_data: hex(bytes, TRANSFER_USER_DATA_OFFSET, TRANSFER_USER_DATA),
  reserved: hex(bytes, TRANSFER_RESERVED_OFFSET, TRANSFER_RESERVED),
  pending_id: hex(bytes, TRANSFER_PENDING_ID_OFFSET, TRANSFER_PENDING_ID),
  timeout: bytes.readUInt32LE(TRANSFER_TIMEOUT_OFFSET),
  ledger: bytes.readUInt32LE(TRANSFER_LEDGER_OFFSET),
  code: bytes.readUInt16LE(TRANSFER_CODE_OFFSET),
  flags: bytes.readUInt16LE(TRANSFER_FLAGS_OFFSET),
  amount: bytes.readUInt32LE(TRANSFER_AMOUNT_OFFSET),
  timestamp: bytes.readUInt32LE(TRANSFER_TIMESTAMP_OFFSET)
};
  
console.log(`id=${transfer.id} flags=${transfer.flags} amount=${transfer.amount} timeout=${transfer.timeout} timestamp=${transfer.timestamp}`);
