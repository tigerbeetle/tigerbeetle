const assert = require('assert');
const fs = require('fs');

const TRANSFER                  = 128;
const TRANSFER_ID               = 16;
const TRANSFER_DEBIT_ID         = 16;
const TRANSFER_CREDIT_ID        = 16;
const TRANSFER_CUSTOM_1         = 16;
const TRANSFER_CUSTOM_2         = 16;
const TRANSFER_CUSTOM_3         = 16;
const TRANSFER_FLAGS            = 8;
const TRANSFER_AMOUNT           = 8;
const TRANSFER_TIMEOUT          = 8;
const TRANSFER_TIMESTAMP        = 8;

const TRANSFER_ID_OFFSET        = 0;
const TRANSFER_DEBIT_ID_OFFSET  = 0 + 16;
const TRANSFER_CREDIT_ID_OFFSET = 0 + 16 + 16;
const TRANSFER_CUSTOM_1_OFFSET  = 0 + 16 + 16 + 16;
const TRANSFER_CUSTOM_2_OFFSET  = 0 + 16 + 16 + 16 + 16;
const TRANSFER_CUSTOM_3_OFFSET  = 0 + 16 + 16 + 16 + 16 + 16;
const TRANSFER_FLAGS_OFFSET     = 0 + 16 + 16 + 16 + 16 + 16 + 16;
const TRANSFER_AMOUNT_OFFSET    = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8;
const TRANSFER_TIMEOUT_OFFSET   = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 8;
const TRANSFER_TIMESTAMP_OFFSET = 0 + 16 + 16 + 16 + 16 + 16 + 16 + 8 + 8 + 8;

assert(TRANSFER_TIMESTAMP_OFFSET + TRANSFER_TIMESTAMP === TRANSFER);

const bytes = fs.readFileSync('transfer');

function hex(bytes, offset, size) {
  return bytes.toString('hex', offset, offset + size);
}

// We use 32-bit integers here for demo purposes to avoid the hassle of 64-bits.
const transfer = {
  id: hex(bytes, TRANSFER_ID_OFFSET, TRANSFER_ID),
  debit_id: hex(bytes, TRANSFER_DEBIT_ID_OFFSET, TRANSFER_DEBIT_ID),
  credit_id: hex(bytes, TRANSFER_CREDIT_ID_OFFSET, TRANSFER_CREDIT_ID),
  custom_1: hex(bytes, TRANSFER_CUSTOM_1_OFFSET, TRANSFER_CUSTOM_1),
  custom_2: hex(bytes, TRANSFER_CUSTOM_2_OFFSET, TRANSFER_CUSTOM_2),
  custom_3: hex(bytes, TRANSFER_CUSTOM_3_OFFSET, TRANSFER_CUSTOM_3),
  flags: bytes.readUInt32LE(TRANSFER_FLAGS_OFFSET),
  amount: bytes.readUInt32LE(TRANSFER_AMOUNT_OFFSET),
  timeout: bytes.readUInt32LE(TRANSFER_TIMEOUT_OFFSET),
  timestamp: bytes.readUInt32LE(TRANSFER_TIMESTAMP_OFFSET)
};
  
console.log(`id=${transfer.id} flags=${transfer.flags} amount=${transfer.amount} timeout=${transfer.timeout} timestamp=${transfer.timestamp}`);
