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

const transfer = Buffer.alloc(TRANSFER);
transfer.write('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', TRANSFER_ID_OFFSET, 'hex');
transfer.write('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', TRANSFER_DEBIT_ID_OFFSET, 'hex');
transfer.write('cccccccccccccccccccccccccccccccc', TRANSFER_CREDIT_ID_OFFSET, 'hex');
transfer.write('dddddddddddddddddddddddddddddddd', TRANSFER_CUSTOM_1_OFFSET, 'hex');
transfer.write('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', TRANSFER_CUSTOM_2_OFFSET, 'hex');
transfer.write('ffffffffffffffffffffffffffffffff', TRANSFER_CUSTOM_3_OFFSET, 'hex');
transfer.writeUInt32LE(1 | 2, TRANSFER_FLAGS_OFFSET);
transfer.writeUInt32LE(1, TRANSFER_AMOUNT_OFFSET);
transfer.writeUInt32LE(777, TRANSFER_TIMEOUT_OFFSET);

const transfers = Buffer.alloc(TRANSFER * 16384);
var offset = 0;
while (offset < transfers.length) {
  offset += transfer.copy(transfers, offset);
}
fs.writeFileSync('transfers', transfers);
