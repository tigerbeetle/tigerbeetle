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

const transfer = Buffer.alloc(TRANSFER);
transfer.write('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', TRANSFER_ID_OFFSET, 'hex');
transfer.write('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', TRANSFER_DEBIT_ID_OFFSET, 'hex');
transfer.write('cccccccccccccccccccccccccccccccc', TRANSFER_CREDIT_ID_OFFSET, 'hex');
transfer.write('dddddddddddddddddddddddddddddddd', TRANSFER_USER_DATA_OFFSET, 'hex');
transfer.write('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', TRANSFER_RESERVED_OFFSET, 'hex');
transfer.write('ffffffffffffffffffffffffffffffff', TRANSFER_PENDING_ID_OFFSET, 'hex');
transfer.writeUInt32LE(777, TRANSFER_TIMEOUT_OFFSET);
transfer.writeUInt32LE(888, TRANSFER_LEDGER_OFFSET);
transfer.writeUInt16LE(999, TRANSFER_CODE_OFFSET);
transfer.writeUInt32LE(1 | 2, TRANSFER_FLAGS_OFFSET);
transfer.writeUInt32LE(1, TRANSFER_AMOUNT_OFFSET);
transfer.writeUInt32LE(1234, TRANSFER_TIMESTAMP_OFFSET);
const transfers = Buffer.alloc(TRANSFER * 16384);
var offset = 0;
while (offset < transfers.length) {
  offset += transfer.copy(transfers, offset);
}
fs.writeFileSync('transfers', transfers);
