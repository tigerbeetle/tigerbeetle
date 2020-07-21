const assert = require('assert');

const Node = {
  fs: require('fs'),
  net: require('net'),
  process: process
};

const Crypto = require('@ronomon/crypto-async');

const HOST = '127.0.0.1'; //'tb.perf.openafrica.network';
const PORT = 30000;

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

const COMMIT_TRANSFER                  = 48;
const COMMIT_TRANSFER_ID               = 16;
const COMMIT_TRANSFER_PREIMAGE         = 32;

const COMMIT_TRANSFER_ID_OFFSET        = 0;
const COMMIT_TRANSFER_PREIMAGE_OFFSET  = 0 + 16;

const PARTICIPANT                      = 24;
const PARTICIPANT_ID                   = 8;
const PARTICIPANT_NET_DEBIT_CAP        = 8;
const PARTICIPANT_POSITION             = 8;

const PARTICIPANT_ID_OFFSET            = 0;
const PARTICIPANT_NET_DEBIT_CAP_OFFSET = 0 + 8;
const PARTICIPANT_POSITION_OFFSET      = 0 + 8 + 8;

const COMMAND_CREATE_TRANSFERS = 1;
const COMMAND_ACCEPT_TRANSFERS = 2;
const COMMAND_REJECT_TRANSFERS = 3;
const COMMAND_ACK = 4;

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

const CHECKSUM = Buffer.alloc(32); // We truncate 256-bit checksums to 128-bit.

const TigerBeetle = {};

// BATCHING:

TigerBeetle.CREATE_TRANSFERS = {
  command: COMMAND_CREATE_TRANSFERS,
  jobs: [],
  timestamp: 0,
  timeout: 0
};

TigerBeetle.ACCEPT_TRANSFERS = {
  command: COMMAND_ACCEPT_TRANSFERS,
  jobs: [],
  timestamp: 0,
  timeout: 0
};

// Add an incoming prepare `buffer` to a batch of prepares.
// `callback` will be called once persisted to TigerBeetle.
TigerBeetle.create = function(buffer, callback) {
  const self = this;
  self.push(self.CREATE_TRANSFERS, buffer, callback);
};

// Add an incoming fulfill `buffer` to a batch of fulfills.
// `callback` will be called once persisted to TigerBeetle.
TigerBeetle.accept = function(buffer, callback) {
  const self = this;
  self.push(self.ACCEPT_TRANSFERS, buffer, callback);
};

TigerBeetle.push = function(batch, buffer, callback) {
  const self = this;
  batch.jobs.push(new TigerBeetle.Job(buffer, callback));
  if (batch.timeout === 0) {
    batch.timestamp = Date.now();
    batch.timeout = setTimeout(
      function() {
        self.execute(batch);
      },
      50 // Wait up to N ms for a batch to be collected...
    );
  } else if (batch.jobs.length > 200) {
    // ... and stop waiting as soon as we have at least N jobs ready to go.
    // This gives us a more consistent performance picture and lets us avoid
    // any non-multiple latency spikes from the timeout.
    clearTimeout(batch.timeout);
    self.execute(batch);
  }
};

TigerBeetle.execute = function(batch) {
  const ms = Date.now() - batch.timestamp;
  // Cache reference to jobs array so we can reset the batch:
  const jobs = batch.jobs;
  // Reset the batch to start collecting a new batch:
  // We collect the new batch while commiting the previous batch to TigerBeetle.
  batch.jobs = [];
  batch.timestamp = 0;
  batch.timeout = 0;
  
  // TODO Use another function to do this to avoid using batch.jobs by mistake:
  const command = batch.command; // This is a constant.
  assert(
    command === COMMAND_CREATE_TRANSFERS || command === COMMAND_ACCEPT_TRANSFERS
  );
  const size = jobs[0].buffer.length;
  const buffer = Buffer.alloc(jobs.length * size);
  let offset = 0;
  for (var index = 0; index < jobs.length; index++) {
    var copied = jobs[index].buffer.copy(buffer, offset);
    assert(copied === size);
    offset += copied;
  }
  assert(offset === buffer.length);
  const start = Date.now();
  TigerBeetle.send(command, buffer,
    function(error) {
      assert(!error);
      const ms = Date.now() - start;
      console.log(`${ms}ms to send ${jobs.length} jobs to TigerBeetle`);
      for (var index = 0; index < jobs.length; index++) {
        jobs[index].callback();
      }
    }
  );
};

TigerBeetle.Job = function(buffer, callback) {
  this.buffer = buffer;
  this.callback = callback;
};

// ENCODING:

TigerBeetle.encodeCreate = function(object) {
  // Convert a Mojaloop prepare payload to a TigerBeetle transfer:
  
  // Mojaloop Prepare {
  //   "transferId":"dee82759-8f75-47d2-9822-7b6e371d34e0",
  //   "payerFsp":"payer",
  //   "payeeFsp":"payee",
  //   "amount":{"amount":"1","currency":"USD"},
  //   "ilpPacket":"AQAAAAAAAADIEHByaXZhdGUucGF5ZWVmc3CCAiB7InRyYW5zYWN0aW9uSWQiOiIyZGY3NzRlMi1mMWRiLTRmZjctYTQ5NS0yZGRkMzdhZjdjMmMiLCJxdW90ZUlkIjoiMDNhNjA1NTAtNmYyZi00NTU2LThlMDQtMDcwM2UzOWI4N2ZmIiwicGF5ZWUiOnsicGFydHlJZEluZm8iOnsicGFydHlJZFR5cGUiOiJNU0lTRE4iLCJwYXJ0eUlkZW50aWZpZXIiOiIyNzcxMzgwMzkxMyIsImZzcElkIjoicGF5ZWVmc3AifSwicGVyc29uYWxJbmZvIjp7ImNvbXBsZXhOYW1lIjp7fX19LCJwYXllciI6eyJwYXJ0eUlkSW5mbyI6eyJwYXJ0eUlkVHlwZSI6Ik1TSVNETiIsInBhcnR5SWRlbnRpZmllciI6IjI3NzEzODAzOTExIiwiZnNwSWQiOiJwYXllcmZzcCJ9LCJwZXJzb25hbEluZm8iOnsiY29tcGxleE5hbWUiOnt9fX0sImFtb3VudCI6eyJjdXJyZW5jeSI6IlVTRCIsImFtb3VudCI6IjIwMCJ9LCJ0cmFuc2FjdGlvblR5cGUiOnsic2NlbmFyaW8iOiJERVBPU0lUIiwic3ViU2NlbmFyaW8iOiJERVBPU0lUIiwiaW5pdGlhdG9yIjoiUEFZRVIiLCJpbml0aWF0b3JUeXBlIjoiQ09OU1VNRVIiLCJyZWZ1bmRJbmZvIjp7fX19",
  //   "condition":"HOr22-H3AfTDHrSkPjJtVPRdKouuMkDXTR4ejlQa8Ks",
  //   "expiration":"2022-02-02T02:02:02.020Z"
  // }

  // TigerBeetle Prepare {
  //                 id: 16 bytes (128-bit)
  //           payer_id:  8 bytes ( 64-bit)
  //           payee_id:  8 bytes ( 64-bit)
  //             amount:  8 bytes ( 64-bit)
  //   expire timestamp:  6 bytes ( 48-bit)
  //   create timestamp:  6 bytes ( 48-bit) [reserved]
  //   commit timestamp:  6 bytes ( 48-bit) [reserved]
  //        userdata_id:  6 bytes ( 48-bit) [reserved]
  // }
  
  const buffer = Buffer.alloc(TRANSFER);

  // TODO Optimize hyphen removal at predetermined indices:
  const transferId = object.transferId.replace(/[^a-fA-F0-9]/g, '');
  assert(transferId.length === 32);
  buffer.write(
    transferId,
    TRANSFER_ID_OFFSET,
    TRANSFER_ID,
    'hex'
  );

  // TODO Use actual participant ids instead of pre-allocated ids:
  buffer.fill(1, TRANSFER_PAYER_OFFSET, TRANSFER_PAYER_OFFSET + TRANSFER_PAYER);
  buffer.fill(2, TRANSFER_PAYEE_OFFSET, TRANSFER_PAYEE_OFFSET + TRANSFER_PAYEE);
  
  // TODO Parse amounts containing decimal places:
  assert(/^\d+$/.test(object.amount.amount));
  buffer.writeUIntBE(
    parseInt(object.amount.amount, 10),
    TRANSFER_AMOUNT_OFFSET,
    6 // TODO 64-bit
  );

  assert(
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(object.expiration)
  );
  buffer.writeUIntBE(
    new Date(object.expiration).getTime(),
    TRANSFER_EXPIRE_TIMESTAMP_OFFSET,
    TRANSFER_EXPIRE_TIMESTAMP
  );

  // TODO Add support for ILP packets and conditions.

  return buffer;
};

TigerBeetle.encodeAccept = function(transferId, object) {
  const buffer = Buffer.alloc(COMMIT_TRANSFER);

  // {
  //   fulfilment: 'XoSz1cL0tljJSCp_VtIYmPNw-zFUgGfbUqf69AagUzY',
  //   transferState: 'COMMITTED',
  //   completedTimestamp: '2020-07-21T17:25:09.808Z'
  // }

  // TODO Optimize hyphen removal at predetermined indices:  
  transferId = transferId.replace(/[^a-fA-F0-9]/g, '');
  assert(transferId.length === 32);
  buffer.write(
    transferId,
    COMMIT_TRANSFER_ID_OFFSET,
    COMMIT_TRANSFER_ID,
    'hex'
  );
  
  // TODO Preimage (base64)
  // TODO Result
  
  return buffer;
};

// NETWORKING:

// TODO Reconnect on connection resets with exponential backoff and jitter.
TigerBeetle.connect = function(host, port, end) {
  const self = this;
  assert(self.socket === null);
  self.socket = Node.net.createConnection(port, host,
    function() {
      console.log('connected to tiger beetle master');
      self.socket.on('data',
        function(buffer) {
          var length = buffer.length;
          while (length >= 64) {
            // TODO Process server result codes for the batch.
            const callback = self.sending.shift();
            callback();
            length -= 64;
          }
          // TODO Concatenate partial buffers.
          assert(length === 0);
        }
      );
      self.socket.on('end',
        function() {
          throw new Error('disconnected from tiger beetle master');
        }
      );
      end();
    }
  );
};

// TODO Queue network requests above the connection layer for limited retries.
TigerBeetle.send = function(command, data, end) {
  const self = this;
  assert(self.socket !== null);
  const header = Buffer.alloc(NETWORK_HEADER);
  // TODO Use a stream cipher CPRNG to write a random header ID instead of 0:
  header.writeUIntBE(0, NETWORK_HEADER_ID_OFFSET, 6);
  MAGIC.copy(header, NETWORK_HEADER_MAGIC_OFFSET);
  header.writeUInt32BE(command, NETWORK_HEADER_COMMAND_OFFSET);
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
  self.socket.write(header);
  self.socket.write(data);
  self.sending.push(end); // TODO Use a hash table to match IDs to callbacks.
};

TigerBeetle.sending = [];

TigerBeetle.socket = null;

module.exports = TigerBeetle;
