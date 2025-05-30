#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "tigerbeetle"

require "securerandom"

# make conversions easier
class String
  def from_uuid_to_int
    self.gsub("-", "").to_i(16)
  end
end

class Integer
  def to_uuid
    hex = self.to_s(16).rjust(32, "0")
    [hex[0..7], hex[8..11], hex[12..15], hex[16..19], hex[20..31]].join("-")
  end
end

# Constants from TigerBeetle C Bindings

TB_ACCOUNT_FLAGS = {
  LINKED: 1 << 0,
  DEBITS_MUST_NOT_EXCEED_CREDITS: 1 << 1,
  CREDITS_MUST_NOT_EXCEED_DEBITS: 1 << 2,
  HISTORY: 1 << 3,
  IMPORTED: 1 << 4,
  CLOSED: 1 << 5,
}

TB_ACCOUNT_FLAGS.each do |const, expected|
  actual = TigerBeetle::Bindings::AccountFlags.const_get(const)
  next if actual == expected

  raise "AccountFlag::#{const} expected #{expected} got #{actual}"
end


TB_TRANSFER_FLAGS = {
  LINKED: 1 << 0,
  PENDING: 1 << 1,
  POST_PENDING_TRANSFER: 1 << 2,
  VOID_PENDING_TRANSFER: 1 << 3,
  BALANCING_DEBIT: 1 << 4,
  BALANCING_CREDIT: 1 << 5,
  CLOSING_DEBIT: 1 << 6,
  CLOSING_CREDIT: 1 << 7,
  IMPORTED: 1 << 8,
}
TB_TRANSFER_FLAGS.each do |const, expected|
  actual = TigerBeetle::Bindings::TransferFlags.const_get(const)
  next if actual == expected

  raise "TransferFlags::#{const} expected #{expected} got #{actual}"
end

TB_CREATE_ACCOUNT_RESULT = {
  OK: 0,
  LINKED_EVENT_FAILED: 1,
  LINKED_EVENT_CHAIN_OPEN: 2,
  IMPORTED_EVENT_EXPECTED: 22,
  IMPORTED_EVENT_NOT_EXPECTED: 23,
  TIMESTAMP_MUST_BE_ZERO: 3,
  IMPORTED_EVENT_TIMESTAMP_OUT_OF_RANGE: 24,
  IMPORTED_EVENT_TIMESTAMP_MUST_NOT_ADVANCE: 25,
  # RESERVED_FIELD: 4,
  # RESERVED_FLAG: 5,
  ID_MUST_NOT_BE_ZERO: 6,
  ID_MUST_NOT_BE_INT_MAX: 7,
  EXISTS_WITH_DIFFERENT_FLAGS: 15,
  EXISTS_WITH_DIFFERENT_USER_DATA_128: 16,
  EXISTS_WITH_DIFFERENT_USER_DATA_64: 17,
  EXISTS_WITH_DIFFERENT_USER_DATA_32: 18,
  EXISTS_WITH_DIFFERENT_LEDGER: 19,
  EXISTS_WITH_DIFFERENT_CODE: 20,
  EXISTS: 21,
  FLAGS_ARE_MUTUALLY_EXCLUSIVE: 8,
  DEBITS_PENDING_MUST_BE_ZERO: 9,
  DEBITS_POSTED_MUST_BE_ZERO: 10,
  CREDITS_PENDING_MUST_BE_ZERO: 11,
  CREDITS_POSTED_MUST_BE_ZERO: 12,
  LEDGER_MUST_NOT_BE_ZERO: 13,
  CODE_MUST_NOT_BE_ZERO: 14,
  IMPORTED_EVENT_TIMESTAMP_MUST_NOT_REGRESS: 26,
}
TB_CREATE_ACCOUNT_RESULT.each do |const, expected|
  actual = TigerBeetle::Bindings::CreateAccountResult.const_get(const)
  next if actual == expected

  raise "CreateAccountResult::#{const} expected #{expected} got #{actual}"
end

TB_CREATE_TRANSFER_RESULT = {
  OK: 0,
  LINKED_EVENT_FAILED: 1,
  LINKED_EVENT_CHAIN_OPEN: 2,
  IMPORTED_EVENT_EXPECTED: 56,
  IMPORTED_EVENT_NOT_EXPECTED: 57,
  TIMESTAMP_MUST_BE_ZERO: 3,
  IMPORTED_EVENT_TIMESTAMP_OUT_OF_RANGE: 58,
  IMPORTED_EVENT_TIMESTAMP_MUST_NOT_ADVANCE: 59,
  # RESERVED_FLAG: 4,
  ID_MUST_NOT_BE_ZERO: 5,
  ID_MUST_NOT_BE_INT_MAX: 6,
  EXISTS_WITH_DIFFERENT_FLAGS: 36,
  EXISTS_WITH_DIFFERENT_PENDING_ID: 40,
  EXISTS_WITH_DIFFERENT_TIMEOUT: 44,
  EXISTS_WITH_DIFFERENT_DEBIT_ACCOUNT_ID: 37,
  EXISTS_WITH_DIFFERENT_CREDIT_ACCOUNT_ID: 38,
  EXISTS_WITH_DIFFERENT_AMOUNT: 39,
  EXISTS_WITH_DIFFERENT_USER_DATA_128: 41,
  EXISTS_WITH_DIFFERENT_USER_DATA_64: 42,
  EXISTS_WITH_DIFFERENT_USER_DATA_32: 43,
  EXISTS_WITH_DIFFERENT_LEDGER: 67,
  EXISTS_WITH_DIFFERENT_CODE: 45,
  EXISTS: 46,
  ID_ALREADY_FAILED: 68,
  FLAGS_ARE_MUTUALLY_EXCLUSIVE: 7,
  DEBIT_ACCOUNT_ID_MUST_NOT_BE_ZERO: 8,
  DEBIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX: 9,
  CREDIT_ACCOUNT_ID_MUST_NOT_BE_ZERO: 10,
  CREDIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX: 11,
  ACCOUNTS_MUST_BE_DIFFERENT: 12,
  PENDING_ID_MUST_BE_ZERO: 13,
  PENDING_ID_MUST_NOT_BE_ZERO: 14,
  PENDING_ID_MUST_NOT_BE_INT_MAX: 15,
  PENDING_ID_MUST_BE_DIFFERENT: 16,
  TIMEOUT_RESERVED_FOR_PENDING_TRANSFER: 17,
  CLOSING_TRANSFER_MUST_BE_PENDING: 64,
  LEDGER_MUST_NOT_BE_ZERO: 19,
  CODE_MUST_NOT_BE_ZERO: 20,
  DEBIT_ACCOUNT_NOT_FOUND: 21,
  CREDIT_ACCOUNT_NOT_FOUND: 22,
  ACCOUNTS_MUST_HAVE_THE_SAME_LEDGER: 23,
  TRANSFER_MUST_HAVE_THE_SAME_LEDGER_AS_ACCOUNTS: 24,
  PENDING_TRANSFER_NOT_FOUND: 25,
  PENDING_TRANSFER_NOT_PENDING: 26,
  PENDING_TRANSFER_HAS_DIFFERENT_DEBIT_ACCOUNT_ID: 27,
  PENDING_TRANSFER_HAS_DIFFERENT_CREDIT_ACCOUNT_ID: 28,
  PENDING_TRANSFER_HAS_DIFFERENT_LEDGER: 29,
  PENDING_TRANSFER_HAS_DIFFERENT_CODE: 30,
  EXCEEDS_PENDING_TRANSFER_AMOUNT: 31,
  PENDING_TRANSFER_HAS_DIFFERENT_AMOUNT: 32,
  PENDING_TRANSFER_ALREADY_POSTED: 33,
  PENDING_TRANSFER_ALREADY_VOIDED: 34,
  PENDING_TRANSFER_EXPIRED: 35,
  IMPORTED_EVENT_TIMESTAMP_MUST_NOT_REGRESS: 60,
  IMPORTED_EVENT_TIMESTAMP_MUST_POSTDATE_DEBIT_ACCOUNT: 61,
  IMPORTED_EVENT_TIMESTAMP_MUST_POSTDATE_CREDIT_ACCOUNT: 62,
  IMPORTED_EVENT_TIMEOUT_MUST_BE_ZERO: 63,
  DEBIT_ACCOUNT_ALREADY_CLOSED: 65,
  CREDIT_ACCOUNT_ALREADY_CLOSED: 66,
  OVERFLOWS_DEBITS_PENDING: 47,
  OVERFLOWS_CREDITS_PENDING: 48,
  OVERFLOWS_DEBITS_POSTED: 49,
  OVERFLOWS_CREDITS_POSTED: 50,
  OVERFLOWS_DEBITS: 51,
  OVERFLOWS_CREDITS: 52,
  OVERFLOWS_TIMEOUT: 53,
  EXCEEDS_CREDITS: 54,
  EXCEEDS_DEBITS: 55,
}
TB_CREATE_TRANSFER_RESULT.each do |const, expected|
  actual = TigerBeetle::Bindings::CreateTransferResult.const_get(const)
  next if actual == expected

  raise "CreateTransferResult::#{const} expected #{expected} got #{actual}"
end

# typedef struct tb_create_accounts_result_t {
#     uint32_t index;
#     uint32_t result;
# } tb_create_accounts_result_t;

# typedef struct tb_create_transfers_result_t {
#     uint32_t index;
#     uint32_t result;
# } tb_create_transfers_result_t;

# typedef struct tb_account_filter_t {
#     tb_uint128_t account_id;
#     tb_uint128_t user_data_128;
#     uint64_t user_data_64;
#     uint32_t user_data_32;
#     uint16_t code;
#     uint8_t reserved[58];
#     uint64_t timestamp_min;
#     uint64_t timestamp_max;
#     uint32_t limit;
#     uint32_t flags;
# } tb_account_filter_t;

TB_ACCOUNT_FILTER_FLAGS = {
  DEBITS: 1 << 0,
  CREDITS: 1 << 1,
  REVERSED: 1 << 2,
}
TB_ACCOUNT_FILTER_FLAGS.each do |const, expected|
  actual = TigerBeetle::Bindings::AccountFilterFlags.const_get(const)
  next if actual == expected

  raise "AccountFilterFlags::#{const} expected #{expected} got #{actual}"
end

# typedef struct tb_account_balance_t {
#     tb_uint128_t debits_pending;
#     tb_uint128_t debits_posted;
#     tb_uint128_t credits_pending;
#     tb_uint128_t credits_posted;
#     uint64_t timestamp;
#     uint8_t reserved[56];
# } tb_account_balance_t;

# typedef struct tb_query_filter_t {
#     tb_uint128_t user_data_128;
#     uint64_t user_data_64;
#     uint32_t user_data_32;
#     uint32_t ledger;
#     uint16_t code;
#     uint8_t reserved[6];
#     uint64_t timestamp_min;
#     uint64_t timestamp_max;
#     uint32_t limit;
#     uint32_t flags;
# } tb_query_filter_t;

TB_QUERY_FILTER_FLAGS = {
  REVERSED: 1 << 0,
}
TB_QUERY_FILTER_FLAGS.each do |const, expected|
  actual = TigerBeetle::Bindings::QueryFilterFlags.const_get(const)
  next if actual == expected

  raise "QueryFilterFlags::#{const} expected #{expected} got #{actual}"
end

# // Opaque struct serving as a handle for the client instance.
# // This struct must be "pinned" (not copyable or movable), as its address must remain stable
# // throughout the lifetime of the client instance.
# typedef struct tb_client_t {
#     uint64_t opaque[4];
# } tb_client_t;

# // Struct containing the state of a request submitted through the client.
# // This struct must be "pinned" (not copyable or movable), as its address must remain stable
# // throughout the lifetime of the request.
# typedef struct tb_packet_t {
#     void* user_data;
#     void* data;
#     uint32_t data_size;
#     uint16_t user_tag;
#     uint8_t operation;
#     uint8_t status;
#     uint8_t opaque[32];
# } tb_packet_t;

TB_OPERATION = {
  PULSE: 128,
  GET_CHANGE_EVENTS: 137,
  CREATE_ACCOUNTS: 138,
  CREATE_TRANSFERS: 139,
  LOOKUP_ACCOUNTS: 140,
  LOOKUP_TRANSFERS: 141,
  GET_ACCOUNT_TRANSFERS: 142,
  GET_ACCOUNT_BALANCES: 143,
  QUERY_ACCOUNTS: 144,
  QUERY_TRANSFERS: 145,
}
TB_OPERATION.each do |const, expected|
  actual = TigerBeetle::Bindings::Operation.const_get(const)
  next if actual == expected

  raise "Operation::#{const} expected #{expected} got #{actual}"
end

TB_PACKET_STATUS = {
  OK: 0,
  TOO_MUCH_DATA: 1,
  CLIENT_EVICTED: 2,
  CLIENT_RELEASE_TOO_LOW: 3,
  CLIENT_RELEASE_TOO_HIGH: 4,
  CLIENT_SHUTDOWN: 5,
  INVALID_OPERATION: 6,
  INVALID_DATA_SIZE: 7,
}
TB_PACKET_STATUS.each do |const, expected|
  actual = TigerBeetle::Bindings::PacketStatus.const_get(const)
  next if actual == expected

  raise "PacketStatus::#{const} expected #{expected} got #{actual}"
end

TB_INIT_STATUS = {
  SUCCESS: 0,
  UNEXPECTED: 1,
  OUT_OF_MEMORY: 2,
  ADDRESS_INVALID: 3,
  ADDRESS_LIMIT_EXCEEDED: 4,
  SYSTEM_RESOURCES: 5,
  NETWORK_SUBSYSTEM: 6,
}
TB_INIT_STATUS.each do |const, expected|
  actual = TigerBeetle::Bindings::InitStatus.const_get(const)
  next if actual == expected

  raise "InitStatus::#{const} expected #{expected} got #{actual}"
end

TB_CLIENT_STATUS = {
  OK: 0,
  INVALID: 1,
}
TB_CLIENT_STATUS.each do |const, expected|
  actual = TigerBeetle::Bindings::ClientStatus.const_get(const)
  next if actual == expected

  raise "ClientStatus::#{const} expected #{expected} got #{actual}"
end

TB_REGISTER_LOG_CALLBACK_STATUS = {
  SUCCESS: 0,
  ALREADY_REGISTERED: 1,
  NOT_REGISTERED: 2,
}
TB_REGISTER_LOG_CALLBACK_STATUS.each do |const, expected|
  actual = TigerBeetle::Bindings::RegisterLogCallbackStatus.const_get(const)
  next if actual == expected

  raise "RegisterLogCallbackStatus::#{const} expected #{expected} got #{actual}"
end

TB_LOG_LEVEL = {
  ERR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3,
}
TB_LOG_LEVEL.each do |const, expected|
  actual = TigerBeetle::Bindings::LogLevel.const_get(const)
  next if actual == expected

  raise "LogLevel::#{const} expected #{expected} got #{actual}"
end

# typedef struct tb_init_parameters_t {
#     tb_uint128_t cluster_id;
#     tb_uint128_t client_id;
#     uint8_t* addresses_ptr;
#     uint64_t addresses_len;
# } tb_init_parameters_t;

# // Initialize a new TigerBeetle client which connects to the addresses provided and
# // completes submitted packets by invoking the callback with the given context.
# TB_INIT_STATUS tb_client_init(
#     tb_client_t *client_out,
#     // 128-bit unsigned integer represented as a 16-byte little-endian array.
#     const uint8_t cluster_id[16],
#     const char *address_ptr,
#     uint32_t address_len,
#     uintptr_t completion_ctx,
#     void (*completion_callback)(uintptr_t, tb_packet_t*, uint64_t, const uint8_t*, uint32_t)
# );

# // Initialize a new TigerBeetle client that echoes back any submitted data.
# TB_INIT_STATUS tb_client_init_echo(
#     tb_client_t *client_out,
#     // 128-bit unsigned integer represented as a 16-byte little-endian array.
#     const uint8_t cluster_id[16],
#     const char *address_ptr,
#     uint32_t address_len,
#     uintptr_t completion_ctx,
#     void (*completion_callback)(uintptr_t, tb_packet_t*, uint64_t, const uint8_t*, uint32_t)
# );

# // Retrieve the parameters initially passed to `tb_client_init` or `tb_client_init_echo`.
# // Return value: `TB_CLIENT_OK` on success, or `TB_CLIENT_INVALID` if the client handle was
# // not initialized or has already been closed.
# TB_CLIENT_STATUS tb_client_init_parameters(
#     tb_client_t* client,
#     tb_init_parameters_t* init_parameters_out
# );

# // Retrieve the callback context initially passed to `tb_client_init` or `tb_client_init_echo`.
# // Return value: `TB_CLIENT_OK` on success, or `TB_CLIENT_INVALID` if the client handle was
# // not initialized or has already been closed.
# TB_CLIENT_STATUS tb_client_completion_context(
#     tb_client_t* client,
#     uintptr_t* completion_ctx_out
# );

# // Submit a packet with its `operation`, `data`, and `data_size` fields set.
# // Once completed, `completion_callback` will be invoked with `completion_ctx`
# // and the given packet on the `tb_client` thread (separate from the caller's thread).
# // Return value: `TB_CLIENT_OK` on success, or `TB_CLIENT_INVALID` if the client handle was
# // not initialized or has already been closed.
# TB_CLIENT_STATUS tb_client_submit(
#     tb_client_t *client,
#     tb_packet_t *packet
# );

# // Closes the client, causing any previously submitted packets to be completed with
# // `TB_PACKET_CLIENT_SHUTDOWN` before freeing any allocated client resources from init.
# // Return value: `TB_CLIENT_OK` on success, or `TB_CLIENT_INVALID` if the client handle was
# // not initialized or has already been closed.
# TB_CLIENT_STATUS tb_client_deinit(
#     tb_client_t *client
# );

# // Registers or unregisters the application log callback.
# TB_REGISTER_LOG_CALLBACK_STATUS tb_client_register_log_callback(
#     void (*callback)(TB_LOG_LEVEL, const uint8_t*, uint32_t),
#     bool debug
# );
#

# typedef struct tb_account_t {
#     tb_uint128_t id;
#     tb_uint128_t debits_pending;
#     tb_uint128_t debits_posted;
#     tb_uint128_t credits_pending;
#     tb_uint128_t credits_posted;
#     tb_uint128_t user_data_128;
#     uint64_t user_data_64;
#     uint32_t user_data_32;
#     uint32_t reserved;
#     uint32_t ledger;
#     uint16_t code;
#     uint16_t flags;
#     uint64_t timestamp;
# } tb_account_t;

def test_account
  account_id = SecureRandom.uuid_v7
  debits_pending = 1000
  debits_posted = 2000
  credits_pending = 3000
  credits_posted = 4000
  user_data_128 = SecureRandom.uuid_v7
  user_data_64 = 5000
  user_data_32 = 6000
  ledger = 1
  code = 1234
  account = TigerBeetle::Bindings::Account.new(
    id: account_id.from_uuid_to_int,
    debits_pending:,
    debits_posted:,
    credits_pending:,
    credits_posted:,
    user_data_128: user_data_128.from_uuid_to_int,
    user_data_64:,
    user_data_32:,
    ledger:,
    code:,
  )
  if account.id.to_uuid != account_id
    raise "Account ID mismatch: expected #{account_id}, got #{account.id.to_uuid}"
  end
  if account.debits_pending != debits_pending
    raise "Debits pending mismatch: expected #{debits_pending}, got #{account.debits_pending}"
  end
  if account.debits_posted != debits_posted
    raise "Debits posted mismatch: expected #{debits_posted}, got #{account.debits_posted}"
  end
  if account.credits_pending != credits_pending
    raise "Credits pending mismatch: expected #{credits_pending}, got #{account.credits_pending}"
  end
  if account.credits_posted != credits_posted
    raise "Credits posted mismatch: expected #{credits_posted}, got #{account.credits_posted}"
  end
  if account.user_data_128.to_uuid != user_data_128
    raise "User data 128 mismatch: expected #{user_data_128}, got #{account.user_data_128.to_uuid}"
  end
  if account.user_data_64 != user_data_64
    raise "User data 64 mismatch: expected #{user_data_64}, got #{account.user_data_64}"
  end
  if account.user_data_32 != user_data_32
    raise "User data 32 mismatch: expected #{user_data_32}, got #{account.user_data_32}"
  end
  if account.ledger != ledger
    raise "Ledger mismatch: expected #{ledger}, got #{account.ledger}"
  end
  if account.code != code
    raise "Code mismatch: expected #{code}, got #{account.code}"
  end

  if account.respond_to?(:reserved) || account.respond_to?(:reserved=)
    raise "Account struct should not have reserved field, but it does"
  end
end
test_account

# typedef struct tb_transfer_t {
#     tb_uint128_t id;
#     tb_uint128_t debit_account_id;
#     tb_uint128_t credit_account_id;
#     tb_uint128_t amount;
#     tb_uint128_t pending_id;
#     tb_uint128_t user_data_128;
#     uint64_t user_data_64;
#     uint32_t user_data_32;
#     uint32_t timeout;
#     uint32_t ledger;
#     uint16_t code;
#     uint16_t flags;
#     uint64_t timestamp;
# } tb_transfer_t;
def test_transfer
  transfer_id = SecureRandom.uuid_v7
  debit_account_id = SecureRandom.uuid_v7
  credit_account_id = SecureRandom.uuid_v7
  amount = 1000
  pending_id = SecureRandom.uuid_v7
  user_data_128 = SecureRandom.uuid_v7
  user_data_64 = 5000
  user_data_32 = 6000
  ledger = 1
  code = 1234
  transfer = TigerBeetle::Bindings::Transfer.new(
    id: transfer_id.from_uuid_to_int,
    debit_account_id: debit_account_id.from_uuid_to_int,
    credit_account_id: credit_account_id.from_uuid_to_int,
    amount:,
    pending_id: pending_id.from_uuid_to_int,
    user_data_128: user_data_128.from_uuid_to_int,
    user_data_64:,
    user_data_32:,
    ledger:,
    code:,
  )
  if transfer.id.to_uuid != transfer_id
    raise "Account ID mismatch: expected #{transfer_id}, got #{transfer.id.to_uuid}"
  end
  if transfer.debit_account_id.to_uuid != debit_account_id
    raise "Debit account ID mismatch: expected #{debit_account_id}, got #{transfer.debit_account_id.to_uuid}"
  end
  if transfer.credit_account_id.to_uuid != credit_account_id
    raise "Credit account ID mismatch: expected #{credit_account_id}, got #{transfer.credit_account_id.to_uuid}"
  end
  if transfer.amount != amount
    raise "Amount mismatch: expected #{amount}, got #{transfer.amount}"
  end
  if transfer.pending_id.to_uuid != pending_id
    raise "Pending ID mismatch: expected #{pending_id}, got #{transfer.pending_id.to_uuid}"
  end
  if transfer.user_data_128.to_uuid != user_data_128
    raise "User data 128 mismatch: expected #{user_data_128}, got #{transfer.user_data_128.to_uuid}"
  end
  if transfer.user_data_64 != user_data_64
    raise "User data 64 mismatch: expected #{user_data_64}, got #{transfer.user_data_64}"
  end
  if transfer.user_data_32 != user_data_32
    raise "User data 32 mismatch: expected #{user_data_32}, got #{transfer.user_data_32}"
  end
  if transfer.ledger != ledger
    raise "Ledger mismatch: expected #{ledger}, got #{transfer.ledger}"
  end
  if transfer.code != code
    raise "Code mismatch: expected #{code}, got #{transfer.code}"
  end
end
test_transfer


ADDRESSES = ENV.fetch("TB_ADDRESSES", "3000").to_s
CLUSTER_ID = ENV.fetch("TB_CLUSTER_ID", "0").to_i
client = TigerBeetle.connect(addresses: ADDRESSES, cluster_id: CLUSTER_ID)

empty_lookup = client.lookup_accounts([10000])
if empty_lookup.size != 0
  raise "Expected empty lookup for non-existent account, got #{empty_lookup.size} results"
end

puts "*"* 80
puts "  SUCCESS: All TigerBeetle constants match expected values\n"
