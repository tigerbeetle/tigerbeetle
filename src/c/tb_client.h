#ifndef TB_CLIENT_C
#define TB_CLIENT_C

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

typedef __uint128_t tb_uint128_t;


typedef enum TB_ACCOUNT_FLAGS {
    TB_ACCOUNT_LINKED = 1 << 0,
    TB_ACCOUNT_DEBITS_MUST_NOT_EXCEED_CREDITS = 1 << 1,
    TB_ACCOUNT_CREDITS_MUST_NOT_EXCEED_DEBITS = 1 << 2,
} TB_ACCOUNT_FLAGS;

typedef struct tb_account_t {
    tb_uint128_t id;
    tb_uint128_t user_data;
    uint8_t reserved[48];
    uint16_t unit;
    uint16_t code;
    uint32_t flags;
    uint64_t debits_reserved;
    uint64_t debits_accepted;
    uint64_t credits_reserved;
    uint64_t credits_accepted;
    uint64_t timestamp;
} tb_account_t;

typedef enum TB_TRANSFER_FLAGS {
    TB_TRANSFER_LINKED = 1 << 0,
    TB_TRANSFER_TWO_PHASE_COMMIT = 1 << 1,
    TB_TRANSFER_CONDITION = 1 << 2,
} TB_TRANSFER_FLAGS;

typedef struct tb_transfer_t {
    tb_uint128_t id;
    tb_uint128_t debit_account_id;
    tb_uint128_t credit_account_id;
    tb_uint128_t user_data;
    uint8_t reserved[32];
    uint64_t timeout;
    uint32_t code;
    uint32_t flags;
    uint64_t amount;
    uint64_t timestamp;
} tb_transfer_t;

typedef enum TB_COMMIT_FLAGS {
    TB_COMMIT_LINKED = 1 << 0,
    TB_COMMIT_REJECT = 1 << 1,
    TB_COMMIT_PREIMAGE = 1 << 2,
} TB_COMMIT_FLAGS;

typedef struct tb_commit_t {
    tb_uint128_t id;
    uint8_t reserved[32];
    uint32_t code;
    uint32_t flags;
    uint64_t timestamp;
} tb_commit_t;

typedef enum TB_CREATE_ACCOUNT_RESULT {
    TB_CREATE_ACCOUNT_OK,
    TB_CREATE_ACCOUNT_LINKED_EVENT_FAILED,
    TB_CREATE_ACCOUNT_EXISTS,
    TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_USER_DATA,
    TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_RESERVED_FIELD,
    TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_UNIT,
    TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_CODE,
    TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_FLAGS,
    TB_CREATE_ACCOUNT_EXCEEDS_CREDITS,
    TB_CREATE_ACCOUNT_EXCEEDS_DEBITS,
    TB_CREATE_ACCOUNT_RESERVE_FAILED,
    TB_CREATE_ACCOUNT_RESERVE_FLAG_PADDING
} TB_CREATE_ACCOUNT_RESULT;

typedef enum TB_CREATE_TRANSFER_RESULT {
    TB_CREATE_TRANSFER_OK,
    TB_CREATE_TRANSFER_LINKED_EVENT_FAILED,
    TB_CREATE_TRANSFER_EXISTS,
    TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_DEBIT_ACCOUNT_ID,
    TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_CREDIT_ACCOUNT_ID,
    TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_USER_DATA,
    TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_RESERVED_FIELD,
    TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_CODE,
    TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_AMOUNT,
    TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_TIMEOUT,
    TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_FLAGS,
    TB_CREATE_TRANSFER_EXISTS_AND_ALREADY_COMMITTED_AND_ACCEPTED,
    TB_CREATE_TRANSFER_EXISTS_AND_ALREADY_COMMITTED_AND_REJECTED,
    TB_CREATE_TRANSFER_RESERVED_FIELD,
    TB_CREATE_TRANSFER_RESERVED_FLAG_PADDING,
    TB_CREATE_TRANSFER_DEBIT_ACCOUNT_NOT_FOUND,
    TB_CREATE_TRANSFER_CREDIT_ACCOUNT_NOT_FOUND,
    TB_CREATE_TRANSFER_ACCOUNTS_ARE_THE_SAME,
    TB_CREATE_TRANSFER_ACCOUNTS_HAVE_DIFFERENT_UNITS,
    TB_CREATE_TRANSFER_AMOUNT_IS_ZERO,
    TB_CREATE_TRANSFER_EXCEEDS_CREDITS,
    TB_CREATE_TRANSFER_EXCEEDS_DEBITS,
    TB_CREATE_TRANSFER_TWO_PHASE_COMMIT_MUST_TIMEOUT,
    TB_CREATE_TRANSFER_TIMEOUT_RESERVED_FOR_TWO_PHASE_COMMIT
} TB_CREATE_TRANSFER_RESULT;

typedef enum TB_COMMIT_TRANSFER_RESULT {
    TB_COMMIT_TRANSFER_OK,
    TB_COMMIT_TRANSFER_LINKED_EVENT_FAILED,
    TB_COMMIT_TRANSFER_RESERVE_FAILED,
    TB_COMMIT_TRANSFER_RESERVE_FLAG_PADDING,
    TB_COMMIT_TRANSFER_NOT_FOUND,
    TB_COMMIT_TRANSFER_NOT_TWO_PHASE_COMMIT,
    TB_COMMIT_TRANSFER_EXPIRED,
    TB_COMMIT_TRANSFER_ALREADY_COMMITTED,
    TB_COMMIT_TRANSFER_ALREADY_COMMITTED_BUT_ACCEPTED,
    TB_COMMIT_TRANSFER_ALREADY_COMMITTED_BUT_REJECTED,
    TB_COMMIT_TRANSFER_DEBIT_ACCOUNT_NOT_FOUND,
    TB_COMMIT_TRANSFER_CREDIT_ACCOUNT_NOT_FOUND,
    TB_COMMIT_TRANSFER_EXCEEDS_CREDITS,
    TB_COMMIT_TRANSFER_EXCEEDS_DEBITS,
    TB_COMMIT_TRANSFER_CONDITION_REQUIRES_PREIMAGE,
    TB_COMMIT_TRANSFER_PREIMAGE_REQUIRES_CONDITION,
    TB_COMMIT_TRANSFER_PREIMAGE_INVALID
} TB_COMMIT_TRANSFER_RESULT;

typedef struct tb_create_accounts_result_t {
    uint32_t index;
    TB_CREATE_ACCOUNT_RESULT result;
} tb_create_accounts_result_t;

typedef struct tb_create_transfers_result_t {
    uint32_t index;
    TB_CREATE_TRANSFER_RESULT result;
} tb_create_transfers_result_t;

typedef struct tb_commit_transfers_result_t {
    uint32_t index;
    TB_COMMIT_TRANSFER_RESULT result;
} tb_commit_transfers_result_t;

typedef enum TB_OPERATION {
    TB_OP_CREATE_ACCOUNTS = 3,
    TB_OP_CREATE_TRANSFERS = 4,
    TB_OP_COMMIT_TRANSFERS = 5,
    TB_OP_LOOKUP_ACCOUNTS = 6,
    TB_OP_LOOKUP_TRANSFERS = 7
} TB_OPERATION;

typedef enum TB_PACKET_STATUS {
    TB_PACKET_OK,
    TB_PACKET_TOO_MUCH_DATA,
    TB_PACKET_INVALID_OPERATION,
    TB_PACKET_INVALID_DATA_SIZE
} TB_PACKET_STATUS;

typedef struct tb_packet_t {
    struct tb_packet_t* next;
    void* user_data;
    uint8_t operation;
    uint8_t status;
    uint32_t data_size;
    void* data;
} tb_packet_t;

typedef struct tb_packet_list_t {
    struct tb_packet_t* head;
    struct tb_packet_t* tail;
} tb_packet_list_t;

typedef void* tb_client_t;

typedef enum TB_STATUS {
    TB_STATUS_SUCCESS = 0,
    TB_STATUS_UNEXPECTED = 1,
    TB_STATUS_OUT_OF_MEMORY = 2,
    TB_STATUS_INVALID_ADDRESS = 3,
    TB_STATUS_SYSTEM_RESOURCES = 4,
    TB_STATUS_NETWORK_SUBSYSTEM = 5,
} TB_STATUS;

TB_STATUS tb_client_init(
    tb_client_t* out_client,
    tb_packet_list_t* out_packets,
    uint32_t cluster_id,
    const char* address_ptr,
    uint32_t address_len,
    uint32_t num_packets,
    uintptr_t on_completion_ctx,
    void (*on_completion_fn)(uintptr_t, tb_client_t, tb_packet_t*, const uint8_t*, uint32_t)
);

void tb_client_submit(
    tb_client_t client,
    tb_packet_list_t* packets
);

void tb_client_deinit(
    tb_client_t client
);

#endif // TB_CLIENT_C