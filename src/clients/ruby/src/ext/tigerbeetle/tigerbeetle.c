#include "ruby.h"
#include "ruby/thread.h"
#include "tb_client.h"
#include <assert.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef enum {
    REQ_PENDING = 0,
    REQ_DONE = 1,
    REQ_ABANDONED = 2,
} rb_tb_request_state_t;

typedef struct rb_tb_request {
    // Set at init, read by callback.
    int write_fd;
    TB_OPERATION operation;
    uint8_t *send_buf;
    tb_packet_t packet;
    // Shared between Ruby thread and callback.
    _Atomic rb_tb_request_state_t state;
    // Written by callback, read by Ruby thread.
    uint8_t status;
    uint8_t *result;
    uint32_t result_size;
} rb_tb_request_t;

static VALUE rb_mTigerBeetle;
static VALUE rb_cRequest;
static VALUE rb_eInitError;

static void rb_tb_init_native_client(VALUE mTigerBeetle);

void Init_tigerbeetle(void) {
    rb_mTigerBeetle = rb_define_module("TigerBeetle");

    rb_eInitError = rb_define_class_under(rb_mTigerBeetle, "InitError", rb_eStandardError);
    rb_define_class_under(rb_mTigerBeetle, "ClientClosedError", rb_eStandardError);
    rb_define_class_under(rb_mTigerBeetle, "PacketError", rb_eStandardError);

    rb_tb_init_native_client(rb_mTigerBeetle);
}

#pragma region Ruby Bridge

static const char *tb_init_error_message(TB_INIT_STATUS status) {
    switch (status) {
    case TB_INIT_UNEXPECTED:
        return "unexpected internal error";
    case TB_INIT_OUT_OF_MEMORY:
        return "out of memory";
    case TB_INIT_ADDRESS_INVALID:
        return "invalid replica address";
    case TB_INIT_ADDRESS_LIMIT_EXCEEDED:
        return "too many replica addresses";
    case TB_INIT_SYSTEM_RESOURCES:
        return "insufficient system resources";
    case TB_INIT_NETWORK_SUBSYSTEM:
        return "network subsystem failed";
    default:
        return "init error";
    }
}

static size_t rb_tb_event_size(TB_OPERATION operation) {
    switch (operation) {
    case TB_OPERATION_CREATE_ACCOUNTS:
        return sizeof(tb_account_t);
    case TB_OPERATION_CREATE_TRANSFERS:
        return sizeof(tb_transfer_t);
    case TB_OPERATION_LOOKUP_ACCOUNTS:
    case TB_OPERATION_LOOKUP_TRANSFERS:
        return sizeof(tb_uint128_t);
    default:
        rb_raise(rb_eRuntimeError, "unsupported operation: %d", (int)operation);
        return 0; // unreachable
    }
}

static inline void pack_u128(VALUE v, void *dst) {
    rb_integer_pack(v, dst, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);
}

static inline VALUE unpack_u128(const void *src) {
    return rb_integer_unpack(src, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);
}

static void rb_tb_serialize_accounts(VALUE items_rb, uint8_t *buf, long count) {
    tb_account_t *accounts = (tb_account_t *)buf;
    for (long i = 0; i < count; i++) {
        VALUE item = RARRAY_AREF(items_rb, i);
        tb_account_t *a = &accounts[i];
        memset(a, 0, sizeof(tb_account_t));
        pack_u128(rb_ivar_get(item, rb_intern("@id")), &a->id);
        pack_u128(rb_ivar_get(item, rb_intern("@debits_pending")), &a->debits_pending);
        pack_u128(rb_ivar_get(item, rb_intern("@debits_posted")), &a->debits_posted);
        pack_u128(rb_ivar_get(item, rb_intern("@credits_pending")), &a->credits_pending);
        pack_u128(rb_ivar_get(item, rb_intern("@credits_posted")), &a->credits_posted);
        pack_u128(rb_ivar_get(item, rb_intern("@user_data_128")), &a->user_data_128);
        a->user_data_64 = NUM2ULL(rb_ivar_get(item, rb_intern("@user_data_64")));
        a->user_data_32 = NUM2UINT(rb_ivar_get(item, rb_intern("@user_data_32")));
        a->ledger = NUM2UINT(rb_ivar_get(item, rb_intern("@ledger")));
        a->code = (uint16_t)NUM2UINT(rb_ivar_get(item, rb_intern("@code")));
        a->flags = (uint16_t)NUM2UINT(rb_ivar_get(item, rb_intern("@flags")));
        a->timestamp = NUM2ULL(rb_ivar_get(item, rb_intern("@timestamp")));
    }
}

static void rb_tb_serialize_lookup_ids(VALUE items_rb, uint8_t *buf, long count) {
    tb_uint128_t *ids = (tb_uint128_t *)buf;
    for (long i = 0; i < count; i++) {
        rb_integer_pack(RARRAY_AREF(items_rb, i), &ids[i], 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);
    }
}

static void rb_tb_serialize_transfers(VALUE items_rb, uint8_t *buf, long count) {
    tb_transfer_t *transfers = (tb_transfer_t *)buf;
    for (long i = 0; i < count; i++) {
        VALUE item = RARRAY_AREF(items_rb, i);
        tb_transfer_t *t = &transfers[i];
        memset(t, 0, sizeof(tb_transfer_t));
        pack_u128(rb_ivar_get(item, rb_intern("@id")), &t->id);
        pack_u128(rb_ivar_get(item, rb_intern("@debit_account_id")), &t->debit_account_id);
        pack_u128(rb_ivar_get(item, rb_intern("@credit_account_id")), &t->credit_account_id);
        pack_u128(rb_ivar_get(item, rb_intern("@amount")), &t->amount);
        pack_u128(rb_ivar_get(item, rb_intern("@pending_id")), &t->pending_id);
        pack_u128(rb_ivar_get(item, rb_intern("@user_data_128")), &t->user_data_128);
        t->user_data_64 = NUM2ULL(rb_ivar_get(item, rb_intern("@user_data_64")));
        t->user_data_32 = NUM2UINT(rb_ivar_get(item, rb_intern("@user_data_32")));
        t->timeout = NUM2UINT(rb_ivar_get(item, rb_intern("@timeout")));
        t->ledger = NUM2UINT(rb_ivar_get(item, rb_intern("@ledger")));
        t->code = (uint16_t)NUM2UINT(rb_ivar_get(item, rb_intern("@code")));
        t->flags = (uint16_t)NUM2UINT(rb_ivar_get(item, rb_intern("@flags")));
        t->timestamp = NUM2ULL(rb_ivar_get(item, rb_intern("@timestamp")));
    }
}

static void rb_tb_serialize(TB_OPERATION operation, VALUE items_rb, uint8_t *buf, long count) {
    switch (operation) {
    case TB_OPERATION_CREATE_ACCOUNTS:
        rb_tb_serialize_accounts(items_rb, buf, count);
        break;
    case TB_OPERATION_CREATE_TRANSFERS:
        rb_tb_serialize_transfers(items_rb, buf, count);
        break;
    case TB_OPERATION_LOOKUP_ACCOUNTS:
    case TB_OPERATION_LOOKUP_TRANSFERS:
        rb_tb_serialize_lookup_ids(items_rb, buf, count);
        break;
    default:
        rb_raise(rb_eRuntimeError, "unsupported operation: %d", (int)operation);
    }
}

static VALUE
rb_tb_deserialize_create_accounts(VALUE mTigerBeetle, const uint8_t *buf, uint32_t buf_size) {
    long count = (long)(buf_size / sizeof(tb_create_account_result_t));
    VALUE results = rb_ary_new_capa(count);
    VALUE klass = rb_const_get(mTigerBeetle, rb_intern("CreateAccountResult"));
    const tb_create_account_result_t *items = (const tb_create_account_result_t *)buf;
    for (long i = 0; i < count; i++) {
        const tb_create_account_result_t *res = &items[i];
        VALUE obj = rb_obj_alloc(klass);
        rb_ivar_set(obj, rb_intern("@timestamp"), ULL2NUM(res->timestamp));
        rb_ivar_set(obj, rb_intern("@status"), UINT2NUM(res->status));
        rb_ary_push(results, obj);
    }
    return results;
}

static VALUE
rb_tb_deserialize_create_transfers(VALUE mTigerBeetle, const uint8_t *buf, uint32_t buf_size) {
    long count = (long)(buf_size / sizeof(tb_create_transfer_result_t));
    VALUE results = rb_ary_new_capa(count);
    VALUE klass = rb_const_get(mTigerBeetle, rb_intern("CreateTransferResult"));
    const tb_create_transfer_result_t *items = (const tb_create_transfer_result_t *)buf;
    for (long i = 0; i < count; i++) {
        const tb_create_transfer_result_t *res = &items[i];
        VALUE obj = rb_obj_alloc(klass);
        rb_ivar_set(obj, rb_intern("@timestamp"), ULL2NUM(res->timestamp));
        rb_ivar_set(obj, rb_intern("@status"), UINT2NUM(res->status));
        rb_ary_push(results, obj);
    }
    return results;
}

static VALUE rb_tb_deserialize_accounts(VALUE mTigerBeetle, const uint8_t *buf, uint32_t buf_size) {
    long count = (long)(buf_size / sizeof(tb_account_t));
    VALUE results = rb_ary_new_capa(count);
    VALUE klass = rb_const_get(mTigerBeetle, rb_intern("Account"));
    const tb_account_t *items = (const tb_account_t *)buf;
    for (long i = 0; i < count; i++) {
        const tb_account_t *a = &items[i];
        VALUE obj = rb_obj_alloc(klass);
        rb_ivar_set(obj, rb_intern("@id"), unpack_u128(&a->id));
        rb_ivar_set(obj, rb_intern("@debits_pending"), unpack_u128(&a->debits_pending));
        rb_ivar_set(obj, rb_intern("@debits_posted"), unpack_u128(&a->debits_posted));
        rb_ivar_set(obj, rb_intern("@credits_pending"), unpack_u128(&a->credits_pending));
        rb_ivar_set(obj, rb_intern("@credits_posted"), unpack_u128(&a->credits_posted));
        rb_ivar_set(obj, rb_intern("@user_data_128"), unpack_u128(&a->user_data_128));
        rb_ivar_set(obj, rb_intern("@user_data_64"), ULL2NUM(a->user_data_64));
        rb_ivar_set(obj, rb_intern("@user_data_32"), UINT2NUM(a->user_data_32));
        rb_ivar_set(obj, rb_intern("@ledger"), UINT2NUM(a->ledger));
        rb_ivar_set(obj, rb_intern("@code"), UINT2NUM(a->code));
        rb_ivar_set(obj, rb_intern("@flags"), UINT2NUM(a->flags));
        rb_ivar_set(obj, rb_intern("@timestamp"), ULL2NUM(a->timestamp));
        rb_ary_push(results, obj);
    }
    return results;
}

static VALUE
rb_tb_deserialize_transfers(VALUE mTigerBeetle, const uint8_t *buf, uint32_t buf_size) {
    long count = (long)(buf_size / sizeof(tb_transfer_t));
    VALUE results = rb_ary_new_capa(count);
    VALUE klass = rb_const_get(mTigerBeetle, rb_intern("Transfer"));
    const tb_transfer_t *items = (const tb_transfer_t *)buf;
    for (long i = 0; i < count; i++) {
        const tb_transfer_t *t = &items[i];
        VALUE obj = rb_obj_alloc(klass);
        rb_ivar_set(obj, rb_intern("@id"), unpack_u128(&t->id));
        rb_ivar_set(obj, rb_intern("@debit_account_id"), unpack_u128(&t->debit_account_id));
        rb_ivar_set(obj, rb_intern("@credit_account_id"), unpack_u128(&t->credit_account_id));
        rb_ivar_set(obj, rb_intern("@amount"), unpack_u128(&t->amount));
        rb_ivar_set(obj, rb_intern("@pending_id"), unpack_u128(&t->pending_id));
        rb_ivar_set(obj, rb_intern("@user_data_128"), unpack_u128(&t->user_data_128));
        rb_ivar_set(obj, rb_intern("@user_data_64"), ULL2NUM(t->user_data_64));
        rb_ivar_set(obj, rb_intern("@user_data_32"), UINT2NUM(t->user_data_32));
        rb_ivar_set(obj, rb_intern("@timeout"), UINT2NUM(t->timeout));
        rb_ivar_set(obj, rb_intern("@ledger"), UINT2NUM(t->ledger));
        rb_ivar_set(obj, rb_intern("@code"), UINT2NUM(t->code));
        rb_ivar_set(obj, rb_intern("@flags"), UINT2NUM(t->flags));
        rb_ivar_set(obj, rb_intern("@timestamp"), ULL2NUM(t->timestamp));
        rb_ary_push(results, obj);
    }
    return results;
}

static VALUE rb_tb_deserialize(TB_OPERATION operation, const uint8_t *buf, uint32_t buf_size) {
    switch (operation) {
    case TB_OPERATION_CREATE_ACCOUNTS:
        return rb_tb_deserialize_create_accounts(rb_mTigerBeetle, buf, buf_size);
    case TB_OPERATION_CREATE_TRANSFERS:
        return rb_tb_deserialize_create_transfers(rb_mTigerBeetle, buf, buf_size);
    case TB_OPERATION_LOOKUP_ACCOUNTS:
        return rb_tb_deserialize_accounts(rb_mTigerBeetle, buf, buf_size);
    case TB_OPERATION_LOOKUP_TRANSFERS:
        return rb_tb_deserialize_transfers(rb_mTigerBeetle, buf, buf_size);
    default:
        rb_raise(rb_eRuntimeError, "unsupported operation: %d", (int)operation);
    }
}

static void rb_tb_request_free(void *ptr) {
    rb_tb_request_t *req = (rb_tb_request_t *)ptr;
    rb_tb_request_state_t expected = REQ_PENDING;
    if (atomic_compare_exchange_strong(&req->state, &expected, REQ_ABANDONED)) {
        // Callback hasn't fired yet. It will free req once it does.
        return;
    }
    // Callback already fired (state == REQ_DONE), we own cleanup.
    assert(expected == REQ_DONE);
    free(req->result);
    free(req);
}

static size_t rb_tb_request_size(const void *ptr) {
    (void)ptr;
    return sizeof(rb_tb_request_t);
}

static const rb_data_type_t rb_tb_request_type = {
    .wrap_struct_name = "TigerBeetle::Request",
    .function =
        {
            .dmark = NULL,
            .dfree = rb_tb_request_free,
            .dsize = rb_tb_request_size,
        },
    .flags = RUBY_TYPED_FREE_IMMEDIATELY,
};

// Request is only ever created from C via TypedData_Wrap_Struct.
static VALUE rb_tb_request_alloc(VALUE klass) {
    (void)klass;
    rb_raise(rb_eTypeError, "TigerBeetle::Request cannot be instantiated directly");
    return Qnil; // unreachable
}

static VALUE rb_tb_request_result(VALUE self) {
    rb_tb_request_t *req;
    TypedData_Get_Struct(self, rb_tb_request_t, &rb_tb_request_type, req);

    VALUE result = Qnil;
    if (req->status == TB_PACKET_OK) {
        if (req->result_size == 0) {
            result = rb_ary_new();
        } else {
            assert(req->result != NULL);
            result = rb_tb_deserialize(req->operation, req->result, req->result_size);
        }
    }

    VALUE ary = rb_ary_new_capa(2);
    rb_ary_push(ary, UINT2NUM(req->status));
    rb_ary_push(ary, result);
    return ary;
}

static void rb_tb_on_completion(
    uintptr_t completion_ctx,
    tb_packet_t *packet,
    uint64_t timestamp,
    const uint8_t *result,
    uint32_t result_size
) {
    (void)completion_ctx;
    (void)timestamp;
    rb_tb_request_t *req = (rb_tb_request_t *)packet->user_data;

    free(req->send_buf);
    req->send_buf = NULL;
    req->status = packet->status;

    if (packet->status == TB_PACKET_OK) {
        req->result_size = result_size;
        assert(result != NULL);

        if (result_size > 0) {
            req->result = malloc(result_size);
            assert(req->result != NULL);
            memcpy(req->result, result, result_size);
        }
    }

    rb_tb_request_state_t expected = REQ_PENDING;
    if (!atomic_compare_exchange_strong(&req->state, &expected, REQ_DONE)) {
        // dfree already ran (state == REQ_ABANDONED). We own cleanup.
        assert(expected == REQ_ABANDONED);
        close(req->write_fd);
        free(req->result);
        free(req);
        return;
    }

    // write_fd is O_NONBLOCK (see rb_tb_client_submit).
    write(req->write_fd, "\x01", 1);
    close(req->write_fd);
}

#pragma endregion

#pragma region Native Client

static void rb_tb_client_free(void *ptr) {
    tb_client_deinit((tb_client_t *)ptr);
    ruby_xfree(ptr);
}

static size_t rb_tb_client_size(const void *ptr) {
    (void)ptr;
    return sizeof(tb_client_t);
}

static const rb_data_type_t rb_tb_client_type = {
    .wrap_struct_name = "TigerBeetle::NativeClient",
    .function =
        {
            .dmark = NULL,
            .dfree = rb_tb_client_free,
            .dsize = rb_tb_client_size,
        },
    .flags = RUBY_TYPED_FREE_IMMEDIATELY,
};

static VALUE rb_tb_client_alloc(VALUE klass) {
    tb_client_t *data = ruby_xmalloc(sizeof(tb_client_t));
    memset(data, 0, sizeof(tb_client_t));
    return TypedData_Wrap_Struct(klass, &rb_tb_client_type, data);
}

static VALUE rb_tb_client_initialize(VALUE self, VALUE cluster_id_rb, VALUE addresses_rb) {
    tb_client_t *client;
    TypedData_Get_Struct(self, tb_client_t, &rb_tb_client_type, client);

    uint8_t cluster_id_bytes[16] = {0};
    rb_integer_pack(cluster_id_rb, cluster_id_bytes, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);

    const char *addr = StringValueCStr(addresses_rb);
    uint32_t addr_len = (uint32_t)RSTRING_LEN(addresses_rb);

    TB_INIT_STATUS status =
        tb_client_init(client, cluster_id_bytes, addr, addr_len, 0, rb_tb_on_completion);

    if (status != TB_INIT_SUCCESS) {
        rb_raise(rb_eInitError, "tb_client_init failed: %s", tb_init_error_message(status));
    }
    return self;
}

static void *rb_tb_deinit_without_gvl(void *arg) {
    tb_client_deinit((tb_client_t *)arg);
    return NULL;
}

static VALUE rb_tb_client_close(VALUE self) {
    tb_client_t *client;
    TypedData_Get_Struct(self, tb_client_t, &rb_tb_client_type, client);
    // deinit blocks until all in-flight callbacks complete. The GVL is released
    // while waiting so other Ruby threads can run.
    rb_thread_call_without_gvl(rb_tb_deinit_without_gvl, client, NULL, NULL);
    return Qnil;
}

static VALUE
rb_tb_client_submit(VALUE self, VALUE operation_rb, VALUE items_rb, VALUE write_fd_rb) {
    tb_client_t *client;
    TypedData_Get_Struct(self, tb_client_t, &rb_tb_client_type, client);

    TB_OPERATION operation = (TB_OPERATION)NUM2INT(operation_rb);
    long count = RARRAY_LEN(items_rb);
    int write_fd = NUM2INT(write_fd_rb);
    int callback_fd = dup(write_fd);
    if (callback_fd < 0) {
        rb_raise(rb_eRuntimeError, "out of file descriptors");
    }

    rb_tb_request_t *req = malloc(sizeof(rb_tb_request_t));
    if (!req) {
        close(callback_fd);
        rb_raise(rb_eNoMemError, "failed to allocate request");
    }
    memset(req, 0, sizeof(rb_tb_request_t));
    req->write_fd = callback_fd;
    req->operation = operation;

    if (count > 0) {
        size_t event_size = rb_tb_event_size(operation);
        size_t send_size = event_size * (size_t)count;
        req->send_buf = malloc(send_size);
        if (!req->send_buf) {
            free(req);
            rb_raise(rb_eNoMemError, "failed to allocate send buffer");
        }
        // Question: Is it worth wrapping this in rb_protect? Serialization
        // failures are caller errors before submission.
        rb_tb_serialize(operation, items_rb, req->send_buf, count);
        req->packet.data_size = (uint32_t)send_size;
    }

    req->packet.data = req->send_buf;
    req->packet.operation = (uint8_t)operation;
    req->packet.user_data = req;

    int fl = fcntl(callback_fd, F_GETFL, 0);
    fcntl(callback_fd, F_SETFL, fl | O_NONBLOCK);

    TB_CLIENT_STATUS cs = tb_client_submit(client, &req->packet);
    if (cs == TB_CLIENT_INVALID) {
        // Client already closed, so callback won't fire. Signal ourselves.
        free(req->send_buf);
        req->send_buf = NULL;
        req->status = TB_PACKET_CLIENT_SHUTDOWN;
        rb_tb_request_state_t expected = REQ_PENDING;
        atomic_compare_exchange_strong(&req->state, &expected, REQ_DONE);
        write(req->write_fd, "\x01", 1);
        close(req->write_fd);
    }

    return TypedData_Wrap_Struct(rb_cRequest, &rb_tb_request_type, req);
}

static void rb_tb_init_native_client(VALUE mTigerBeetle) {
    rb_define_const(mTigerBeetle, "PACKET_OK", INT2NUM(TB_PACKET_OK));
    rb_define_const(mTigerBeetle, "PACKET_CLIENT_SHUTDOWN", INT2NUM(TB_PACKET_CLIENT_SHUTDOWN));

    VALUE cNativeClient = rb_define_class_under(mTigerBeetle, "NativeClient", rb_cObject);
    rb_define_alloc_func(cNativeClient, rb_tb_client_alloc);
    rb_define_method(cNativeClient, "initialize", rb_tb_client_initialize, 2);
    rb_define_method(cNativeClient, "submit", rb_tb_client_submit, 3);
    rb_define_method(cNativeClient, "close", rb_tb_client_close, 0);

    rb_cRequest = rb_define_class_under(mTigerBeetle, "Request", rb_cObject);
    rb_define_alloc_func(rb_cRequest, rb_tb_request_alloc);
    rb_define_method(rb_cRequest, "result", rb_tb_request_result, 0);
}

#pragma endregion
