#include "ruby.h"
#include "ruby/thread.h"
#include "tb_client.h"
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    tb_client_t client;
    _Atomic bool closed;
    pthread_mutex_t mutex;
} rb_tb_client_t;

typedef struct rb_tb_request {
    rb_tb_client_t *client;
    uint8_t *send_buf;
    uint8_t *recv_buf;
    uint32_t recv_size;
    bool recv_oom;
    uint8_t status; // TB_PACKET_STATUS
    bool done;
    bool abandoned; // Set by UBF. Client owns cleanup if true.
    pthread_cond_t cond;
    tb_packet_t packet;
} rb_tb_request_t;

static VALUE rb_eInitError;
static VALUE rb_eClientClosedError;
static VALUE rb_ePacketError;

static VALUE rb_mTigerBeetle;

static void rb_tb_init_error_classes(VALUE mTigerBeetle);
static void rb_tb_init_native_client(VALUE mTigerBeetle);

void Init_tigerbeetle(void) {
    rb_mTigerBeetle = rb_define_module("TigerBeetle");
    rb_gc_register_address(&rb_mTigerBeetle);
    rb_tb_init_error_classes(rb_mTigerBeetle);
    rb_tb_init_native_client(rb_mTigerBeetle);
}

static void rb_tb_client_free(void *ptr) {
    rb_tb_client_t *client = ptr;
    if (!atomic_load(&client->closed)) {
        atomic_store(&client->closed, true);
        tb_client_deinit(&client->client);
    }
    // tb_client_deinit completes all in-flight callbacks before returning,
    // so no requests are in flight here and the mutex can be safely destroyed.
    pthread_mutex_destroy(&client->mutex);
    ruby_xfree(client);
}

static size_t rb_tb_client_memsize(const void *ptr) {
    (void)ptr;
    return sizeof(rb_tb_client_t);
}

static const rb_data_type_t rb_tb_client_type = {
    .wrap_struct_name = "TigerBeetle::NativeClient",
    .function =
        {
            // The struct contains no Ruby values, nothing to mark.
            .dmark = NULL,
            .dfree = rb_tb_client_free,
            .dsize = rb_tb_client_memsize,
        },
    .flags = RUBY_TYPED_FREE_IMMEDIATELY,
};

static VALUE rb_tb_client_alloc(VALUE klass) {
    rb_tb_client_t *data = ruby_xmalloc(sizeof(rb_tb_client_t));
    memset(data, 0, sizeof(rb_tb_client_t));
    atomic_store(&data->closed, true);
    if (pthread_mutex_init(&data->mutex, NULL) != 0) {
        ruby_xfree(data);
        rb_raise(rb_eRuntimeError, "pthread_mutex_init failed");
    }
    return TypedData_Wrap_Struct(klass, &rb_tb_client_type, data);
}

// Called from the tb_client internal thread — no GVL.
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
    rb_tb_client_t *client = req->client;

    uint8_t *recv_buf = NULL;
    uint32_t recv_size = 0;
    bool recv_oom = false;
    // result is only valid for this callback's duration, so copy on success
    if (packet->status == TB_PACKET_OK && result != NULL && result_size > 0) {
        recv_buf = malloc(result_size);
        if (recv_buf) {
            memcpy(recv_buf, result, result_size);
            recv_size = result_size;
        } else {
            recv_oom = true;
        }
    }

    pthread_mutex_lock(&client->mutex);

    if (req->abandoned) {
        // UBF fired, callback owns all cleanup.
        pthread_mutex_unlock(&client->mutex);
        if (recv_buf)
            free(recv_buf);
        free(req->send_buf);
        pthread_cond_destroy(&req->cond);
        free(req);
        return;
    }

    req->status = packet->status;
    req->recv_buf = recv_buf;
    req->recv_size = recv_size;
    req->recv_oom = recv_oom;
    req->done = true;
    pthread_cond_signal(&req->cond);
    pthread_mutex_unlock(&client->mutex);
}

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

static VALUE rb_tb_client_initialize(VALUE self, VALUE cluster_id_rb, VALUE addresses_rb) {
    rb_tb_client_t *client;
    TypedData_Get_Struct(self, rb_tb_client_t, &rb_tb_client_type, client);

    if (!atomic_load(&client->closed))
        rb_raise(rb_eRuntimeError, "already initialized");

    uint8_t cluster_id_bytes[16] = {0};
    rb_integer_pack(cluster_id_rb, cluster_id_bytes, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);

    const char *addr = StringValueCStr(addresses_rb);
    uint32_t addr_len = (uint32_t)RSTRING_LEN(addresses_rb);

    TB_INIT_STATUS status = tb_client_init(
        &client->client, cluster_id_bytes, addr, addr_len, (uintptr_t)client, rb_tb_on_completion
    );

    if (status != TB_INIT_SUCCESS) {
        rb_raise(rb_eInitError, "tb_client_init failed: %s", tb_init_error_message(status));
    }
    atomic_store(&client->closed, false);
    return self;
}

static void *rb_tb_deinit_without_gvl(void *arg) {
    tb_client_deinit((tb_client_t *)arg);
    return NULL;
}

static VALUE rb_tb_client_close(VALUE self) {
    rb_tb_client_t *client;
    TypedData_Get_Struct(self, rb_tb_client_t, &rb_tb_client_type, client);
    pthread_mutex_lock(&client->mutex);
    if (atomic_load(&client->closed)) {
        pthread_mutex_unlock(&client->mutex);
        return Qnil;
    }
    atomic_store(&client->closed, true);
    pthread_mutex_unlock(&client->mutex);
    // deinit blocks until all in-flight callbacks complete. The GVL is released
    // while waiting so other Ruby threads can run.
    rb_thread_call_without_gvl(rb_tb_deinit_without_gvl, &client->client, NULL, NULL);
    return Qnil;
}

static VALUE rb_tb_client_closed_p(VALUE self) {
    rb_tb_client_t *client;
    TypedData_Get_Struct(self, rb_tb_client_t, &rb_tb_client_type, client);
    return atomic_load(&client->closed) ? Qtrue : Qfalse;
}

static size_t rb_tb_event_size(TB_OPERATION operation) {
    switch (operation) {
    case TB_OPERATION_CREATE_ACCOUNTS:
        return sizeof(tb_account_t);
    case TB_OPERATION_CREATE_TRANSFERS:
        return sizeof(tb_transfer_t);
    case TB_OPERATION_LOOKUP_ACCOUNTS:
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

// TODO: Cache all these rb_intern calls so they only happen at init time

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
    VALUE cClass = rb_const_get(mTigerBeetle, rb_intern("CreateAccountResult"));
    for (long i = 0; i < count; i++) {
        const tb_create_account_result_t *res =
            (const tb_create_account_result_t *)(buf + i * sizeof(tb_create_account_result_t));
        VALUE obj = rb_obj_alloc(cClass);
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
    VALUE cClass = rb_const_get(mTigerBeetle, rb_intern("CreateTransferResult"));
    for (long i = 0; i < count; i++) {
        const tb_create_transfer_result_t *res =
            (const tb_create_transfer_result_t *)(buf + i * sizeof(tb_create_transfer_result_t));
        VALUE obj = rb_obj_alloc(cClass);
        rb_ivar_set(obj, rb_intern("@timestamp"), ULL2NUM(res->timestamp));
        rb_ivar_set(obj, rb_intern("@status"), UINT2NUM(res->status));
        rb_ary_push(results, obj);
    }
    return results;
}

static VALUE rb_tb_deserialize_accounts(VALUE mTigerBeetle, const uint8_t *buf, uint32_t buf_size) {
    long count = (long)(buf_size / sizeof(tb_account_t));
    VALUE results = rb_ary_new_capa(count);
    VALUE cClass = rb_const_get(mTigerBeetle, rb_intern("Account"));
    for (long i = 0; i < count; i++) {
        const tb_account_t *a = (const tb_account_t *)(buf + i * sizeof(tb_account_t));
        VALUE obj = rb_obj_alloc(cClass);
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

static VALUE rb_tb_deserialize(TB_OPERATION operation, const uint8_t *buf, uint32_t buf_size) {
    switch (operation) {
    case TB_OPERATION_CREATE_ACCOUNTS:
        return rb_tb_deserialize_create_accounts(rb_mTigerBeetle, buf, buf_size);
    case TB_OPERATION_CREATE_TRANSFERS:
        return rb_tb_deserialize_create_transfers(rb_mTigerBeetle, buf, buf_size);
    case TB_OPERATION_LOOKUP_ACCOUNTS:
        return rb_tb_deserialize_accounts(rb_mTigerBeetle, buf, buf_size);
    default:
        rb_raise(rb_eRuntimeError, "unsupported operation: %d", (int)operation);
    }
}

static rb_tb_request_t *rb_tb_client_submit_request_init(
    rb_tb_client_t *client, uint8_t *send_buf, size_t send_size_bytes, TB_OPERATION operation
) {
    rb_tb_request_t *req = malloc(sizeof(rb_tb_request_t));
    if (!req) {
        free(send_buf);
        rb_raise(rb_eNoMemError, "failed to allocate request");
    }
    memset(req, 0, sizeof(rb_tb_request_t));
    req->client = client;
    req->send_buf = send_buf;

    if (pthread_cond_init(&req->cond, NULL) != 0) {
        free(send_buf);
        free(req);
        rb_raise(rb_eRuntimeError, "pthread_cond_init failed");
    }

    req->packet.data = send_buf;
    req->packet.data_size = (uint32_t)send_size_bytes;
    req->packet.operation = (uint8_t)operation;
    req->packet.user_data = req;

    return req;
}

// Called without GVL: submits the packet and waits for the completion callback.
static void *rb_tb_client_submit_without_gvl(void *arg) {
    rb_tb_request_t *req = (rb_tb_request_t *)arg;
    rb_tb_client_t *client = req->client;

    TB_CLIENT_STATUS status = tb_client_submit(&client->client, &req->packet);

    pthread_mutex_lock(&client->mutex);
    if (status == TB_CLIENT_INVALID) {
        req->status = TB_PACKET_CLIENT_SHUTDOWN;
        req->done = true;
        pthread_cond_signal(&req->cond);
    }
    while (!req->done) {
        pthread_cond_wait(&req->cond, &client->mutex);
    }
    pthread_mutex_unlock(&client->mutex);
    return NULL;
}

// UBF: called when the thread receives an interrupt (Thread#raise, Timeout).
static void rb_tb_client_submit_ubf(void *arg) {
    rb_tb_request_t *req = (rb_tb_request_t *)arg;
    pthread_mutex_lock(&req->client->mutex);
    req->abandoned = true;
    req->done = true;
    pthread_cond_signal(&req->cond);
    pthread_mutex_unlock(&req->client->mutex);
}

static VALUE rb_tb_client_submit(VALUE self, VALUE operation_rb, VALUE items_rb) {
    rb_tb_client_t *client;
    TypedData_Get_Struct(self, rb_tb_client_t, &rb_tb_client_type, client);

    if (atomic_load(&client->closed)) {
        rb_raise(rb_eClientClosedError, "client is closed");
    }

    TB_OPERATION operation = (TB_OPERATION)NUM2INT(operation_rb);
    long count = RARRAY_LEN(items_rb);

    if (count == 0)
        return rb_ary_new();

    size_t event_size = rb_tb_event_size(operation);
    size_t send_size_bytes = event_size * (size_t)count;
    uint8_t *send_buf = malloc(send_size_bytes);
    if (!send_buf)
        rb_raise(rb_eNoMemError, "failed to allocate send buffer");
    rb_tb_serialize(operation, items_rb, send_buf, count);

    rb_tb_request_t *req =
        rb_tb_client_submit_request_init(client, send_buf, send_size_bytes, operation);

    rb_thread_call_without_gvl(rb_tb_client_submit_without_gvl, req, rb_tb_client_submit_ubf, req);

    pthread_mutex_lock(&client->mutex);
    if (req->abandoned) {
        // Callback will remove and free req.
        pthread_mutex_unlock(&client->mutex);
        rb_thread_check_ints(); // reraise pending interrupt
        return Qnil;
    }

    uint8_t pkt_status = req->status;
    uint8_t *recv_buf = req->recv_buf;
    uint32_t recv_size = req->recv_size;
    bool recv_oom = req->recv_oom;
    pthread_mutex_unlock(&client->mutex);

    pthread_cond_destroy(&req->cond);
    free(send_buf);
    free(req);

    if (pkt_status != TB_PACKET_OK) {
        if (recv_buf)
            free(recv_buf);
        if (pkt_status == TB_PACKET_CLIENT_SHUTDOWN)
            rb_raise(rb_eClientClosedError, "client was shut down");
        rb_raise(rb_ePacketError, "packet error status: %d", (int)pkt_status);
    }

    if (recv_oom) {
        rb_raise(rb_eNoMemError, "out of memory copying result in completion callback");
    }

    VALUE result = rb_tb_deserialize(operation, recv_buf, recv_size);
    free(recv_buf);
    return result;
}

static void rb_tb_init_error_classes(VALUE mTigerBeetle) {
    VALUE eStandardError = rb_const_get(rb_cObject, rb_intern("StandardError"));
    rb_eInitError = rb_define_class_under(mTigerBeetle, "InitError", eStandardError);
    rb_gc_register_address(&rb_eInitError);
    rb_eClientClosedError =
        rb_define_class_under(mTigerBeetle, "ClientClosedError", eStandardError);
    rb_gc_register_address(&rb_eClientClosedError);
    rb_ePacketError = rb_define_class_under(mTigerBeetle, "PacketError", eStandardError);
    rb_gc_register_address(&rb_ePacketError);
}

static void rb_tb_init_native_client(VALUE mTigerBeetle) {
    VALUE cNativeClient = rb_define_class_under(mTigerBeetle, "NativeClient", rb_cObject);
    rb_define_alloc_func(cNativeClient, rb_tb_client_alloc);
    rb_define_method(cNativeClient, "initialize", rb_tb_client_initialize, 2);
    rb_define_method(cNativeClient, "submit", rb_tb_client_submit, 2);
    rb_define_method(cNativeClient, "close", rb_tb_client_close, 0);
    rb_define_method(cNativeClient, "closed?", rb_tb_client_closed_p, 0);
}
