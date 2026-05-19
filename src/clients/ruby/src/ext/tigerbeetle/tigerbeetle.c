#include "rb_tb_gen.h"
#include "ruby.h"
#include "ruby/thread.h"
#include "tb_client.h"
#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
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
static VALUE rb_eClientClosedError;

static void rb_tb_init_native_client(VALUE mTigerBeetle);

void Init_tigerbeetle(void) {
    rb_mTigerBeetle = rb_define_module("TigerBeetle");

    rb_eInitError = rb_define_class_under(rb_mTigerBeetle, "InitError", rb_eStandardError);
    rb_eClientClosedError =
        rb_define_class_under(rb_mTigerBeetle, "ClientClosedError", rb_eStandardError);
    rb_define_class_under(rb_mTigerBeetle, "PacketError", rb_eStandardError);

    rb_tb_init_native_client(rb_mTigerBeetle);
}

#pragma region Ruby Bridge

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

static void rb_tb_write_completion(int fd, rb_tb_request_t *req) {
    uint64_t request_id = (uint64_t)(uintptr_t)req;
    ssize_t written = write(fd, &request_id, sizeof(request_id));
    (void)written;
    assert(written == sizeof(request_id));
}

static void rb_tb_on_completion(
    uintptr_t completion_ctx,
    tb_packet_t *packet,
    uint64_t timestamp,
    const uint8_t *result,
    uint32_t result_size
) {
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
        free(req->result);
        free(req);
        return;
    }

    rb_tb_write_completion((int)completion_ctx, req);
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

static VALUE rb_tb_client_initialize(
    VALUE self, VALUE cluster_id_rb, VALUE addresses_rb, VALUE completion_fd_rb
) {
    tb_client_t *client;
    TypedData_Get_Struct(self, tb_client_t, &rb_tb_client_type, client);

    uint8_t cluster_id_bytes[16] = {0};
    rb_integer_pack(cluster_id_rb, cluster_id_bytes, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);

    const char *addr = StringValueCStr(addresses_rb);
    uint32_t addr_len = (uint32_t)RSTRING_LEN(addresses_rb);

    int completion_fd = NUM2INT(completion_fd_rb);

    TB_INIT_STATUS status = tb_client_init(
        client, cluster_id_bytes, addr, addr_len, (uintptr_t)completion_fd, rb_tb_on_completion
    );

    if (status != TB_INIT_SUCCESS) {
        rb_raise(rb_eInitError, "Init error: %s", rb_tb_init_error_message(status));
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

typedef struct rb_tb_serialize_context {
    TB_OPERATION operation;
    VALUE items_rb;
    uint8_t *buf;
    long count;
} rb_tb_serialize_context_t;

static VALUE rb_tb_serialize_protected(VALUE context_value) {
    rb_tb_serialize_context_t *context = (rb_tb_serialize_context_t *)context_value;
    rb_tb_serialize(context->operation, context->items_rb, context->buf, context->count);
    return Qnil;
}

static VALUE rb_tb_client_submit(VALUE self, VALUE operation_rb, VALUE items_rb) {
    tb_client_t *client;
    TypedData_Get_Struct(self, tb_client_t, &rb_tb_client_type, client);

    TB_OPERATION operation = (TB_OPERATION)NUM2INT(operation_rb);
    long count = RARRAY_LEN(items_rb);

    rb_tb_request_t *req = malloc(sizeof(rb_tb_request_t));
    if (!req) {
        rb_raise(rb_eNoMemError, "failed to allocate request");
    }
    memset(req, 0, sizeof(rb_tb_request_t));
    req->operation = operation;

    if (count > 0) {
        size_t event_size = rb_tb_event_size(operation);
        size_t send_size = event_size * (size_t)count;
        req->send_buf = malloc(send_size);
        if (!req->send_buf) {
            free(req);
            rb_raise(rb_eNoMemError, "failed to allocate send buffer");
        }

        rb_tb_serialize_context_t serialize_context = {
            .operation = operation,
            .items_rb = items_rb,
            .buf = req->send_buf,
            .count = count,
        };
        int serialize_state = 0;
        rb_protect(rb_tb_serialize_protected, (VALUE)&serialize_context, &serialize_state);
        if (serialize_state) {
            free(req->send_buf);
            free(req);
            rb_jump_tag(serialize_state);
        }

        req->packet.data_size = (uint32_t)send_size;
    }

    req->packet.data = req->send_buf;
    req->packet.operation = (uint8_t)operation;
    req->packet.user_data = req;

    TB_CLIENT_STATUS cs = tb_client_submit(client, &req->packet);
    if (cs == TB_CLIENT_INVALID) {
        free(req->send_buf);
        free(req);
        rb_raise(rb_eClientClosedError, "client is closed");
    }

    return TypedData_Wrap_Struct(rb_cRequest, &rb_tb_request_type, req);
}

static VALUE rb_tb_request_id(VALUE self) {
    rb_tb_request_t *req;
    TypedData_Get_Struct(self, rb_tb_request_t, &rb_tb_request_type, req);
    return ULL2NUM((unsigned long long)(uintptr_t)req);
}

static void rb_tb_init_native_client(VALUE mTigerBeetle) {
    rb_define_const(mTigerBeetle, "PACKET_OK", INT2NUM(TB_PACKET_OK));
    rb_define_const(mTigerBeetle, "PACKET_CLIENT_SHUTDOWN", INT2NUM(TB_PACKET_CLIENT_SHUTDOWN));

    VALUE cNativeClient = rb_define_class_under(mTigerBeetle, "NativeClient", rb_cObject);
    rb_define_alloc_func(cNativeClient, rb_tb_client_alloc);
    rb_define_method(cNativeClient, "initialize", rb_tb_client_initialize, 3);
    rb_define_method(cNativeClient, "submit", rb_tb_client_submit, 2);
    rb_define_method(cNativeClient, "close", rb_tb_client_close, 0);

    rb_cRequest = rb_define_class_under(mTigerBeetle, "Request", rb_cObject);
    rb_define_alloc_func(rb_cRequest, rb_tb_request_alloc);
    rb_define_method(rb_cRequest, "id", rb_tb_request_id, 0);
    rb_define_method(rb_cRequest, "result", rb_tb_request_result, 0);
}

#pragma endregion
