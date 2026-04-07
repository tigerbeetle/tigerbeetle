#include "ruby.h"
#include "tb_client.h"
#include <stdatomic.h>

typedef struct {
    tb_client_t client;
    _Atomic bool closed;
} rb_tb_client_t;

static VALUE rb_tb_client_alloc(VALUE klass);
static VALUE rb_tb_client_initialize(VALUE self, VALUE cluster_id_rb, VALUE addresses_rb);
static VALUE rb_tb_client_close(VALUE self);
static VALUE rb_tb_client_closed_p(VALUE self);

void Init_tigerbeetle(void) {
    VALUE mTigerBeetle = rb_define_module("TigerBeetle");
    VALUE cNativeClient = rb_define_class_under(mTigerBeetle, "NativeClient", rb_cObject);
    rb_define_alloc_func(cNativeClient, rb_tb_client_alloc);
    rb_define_method(cNativeClient, "initialize", rb_tb_client_initialize, 2);
    rb_define_method(cNativeClient, "close", rb_tb_client_close, 0);
    rb_define_method(cNativeClient, "closed?", rb_tb_client_closed_p, 0);
}

static void rb_tb_client_free(void *ptr) {
    rb_tb_client_t *data = ptr;
    if (!atomic_load(&data->closed)) {
        atomic_store(&data->closed, true);
        tb_client_deinit(&data->client);
    }
    ruby_xfree(data);
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
    return TypedData_Wrap_Struct(klass, &rb_tb_client_type, data);
}

static void rb_tb_on_completion(
    uintptr_t ctx,
    tb_packet_t *packet,
    uint64_t timestamp,
    const uint8_t *result,
    uint32_t result_size
) {
    (void)ctx;
    (void)packet;
    (void)timestamp;
    (void)result;
    (void)result_size;
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
    rb_tb_client_t *data;
    TypedData_Get_Struct(self, rb_tb_client_t, &rb_tb_client_type, data);

    uint8_t cluster_id_bytes[16] = {0};
    rb_integer_pack(cluster_id_rb, cluster_id_bytes, 16, 1, 0, INTEGER_PACK_LITTLE_ENDIAN);

    const char *addr = StringValueCStr(addresses_rb);
    uint32_t addr_len = (uint32_t)RSTRING_LEN(addresses_rb);

    TB_INIT_STATUS status =
        tb_client_init(&data->client, cluster_id_bytes, addr, addr_len, 0, rb_tb_on_completion);

    if (status != TB_INIT_SUCCESS) {
        rb_raise(rb_eRuntimeError, "%s", tb_init_error_message(status));
    }
    atomic_store(&data->closed, false);
    return self;
}

static void *rb_tb_deinit_without_gvl(void *arg) {
    tb_client_deinit((tb_client_t *)arg);
    return NULL;
}

static VALUE rb_tb_client_close(VALUE self) {
    rb_tb_client_t *data;
    TypedData_Get_Struct(self, rb_tb_client_t, &rb_tb_client_type, data);
    if (atomic_load(&data->closed))
        return Qnil;
    atomic_store(&data->closed, true);
    // deinit blocks until all in-flight callbacks complete. The GVL is released
    // while waiting so other Ruby threads can run.
    rb_thread_call_without_gvl(rb_tb_deinit_without_gvl, &data->client, NULL, NULL);
    return Qnil;
}

static VALUE rb_tb_client_closed_p(VALUE self) {
    rb_tb_client_t *data;
    TypedData_Get_Struct(self, rb_tb_client_t, &rb_tb_client_type, data);
    return atomic_load(&data->closed) ? Qtrue : Qfalse;
}
