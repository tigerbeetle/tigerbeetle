#include <ruby.h>
#include "rb_tigerbeetle.h"

VALUE wrapped_uint2num(unsigned int i) {
    return UINT2NUM(i);
}

bool wrapped_nil_p(VALUE v) {
    return NIL_P(v);
}

VALUE wrapped_int2num(int v) {
  return INT2NUM(v);
}

VALUE wrapped_ull2num(unsigned long long i) {
    return ULL2NUM(i);
}

VALUE wrapped_id2sym(ID id) {
    return ID2SYM(id);
}

bool wrapped_symbol_p(VALUE val) {
  return SYMBOL_P(val);
}

unsigned int wrapped_num2uint(VALUE v) {
  return NUM2UINT(v);
}

bool wrapped_rb_type_p(VALUE obj, int type) {
    return RB_TYPE_P(obj, type);
}

unsigned long wrapped_num2ulong(VALUE v) {
  return NUM2ULONG(v);
}

unsigned long long wrapped_num2ull(VALUE v) {
  return NUM2ULL(v);
}

long wrapped_rarray_len(VALUE arr) {
  return RARRAY_LEN(arr);
}

long wrapped_rstring_len(VALUE str) {
    return RSTRING_LEN(str);
}

bool wrapped_rtest(VALUE v) {
  return RTEST(v);
}

int wrapped_type(VALUE v) {
  return TYPE(v);
}

const char* wrapped_rstring_ptr(VALUE str) {
    return RSTRING_PTR(str);
}

void Init_tb_client(void) {
  initialize_ruby_client();
}
