const std = @import("std");

pub const VALUE = usize;
pub const ID = usize;

// Ruby constants - hardcoded because they're macros in Ruby
pub const Qnil: VALUE = 8;
pub const Qtrue: VALUE = 20;
pub const Qfalse: VALUE = 0;
pub const Qundef: VALUE = 52;

// Ruby type constants
// https://github.com/ruby/ruby/blob/master/include/ruby/internal/value_type.h
pub const T_STRING: c_int = 0x05;
pub const T_ARRAY: c_int = 0x07;
pub const T_BIGNUM: c_int = 0x0a;
pub const T_DATA: c_int = 0x0c;
pub const T_FIXNUM: c_int = 0x15;

// Extern variables - resolved at runtime
pub extern var rb_cObject: VALUE;
pub extern var rb_eArgError: VALUE;
pub extern var rb_eNoMemError: VALUE;
pub extern var rb_eNoMethodError: VALUE;
pub extern var rb_eRangeError: VALUE;
pub extern var rb_eRuntimeError: VALUE;
pub extern var rb_eTypeError: VALUE;

// Constants
// https://github.com/ruby/ruby/blob/master/include/ruby/internal/core/rtypeddata.h#L111
pub const RUBY_TYPED_FREE_IMMEDIATELY: VALUE = 1 << 0;
// https://github.com/ruby/ruby/blob/master/include/ruby/internal/intern/bignum.h#L567
pub const INTEGER_PACK_LITTLE_ENDIAN: c_int = 0x22;

// Ruby's data type structure
pub const rb_data_type_t = extern struct {
    wrap_struct_name: [*c]const u8,
    function: extern struct {
        dmark: ?*const fn (?*anyopaque) callconv(.C) void,
        dfree: ?*const fn (?*anyopaque) callconv(.C) void,
        dsize: ?*const fn (?*const anyopaque) callconv(.C) usize,
        reserved: [2]?*anyopaque,
    },
    parent: ?*const rb_data_type_t,
    data: ?*anyopaque,
    flags: VALUE,
};

// Ruby API functions
pub extern fn rb_ary_entry(ary: VALUE, offset: c_long) VALUE;
pub extern fn rb_ary_new() VALUE;
pub extern fn rb_ary_push(ary: VALUE, item: VALUE) VALUE;
pub extern fn rb_check_typeddata(obj: VALUE, data_type: *const rb_data_type_t) ?*anyopaque;
pub extern fn rb_class_new_instance(argc: c_int, argv: [*c]const VALUE, klass: VALUE) VALUE;
pub extern fn rb_const_get(klass: VALUE, id: ID) VALUE;
pub extern fn rb_data_typed_object_zalloc(klass: VALUE, size: usize, typ: *const rb_data_type_t) VALUE;
pub extern fn rb_define_alloc_func(klass: VALUE, func: *const fn (VALUE) callconv(.C) VALUE) void;
pub extern fn rb_define_class_under(outer: VALUE, name: [*c]const u8, super: VALUE) VALUE;
pub extern fn rb_define_const(klass: VALUE, name: [*c]const u8, val: VALUE) void;
pub extern fn rb_define_method(klass: VALUE, name: [*c]const u8, func: *const anyopaque, argc: c_int) void;
pub extern fn rb_define_module(name: [*c]const u8) VALUE;
pub extern fn rb_define_module_under(outer: VALUE, name: [*c]const u8) VALUE;
pub extern fn rb_funcall(recv: VALUE, mid: ID, argc: c_int, ...) VALUE;
pub extern fn rb_hash_aset(hash: VALUE, key: VALUE, val: VALUE) VALUE;
pub extern fn rb_hash_foreach(hash: VALUE, func: *const fn (VALUE, VALUE, VALUE) callconv(.C) c_int, farg: VALUE) void;
pub extern fn rb_hash_new() VALUE;
pub extern fn rb_integer_pack(val: VALUE, words: *anyopaque, numwords: usize, wordsize: usize, nails: usize, flags: c_int) c_int;
pub extern fn rb_integer_unpack(words: *const anyopaque, numwords: usize, wordsize: usize, nails: usize, flags: c_int) VALUE;
pub extern fn rb_intern(name: [*c]const u8) ID;
pub extern fn rb_raise(exc: VALUE, fmt: [*c]const u8, ...) noreturn;
pub extern fn rb_respond_to(obj: VALUE, id: ID) c_int;
pub extern fn rb_scan_args(argc: c_int, argv: [*c]const VALUE, fmt: [*c]const u8, ...) c_int;
pub extern fn rb_sym2str(symbol: VALUE) VALUE;
pub extern fn ruby_xfree(ptr: ?*anyopaque) void;

// Wrapper functions defined in src/clients/ruby/ext/tb_client/tb_client.c
pub extern fn wrapped_id2sym(id: ID) VALUE;
pub extern fn wrapped_int2num(v: c_int) VALUE;
pub extern fn wrapped_nil_p(v: VALUE) bool;
pub extern fn wrapped_num2uint(v: VALUE) c_uint;
pub extern fn wrapped_num2ulong(v: VALUE) c_ulong;
pub extern fn wrapped_num2ull(v: VALUE) c_ulonglong;
pub extern fn wrapped_rarray_len(v: VALUE) c_long;
pub extern fn wrapped_rstring_len(v: VALUE) c_long;
pub extern fn wrapped_rstring_ptr(v: VALUE) [*c]const u8;
pub extern fn wrapped_symbol_p(v: VALUE) bool;
pub extern fn wrapped_uint2num(i: c_uint) VALUE;
pub extern fn wrapped_ull2num(i: c_ulonglong) VALUE;
pub extern fn wrapped_rb_type_p(v: VALUE, t: c_int) bool;
