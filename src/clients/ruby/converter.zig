const std = @import("std");

const ruby = @cImport(@cInclude("ruby.h"));

pub fn to_ruby_class(comptime ZigType: type) type {
    if (@typeInfo(ZigType) != .Struct) {
        @compileError("Expected a struct type for Ruby C struct conversion, got: " ++ @typeInfo(ZigType));
    }

    return struct {
        const Self = @This();
        const type_info = @typeInfo(ZigType);
        const type_name = @typeName(ZigType) ++ "\x00";

        fn alloc_fn(self: ruby.VALUE) callconv(.C) ruby.VALUE {
            return ruby.rb_data_typed_object_zalloc(self, @sizeOf(ZigType), &rb_data_type);
        }

        fn free_fn(ptr: ?*anyopaque) callconv(.C) void {
            if (ptr) |p| {
                ruby.xfree(p);
            }
        }

        fn size_fn(ptr: ?*const anyopaque) callconv(.C) usize {
            _ = ptr;
            return @sizeOf(ZigType);
        }

        const rb_data_type = ruby.rb_data_type_t{
            .wrap_struct_name = &type_name[0],
            .function = .{
                .dmark = null,
                .dfree = free_fn,
                .dsize = size_fn,
            },
            .data = null,
            .flags = ruby.RUBY_TYPED_FREE_IMMEDIATELY,
        };

        fn convert_to_ruby_hash(self: ruby.VALUE) callconv(.C) ruby.VALUE {
            const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
            const hash: ruby.VALUE = ruby.rb_hash_new();

            inline for (type_info.Struct.fields) |field| {
                const field_value = @field(wrapper.*, field.name);
                const key_str = ruby.rb_str_new_cstr(field.name ++ "\x00");
                const key = ruby.rb_intern(ruby.RSTRING_PTR(key_str));

                const value = zigToRuby(@TypeOf(field_value), field_value);

                _ = ruby.rb_hash_aset(hash, key, value);
            }

            return hash;
        }

        pub fn init_methods(class: ruby.VALUE) void {
            const method_defs: []const MethodDef = blk: {
                if (@typeInfo(ZigType) != .Struct) {
                    @compileError("Expected a struct type for Ruby class conversion, got: " ++ @typeInfo(ZigType));
                }

                const fields = type_info.Struct.fields;
                var defs: [fields.len * 2]MethodDef = undefined;

                inline for (fields, 0..) |field, i| {
                    const getter_name = field.name ++ "\x00";
                    const setter_name = field.name ++ "=\x00";

                    defs[i * 2] = MethodDef{
                        .name = &getter_name[0],
                        .func = @ptrCast(&makeGetter(field.name)),
                        .argc = 0,
                    };
                    defs[i * 2 + 1] = MethodDef{
                        .name = &setter_name[0],
                        .func = @ptrCast(&makeSetter(field.name)),
                        .argc = 1,
                    };
                }

                break :blk defs[0..];
            };

            ruby.rb_define_alloc_func(class, alloc_fn);
            ruby.rb_define_method(class, "initialize", @ptrCast(&initialize), -1);
            ruby.rb_define_method(class, "to_h", @ptrCast(&convert_to_ruby_hash), 0);

            // Runtime calls to rb_define_method
            for (method_defs) |method_def| {
                ruby.rb_define_method(class, method_def.name, @ptrCast(method_def.func), method_def.argc);
            }
        }

        const MethodDef = struct {
            name: [*c]const u8,
            func: ?*const anyopaque,
            argc: c_int,
        };

        fn makeGetter(comptime field_name: []const u8) fn (ruby.VALUE) callconv(.C) ruby.VALUE {
            return struct {
                fn get(self: ruby.VALUE) callconv(.C) ruby.VALUE {
                    const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
                    const field_value = @field(wrapper.*, field_name);
                    return zigToRuby(@TypeOf(field_value), field_value);
                }
            }.get;
        }

        // Generate setter for a specific field
        fn makeSetter(comptime field_name: []const u8) fn (ruby.VALUE, ruby.VALUE) callconv(.C) ruby.VALUE {
            return struct {
                fn set(self: ruby.VALUE, value: ruby.VALUE) callconv(.C) ruby.VALUE {
                    const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
                    const FieldType = @TypeOf(@field(wrapper.*, field_name));
                    @field(wrapper.*, field_name) = rubyToZig(FieldType, value);
                    return value;
                }
            }.set;
        }

        // Condensed type conversion helpers
        fn zigToRuby(comptime T: type, value: T) ruby.VALUE {
            return switch (@typeInfo(T)) {
                .Int => {
                    return switch (T) {
                        u8, u16, u32 => ruby.UINT2NUM(value),
                        u64 => ruby.ULONG2NUM(value),
                        u128 => convert_u128_to_ruby(value),
                        else => @compileError("Unsupported integer size: " ++ @typeName(T)),
                    };
                },
                else => {
                    // TODO dig dig deeper for more complex types
                    return ruby.Qnil;
                },
            };
        }

        fn rubyToZig(comptime T: type, value: ruby.VALUE) T {
            return switch (@typeInfo(T)) {
                .Int => {
                    return switch (T) {
                        u8 => {
                            const val = ruby.NUM2UINT(value);
                            if (val > std.math.maxInt(u8)) {
                                ruby.rb_raise(ruby.rb_eRangeError, "integer %lu too big for u8 (max: %u)", val, @as(c_uint, std.math.maxInt(u8)));
                                return 0;
                            }
                            return @as(u8, @intCast(val));
                        },
                        u16 => {
                            const val = ruby.NUM2UINT(value);
                            if (val > std.math.maxInt(u16)) {
                                ruby.rb_raise(ruby.rb_eRangeError, "integer %lu too big for u16 (max: %u)", val, @as(c_uint, std.math.maxInt(u16)));
                                return 0;
                            }
                            return @as(u16, @intCast(val));
                        },
                        u32 => {
                            const val = ruby.NUM2UINT(value);
                            if (val > std.math.maxInt(u32)) {
                                ruby.rb_raise(ruby.rb_eRangeError, "integer %lu too big for u32 (max: %lu)", val, @as(c_ulong, std.math.maxInt(u32)));
                                return 0;
                            }
                            return @as(u32, @intCast(val));
                        },
                        u64 => {
                            // NUM2ULONG is used for 64-bit integers in Ruby
                            return @as(u64, @intCast(ruby.NUM2ULONG(value)));
                        },
                        u128 => convert_ruby_int_to_u128(value),
                        else => @compileError("Unsupported integer type: " ++ @typeName(T)),
                    };
                },
                else => {
                    // TODO dig deeper
                    return undefined;
                },
            };
        }

        fn convert_ruby_int_to_u128(value: ruby.VALUE) u128 {
            if (!ruby.RB_INTEGER_TYPE_P(value)) {
                ruby.rb_raise(ruby.rb_eTypeError, "expected Integer");
                return 0;
            }

            if (ruby.RTEST(ruby.rb_funcall(value, ruby.rb_intern("negative?"), 0))) {
                ruby.rb_raise(ruby.rb_eRangeError, "negative numbers not supported");
                return 0;
            }

            var result: u128 = 0;
            const overflow = ruby.rb_integer_pack(value, // val: the Ruby VALUE
                &result, // words: pointer to output buffer
                1, // numwords: we want 1 chunk of 16 bytes
                @sizeOf(u128), // wordsize: 16 bytes
                0, // nails: 0 (no nail bits)
                ruby.INTEGER_PACK_NATIVE_BYTE_ORDER // flags
            );

            if (overflow == 2) {
                ruby.rb_raise(ruby.rb_eRangeError, "integer too big for u128");
                return 0;
            }

            return result;
        }

        fn convert_u128_to_ruby(value: u128) ruby.VALUE {
            if (value == 0) {
                return ruby.UINT2NUM(0);
            }

            return ruby.rb_integer_unpack(&value, // ruby value
                1, // numwords: we want 1 chunk of 16 bytes
                @sizeOf(u128), // wordsize: 16 bytes
                0, // nails: 0 (no nail bits)
                ruby.INTEGER_PACK_NATIVE_BYTE_ORDER // flags
            );
        }

        fn initialize(argc: c_int, argv: [*]ruby.VALUE, self: ruby.VALUE) callconv(.C) ruby.VALUE {
            _ = ruby.rb_check_typeddata(self, &rb_data_type);

            var kwargs: ruby.VALUE = ruby.Qnil;
            _ = ruby.rb_scan_args(argc, argv, ":", &kwargs);
            if (ruby.NIL_P(kwargs)) {
                return self;
            }

            _ = ruby.rb_hash_foreach(kwargs, initialize_keyword_parameter_value, self);

            return self;
        }

        fn initialize_keyword_parameter_value(key: ruby.VALUE, value: ruby.VALUE, self: ruby.VALUE) callconv(.C) c_int {
            if (!ruby.SYMBOL_P(key)) {
                const key_class = ruby.rb_funcall(key, ruby.rb_intern("class"), 0);
                const class_str = ruby.rb_funcall(key_class, ruby.rb_intern("to_s"), 0);
                ruby.rb_raise(ruby.rb_eArgError, "keyword arguments must use symbol keys, got %s", ruby.RSTRING_PTR(class_str));
                return 1;
            }

            const key_str = ruby.rb_sym2str(key);
            const setter_str = ruby.rb_str_plus(key_str, ruby.rb_str_new2("="));
            const setter_cstr = ruby.RSTRING_PTR(setter_str);
            const setter_id = ruby.rb_intern(setter_cstr);

            if (ruby.rb_respond_to(self, setter_id) == 0) {
                ruby.rb_raise(ruby.rb_eNoMethodError, "undefined method '%s' for object", setter_cstr);
                return 1;
            }

            _ = ruby.rb_funcall(self, setter_id, 1, value);
            return 0;
        }
    };
}
