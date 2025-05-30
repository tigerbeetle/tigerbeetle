const std = @import("std");
const assert = std.debug.assert;

const ruby = @cImport(@cInclude("ruby.h"));

const SKIP_PREFIXES = [_][]const u8{ "reserved", "opaque", "deprecated" };

fn skip_field(comptime field_name: []const u8) bool {
    inline for (SKIP_PREFIXES) |prefix| {
        if (comptime std.mem.startsWith(u8, field_name, prefix)) {
            return true;
        }
    }
    return false;
}

pub fn rb_int_to_u128(value: ruby.VALUE) u128 {
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

pub fn u128_to_rb_int(value: u128) ruby.VALUE {
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

pub fn to_ruby_class(comptime ZigType: type, comptime ruby_name: []const u8) type {
    if (@typeInfo(ZigType) != .Struct) {
        @compileError("Expected a struct type for Ruby C struct conversion, got: " ++ @typeInfo(ZigType));
    }

    return struct {
        const Self = @This();
        const type_info = @typeInfo(ZigType);
        const type_name = @typeName(ZigType) ++ "\x00";
        const rb_class_name = ruby_name ++ "\x00";

        pub const rb_data_type = ruby.rb_data_type_t{
            .wrap_struct_name = &type_name[0],
            .function = .{
                .dmark = null,
                .dfree = free_fn,
                .dsize = size_fn,
            },
            .data = null,
            .flags = ruby.RUBY_TYPED_FREE_IMMEDIATELY,
        };

        pub fn get_rb_data_type_ptr() *const ruby.rb_data_type_t {
            return &rb_data_type;
        }

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

        fn convert_to_ruby_hash(self: ruby.VALUE) callconv(.C) ruby.VALUE {
            const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
            const hash: ruby.VALUE = ruby.rb_hash_new();

            inline for (type_info.Struct.fields) |field| {
                if (comptime skip_field(field.name)) {
                    continue;
                }
                const field_value = @field(wrapper.*, field.name);
                const field_name_cstr = field.name ++ "\x00";
                const key_id = ruby.rb_intern(&field_name_cstr[0]);
                const key_symbol = ruby.ID2SYM(key_id); // Convert ID to symbol

                const value = zigToRuby(@TypeOf(field_value), field_value);

                _ = ruby.rb_hash_aset(hash, key_symbol, value);
            }

            return hash;
        }

        pub fn init_methods(parent: ruby.VALUE) void {
            const method_defs: []const MethodDef = blk: {
                if (@typeInfo(ZigType) != .Struct) {
                    @compileError("Expected a struct type for Ruby class conversion, got: " ++ @typeInfo(ZigType));
                }

                const fields = type_info.Struct.fields;
                var defs: [fields.len * 2]MethodDef = undefined;

                comptime var i = 0;
                inline for (fields) |field| {
                    if (comptime skip_field(field.name)) {
                        continue;
                    }

                    const getter_name = field.name ++ "\x00";
                    const setter_name = field.name ++ "=\x00";

                    defs[i] = MethodDef{
                        .name = &getter_name[0],
                        .func = @ptrCast(&make_getter_fn(field.name)),
                        .argc = 0,
                    };
                    defs[i + 1] = MethodDef{
                        .name = &setter_name[0],
                        .func = @ptrCast(&make_setter_fn(field.name)),
                        .argc = 1,
                    };
                    i += 2;
                }

                break :blk defs[0..i];
            };

            const class = ruby.rb_define_class_under(parent, &rb_class_name[0], ruby.rb_cObject);
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

        fn make_getter_fn(comptime field_name: []const u8) fn (ruby.VALUE) callconv(.C) ruby.VALUE {
            return struct {
                fn get(self: ruby.VALUE) callconv(.C) ruby.VALUE {
                    const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
                    const field_value = @field(wrapper.*, field_name);
                    return zigToRuby(@TypeOf(field_value), field_value);
                }
            }.get;
        }

        fn make_setter_fn(comptime field_name: []const u8) fn (ruby.VALUE, ruby.VALUE) callconv(.C) ruby.VALUE {
            return struct {
                fn set(self: ruby.VALUE, value: ruby.VALUE) callconv(.C) ruby.VALUE {
                    const wrapper: *ZigType = @ptrCast(@alignCast(ruby.rb_check_typeddata(self, &rb_data_type)));
                    const FieldType = @TypeOf(@field(wrapper.*, field_name));
                    @field(wrapper.*, field_name) = rubyToZig(FieldType, value);
                    return value;
                }
            }.set;
        }

        fn uintToRuby(comptime T: type, value: T) ruby.VALUE {
            if (@typeInfo(T) != .Int) {
                @compileError("Expected an integer type for Ruby conversion, got: " ++ @typeInfo(T));
            }

            return switch (T) {
                u8, u16, u32 => ruby.UINT2NUM(value),
                u64 => ruby.ULONG2NUM(value),
                u128 => u128_to_rb_int(value),
                else => @compileError("Unsupported integer size: " ++ @typeName(T)),
            };
        }

        // Condensed type conversion helpers
        fn zigToRuby(comptime T: type, value: T) ruby.VALUE {
            return switch (@typeInfo(T)) {
                .Int => uintToRuby(T, value),
                .Enum => |info| {
                    const tag_type = info.tag_type;
                    return switch (@typeInfo(tag_type)) {
                        .Int => {
                            const int_value = @intFromEnum(value);
                            return uintToRuby(tag_type, int_value);
                        },
                        else => @compileError("Unsupported enum type: " ++ @typeName(tag_type)),
                    };
                },
                .Struct => |info| {
                    if (info.layout == .@"packed") {
                        if (info.backing_integer) |backing_type| {
                            const int_value: backing_type = @bitCast(value);
                            return uintToRuby(backing_type, int_value);
                        } else {
                            @compileError("Packed struct has no backing integer type");
                        }
                    }
                    @compileError("Unsupported struct layout for Ruby conversion: " ++ @typeName(T));
                },
                else => {
                    @compileError("Unsupported type for Ruby conversion: " ++ @typeName(T));
                },
            };
        }

        fn rubyToUint(comptime T: type, value: ruby.VALUE) T {
            if (@typeInfo(T) != .Int) {
                @compileError("Expected an integer type for Ruby conversion, got: " ++ @typeInfo(T));
            }

            return switch (T) {
                u8 => @intCast(ruby.NUM2UINT(value)),
                u16 => @intCast(ruby.NUM2UINT(value)),
                u32 => ruby.NUM2UINT(value),
                u64 => @intCast(ruby.NUM2ULONG(value)),
                u128 => rb_int_to_u128(value),
                else => @compileError("Unsupported integer type: " ++ @typeName(T)),
            };
        }

        fn rubyToZig(comptime T: type, value: ruby.VALUE) T {
            return switch (@typeInfo(T)) {
                .Int => rubyToUint(T, value),
                .Enum => |info| {
                    const tag_type = info.tag_type;
                    return switch (@typeInfo(tag_type)) {
                        .Int => {
                            const int_value = rubyToUint(tag_type, value);
                            return @enumFromInt(int_value);
                        },
                        else => @compileError("Unsupported enum type: " ++ @typeName(tag_type)),
                    };
                },
                .Struct => |info| {
                    if (info.layout == .@"packed") {
                        if (info.backing_integer) |backing_type| {
                            const int_value = rubyToUint(backing_type, value);
                            return @bitCast(int_value);
                        } else {
                            @compileError("Packed struct has no backing integer type");
                        }
                    }
                    @compileLog("Unsupported struct layout for Ruby conversion: " ++ @typeName(T));
                },
                else => {
                    @compileError("Unsupported type for Ruby conversion: " ++ @typeName(T));
                },
            };
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

pub fn to_ruby_enum(comptime ZigType: type, comptime ruby_name: []const u8) type {
    if (@typeInfo(ZigType) != .Enum and @typeInfo(ZigType) != .Struct) {
        @compileError("Expected an enum or struct type for Ruby module conversion, got: " ++ @typeInfo(ZigType));
    }

    return struct {
        const Self = @This();
        const class_name = ruby_name ++ "\x00";

        pub fn init_methods(module: ruby.VALUE) void {
            const ruby_enum = ruby.rb_define_module_under(module, &class_name[0]);

            switch (@typeInfo(ZigType)) {
                .Enum => |enum_info| {
                    inline for (enum_info.fields) |field| {
                        if (comptime skip_field(field.name)) {
                            continue;
                        }
                        const enum_value = @field(ZigType, field.name);
                        const ruby_value = @intFromEnum(enum_value);

                        const ruby_const_name = to_upper_case(field.name);

                        _ = ruby.rb_define_const(ruby_enum, &ruby_const_name, ruby.UINT2NUM(ruby_value));
                    }
                },
                .Struct => |struct_info| {
                    const layout = struct_info.layout;
                    if (layout != .@"packed") {
                        @compileError("Only packed structs can be converted to Ruby enums: " ++ @typeName(ZigType));
                    }

                    inline for (struct_info.fields, 0..) |field, i| {
                        if (comptime skip_field(field.name)) {
                            continue;
                        }
                        const ruby_const_name = to_upper_case(field.name);
                        _ = ruby.rb_define_const(ruby_enum, &ruby_const_name, ruby.UINT2NUM(1 << i));
                    }
                },
                else => @compileError("Invalid conversion to enum: " ++ ZigType),
            }
        }
    };
}

fn to_upper_case(comptime input: []const u8) [input.len + 1:0]u8 {
    var result: [input.len + 1:0]u8 = undefined;
    _ = std.ascii.upperString(result[0..input.len], input);
    result[input.len] = 0; // null terminator
    return result;
}
