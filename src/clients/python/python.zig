const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;

pub const vsr = @import("vsr");
const exports = vsr.tb_client.exports;
const tb = vsr.tigerbeetle;
const constants = vsr.constants;

const Tracer = vsr.trace.TracerType(vsr.time.Time);
const Storage = vsr.storage.StorageType(vsr.io.IO, Tracer);
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Operation = StateMachine.Operation;

// libtb_client.zig exports the client symbols.
const libtb_client = @import("libtb_client");
pub const std_options = libtb_client.std_options;

const py = @cImport({
    @cDefine("PY_SSIZE_T_CLEAN", {});
    @cDefine("Py_LIMITED_API", "0x03070000"); // Require Python 3.7 and up.
    @cInclude("Python.h");
});

const PyObject = py.PyObject;

var time_ms_last: u128 = 0;
fn id(self: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*]PyObject {
    const gil_state = py.PyGILState_Ensure();
    defer py.PyGILState_Release(gil_state);

    _ = self;
    _ = args;
    const time_ms = blk: {
        const time_ms_candiate: u128 = @intCast(std.time.milliTimestamp());

        if (time_ms_candiate > time_ms_last) {
            time_ms_last = time_ms_candiate;
            break :blk time_ms_candiate;
        } else {
            break :blk time_ms_last;
        }
    };

    const random: u128 = std.crypto.random.int(u80) << 48;
    const output: u128 = random | time_ms;

    std.log.info("output: {}, time_ms: {} random: {}", .{ output, time_ms, random });
    return translate.u128_to_object(output);
}

fn encode(self: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*c]PyObject {
    const gil_state = py.PyGILState_Ensure();
    defer py.PyGILState_Release(gil_state);

    _ = self;

    var operation_int: u32 = 0;
    var operation_list: [*c]PyObject = null;

    if (py.PyArg_ParseTuple(args, "IO", &operation_int, &operation_list) != 1) {
        py.PyErr_SetString(py.PyExc_ValueError, "Invalid arguments to encode()");
        return null;
    }

    if (!@as(vsr.Operation, @enumFromInt(operation_int)).valid(StateMachine)) {
        py.PyErr_SetString(py.PyExc_ValueError, "Unknown operation.");
        return null;
    }

    assert(operation_list != null);
    assert(operation_int != 0);

    const operation: Operation = @enumFromInt(@as(u8, @intCast(operation_int)));

    // PySequence_Fast_* isn't part of the stable API, despite what the docs say!
    const sequence_length: u32 = @intCast(py.PySequence_Size(operation_list));

    switch (operation) {
        inline else => |operation_comptime| {
            const Event = StateMachine.EventType(operation_comptime);

            // Avoid allocating memory for requests that are known to be too large.
            // However, the final validation happens in `tb_client` against the runtime-known
            // maximum size.
            if (sequence_length * @sizeOf(Event) > vsr.constants.message_body_size_max) {
                py.PyErr_SetString(py.PyExc_ValueError, "Too many items for a single batch");
                return null;
            }

            // Why is this needed on arm?
            if (sequence_length == 0) {
                const buffer = py.PyBytes_FromStringAndSize(null, 0);
                return buffer;
            }

            // FIX2ME: Alignment?
            const buffer = py.PyBytes_FromStringAndSize(null, @sizeOf(Event) * sequence_length);
            const buffer_raw = py.PyBytes_AsString(buffer);
            const buffer_typed = @as(
                [*]Event,
                @alignCast(@ptrCast(buffer_raw)),
            )[0..sequence_length];

            python_sequence_to_events(Event, operation_list, buffer_typed) catch {
                return null;
            };

            return buffer;
        },
        .pulse, .get_events => unreachable,
    }
}

fn python_sequence_to_events(comptime Event: type, list: [*c]PyObject, events: []Event) !void {
    // Generate and cache the Python property names, rather than doing the work each time:
    const fields_event = if (Event == u128) .{} else std.meta.fields(Event);
    var fields_python: [fields_event.len][*c]PyObject = undefined;

    inline for (fields_event, &fields_python) |field_event, *field_python| {
        field_python.* = py.PyUnicode_FromString(field_event.name);
    }
    defer for (fields_python) |field_python| py.Py_DecRef(field_python);

    for (events, 0..) |*event, i| {
        const object = py.PySequence_GetItem(list, @intCast(i)) orelse return error.RaiseException;
        defer py.Py_DecRef(object);

        switch (Event) {
            tb.Account,
            tb.Transfer,
            tb.AccountFilter,
            tb.AccountBalance,
            tb.QueryFilter,
            => {
                inline for (fields_event, fields_python) |field, field_python| {
                    var field_value = py.PyObject_GetAttr(object, field_python);

                    // Allow reserved to be missing.
                    if (field_value == null and std.mem.eql(u8, field.name, "reserved")) {
                        py.PyErr_Clear();
                        field_value = py.PyLong_FromLong(0);
                    } else if (field_value == null) {
                        // PyObject_GetAttr sets exception.
                        return error.RaiseException;
                    }
                    defer py.Py_DecRef(field_value);

                    const value: field.type = switch (@typeInfo(field.type)) {
                        .Struct => |info| @bitCast(try @field(
                            translate,
                            @typeName(info.backing_integer.?) ++ "_from_object",
                        )(
                            field_value,
                        )),
                        .Int => try @field(translate, @typeName(field.type) ++ "_from_object")(
                            field_value,
                        ),
                        // Arrays are only used for padding/reserved fields,
                        // instead of requiring the user to explicitly set an empty buffer,
                        // we just hide those fields and preserve their default value.
                        .Array => @as(
                            *const field.type,
                            @ptrCast(@alignCast(field.default_value.?)),
                        ).*,
                        else => unreachable,
                    };

                    @field(event, field.name) = value;
                }
            },
            u128 => event.* = try translate.u128_from_object(object),
            else => @compileError("invalid Event type"),
        }
    }
}

fn decode(self: [*c]PyObject, args: [*c]PyObject) callconv(.C) [*c]PyObject {
    const gil_state = py.PyGILState_Ensure();
    defer py.PyGILState_Release(gil_state);

    _ = self;

    var operation_int: u32 = 0;
    var result_class: [*c]PyObject = null;

    var results_buffer: [*c]const u8 = null;
    var results_buffer_len: u64 = 0;

    if (py.PyArg_ParseTuple(
        args,
        "IOy#",
        &operation_int,
        &result_class,
        &results_buffer,
        &results_buffer_len,
    ) != 1) {
        py.PyErr_SetString(py.PyExc_ValueError, "Invalid arguments to encode()");
        return null;
    }

    if (!@as(vsr.Operation, @enumFromInt(operation_int)).valid(StateMachine)) {
        py.PyErr_SetString(py.PyExc_ValueError, "Unknown operation.");
        return null;
    }

    if (py.PyCallable_Check(result_class) == 0) {
        py.PyErr_SetString(py.PyExc_TypeError, "Expected a callable/class object");
        return null;
    }

    assert(result_class != null);
    assert(operation_int != 0);
    assert(results_buffer != null);
    vsr.stdx.maybe(results_buffer_len != 0);

    const operation: Operation = @enumFromInt(@as(u8, @intCast(operation_int)));

    switch (operation) {
        inline else => |operation_comptime| {
            const Result = StateMachine.ResultType(operation_comptime);
            assert(results_buffer_len % @sizeOf(Result) == 0);

            const results = std.mem.bytesAsSlice(Result, results_buffer[0..results_buffer_len]);
            const list = py.PyList_New(@intCast(results.len));

            const result_without_reserved_fields = comptime blk: {
                var result: []const std.builtin.Type.StructField = &.{};
                for (std.meta.fields(Result)) |field| {
                    if (!std.mem.eql(u8, field.name, "reserved")) {
                        result = result ++ &[_]std.builtin.Type.StructField{field};
                    }
                }

                break :blk result;
            };

            for (results, 0..) |result, i| {
                const result_python_args = py.PyTuple_New(
                    result_without_reserved_fields.len,
                ) orelse return null;

                inline for (result_without_reserved_fields, 0..) |field, j| {
                    if (comptime std.mem.eql(u8, field.name, "reserved")) {
                        continue;
                    }

                    const FieldInt = switch (@typeInfo(field.type)) {
                        .Struct => |info| info.backing_integer.?,
                        .Enum => |info| info.tag_type,
                        // Arrays are only used for padding/reserved fields
                        .Array => unreachable,
                        else => field.type,
                    };

                    const value: FieldInt = switch (@typeInfo(field.type)) {
                        .Struct => @bitCast(@field(result, field.name)),
                        .Enum => @intFromEnum(@field(result, field.name)),
                        else => @field(result, field.name),
                    };

                    const python_value = @field(
                        translate,
                        @typeName(FieldInt) ++ "_to_object",
                    )(value);
                    if (py.PyTuple_SetItem(result_python_args, @intCast(j), python_value) != 0) {
                        // PyTuple_SetItem sets exception.
                        return null;
                    }
                }

                const result_python = py.PyObject_CallObject(
                    result_class,
                    result_python_args,
                ) orelse return null;

                if (py.PyList_SetItem(list, @intCast(i), result_python) != 0) {
                    // PyList_SetItem sets exception.
                    return null;
                }
            }
            return list;
        },
        .pulse, .get_events => unreachable,
    }
}

var Methods = [_]py.PyMethodDef{
    py.PyMethodDef{
        .ml_name = "id",
        .ml_meth = id,
        .ml_flags = py.METH_NOARGS,
        .ml_doc = null,
    },
    py.PyMethodDef{
        .ml_name = "decode",
        .ml_meth = decode,
        .ml_flags = py.METH_VARARGS,
        .ml_doc = null,
    },
    py.PyMethodDef{
        .ml_name = "encode",
        .ml_meth = encode,
        .ml_flags = py.METH_VARARGS,
        .ml_doc = null,
    },
    py.PyMethodDef{
        .ml_name = null,
        .ml_meth = null,
        .ml_flags = 0,
        .ml_doc = null,
    },
};

var module = py.PyModuleDef{
    .m_base = py.PyModuleDef_Base{
        .ob_base = PyObject{
            .ob_type = null,
        },
        .m_init = null,
        .m_index = 0,
        .m_copy = null,
    },
    .m_name = "libtb_pythonclient",
    .m_doc = null,
    .m_size = -1,
    .m_methods = &Methods,
    .m_slots = null,
    .m_traverse = null,
    .m_clear = null,
    .m_free = null,
};

pub export fn PyInit_libtb_pythonclient() [*]PyObject {
    return py.PyModule_Create(&module);
}

const translate = struct {
    pub fn u128_from_object(value_python: [*c]PyObject) !u128 {
        const python_64 = py.PyLong_FromLong(64);
        assert(python_64 != 0);
        defer py.Py_DecRef(python_64);

        const low: u64 = py.PyLong_AsUnsignedLongLongMask(value_python);
        if (py.PyErr_Occurred() != 0) {
            return error.RaiseException;
        }

        const high_obj = py.PyNumber_Rshift(value_python, python_64);
        defer py.Py_DecRef(high_obj);

        const high: u64 = py.PyLong_AsUnsignedLongLongMask(high_obj);
        if (py.PyErr_Occurred() != 0) {
            return error.RaiseException;
        }

        return (@as(u128, @intCast(high)) << 64) | low;
    }

    pub fn u64_from_object(value_python: [*c]PyObject) !u64 {
        const low: u64 = py.PyLong_AsUnsignedLongLong(value_python);
        if (py.PyErr_Occurred() != 0) {
            return error.RaiseException;
        }

        return low;
    }

    pub fn u32_from_object(value_python: [*c]PyObject) !u32 {
        const low: u64 = py.PyLong_AsUnsignedLong(value_python);
        if (py.PyErr_Occurred() != 0) {
            return error.RaiseException;
        }

        if (low > std.math.maxInt(u32)) {
            py.PyErr_SetString(py.PyExc_ValueError, "Integer overflow");
            return error.RaiseException;
        }

        return @intCast(low);
    }

    pub fn u16_from_object(value_python: [*c]PyObject) !u16 {
        const low: u64 = py.PyLong_AsUnsignedLong(value_python);
        if (py.PyErr_Occurred() != 0) {
            return error.RaiseException;
        }

        if (low > std.math.maxInt(u16)) {
            py.PyErr_SetString(py.PyExc_ValueError, "Integer overflow");
            return error.RaiseException;
        }

        return @intCast(low);
    }

    pub fn u128_to_object(value: u128) [*c]PyObject {
        const low: u64 = @truncate(value);
        const high: u64 = @truncate(value >> 64);

        // Shortcut if the high bits are all zero.
        if (high == 0) {
            return py.PyLong_FromUnsignedLongLong(low);
        }

        const python_low = py.PyLong_FromUnsignedLongLong(low);
        assert(python_low != null);
        defer py.Py_DecRef(python_low);

        const python_high = py.PyLong_FromUnsignedLongLong(high);
        assert(python_high != null);
        defer py.Py_DecRef(python_high);

        const python_64 = py.PyLong_FromLong(64);
        assert(python_64 != null);
        defer py.Py_DecRef(python_64);

        const python_high_shifted = py.PyNumber_Lshift(python_high, python_64);
        assert(python_high_shifted != null);
        defer py.Py_DecRef(python_high_shifted);

        const python_result = py.PyNumber_Add(python_high_shifted, python_low);
        assert(python_result != null);

        assert(py.PyErr_Occurred() == 0);
        return python_result;
    }

    pub fn u64_to_object(value: u64) [*c]PyObject {
        const python_result = py.PyLong_FromUnsignedLongLong(value);
        assert(python_result != null);
        assert(py.PyErr_Occurred() == 0);

        return python_result;
    }

    pub fn u32_to_object(value: u32) [*c]PyObject {
        const python_result = py.PyLong_FromUnsignedLongLong(value);
        assert(python_result != null);
        assert(py.PyErr_Occurred() == 0);

        return python_result;
    }

    pub fn u16_to_object(value: u16) [*c]PyObject {
        const python_result = py.PyLong_FromUnsignedLongLong(value);
        assert(python_result != null);
        assert(py.PyErr_Occurred() == 0);

        return python_result;
    }
};
