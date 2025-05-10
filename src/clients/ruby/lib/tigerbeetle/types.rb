require 'ffi'

require_relative "struct_converter"

module TigerBeetle
  module Types
    class UINT128 < FFI::Struct
      include StructConverter

      layout(low: :uint64, high: :uint64)

      class << self
        def to_native(value, _ctx)
          case value
          when UINT128
            value.pointer
          when Integer
            obj = self.new
            obj[:low] = value & 0xFFFFFFFFFFFFFFFF
            obj[:high] = (value >> 64) & 0xFFFFFFFFFFFFFFFF
            obj.pointer
          when FFI::Pointer, FFI::MemoryPointer
            instance = UINT128.new
            instance[:low] = value.read_uint64
            instance[:high] = FFI::Pointer.new(value.address + 8).read_uint64
            instance
          when Array
            raise ArgumentError, "Array must have 2 elements" unless value.length == 2

            obj = self.new
            instance[:low] = value[0] & 0xFFFFFFFFFFFFFFFF
            instance[:high] = value[1] & 0xFFFFFFFFFFFFFFFF
            obj
          else
            raise TypeError, "can't convert #{value.class} to UINT128"
          end
        end

        def from_native(value, _ctx)
          if value.is_a?(self)
            value
          elsif value.is_a?(FFI::Struct::InlineArray)
            raise ArgumentError, "Array must have 2 elements" unless value.length == 2

            obj = self.new
            obj[:low] = value[0]
            obj[:high] = value[1]
            obj
          elsif value.is_a?(FFI::Pointer) || value.is_a?(FFI::MemoryPointer)
            obj = self.new
            obj[:low] = value.read_uint64
            obj[:high] = FFI::Pointer.new(value + 8).read_uint64
            obj
          else
            raise TypeError, "can't convert #{value.class} to UINT128"
          end
        end

        def native_type
          @native_type ||= FFI::Type::Array.new(FFI::Type::UINT64, 2)
        end
      end

      def to_i
        self[:high].to_i << 64 | self[:low].to_i
      end

      def to_s
        to_i.to_s
      end

      def inspect
        "#<#{self.class.name} i=#{to_i}>"
      end
    end
  end
end
