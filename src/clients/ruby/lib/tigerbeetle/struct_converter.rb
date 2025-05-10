require "ffi"

module TigerBeetle
  module StructConverter
    def self.included(base)
      base.extend FFI::DataConverter
      base.extend ClassMethods
    end

    module ClassMethods
      def from_native(value, context)
        if value.is_a?(self)
          value
        else
          self.new(ptr)
        end
      end

      def to_native(value, context)
        case value
        when self
          value.pointer
        when Hash
          struct = self.new
          raise ArgumentError if (struct.members - value.keys).any?

          value.each do |key, val|
            struct[key] = val
          end

          struct.pointer
        else
          raise TypeError, "can't convert #{value.class} to #{self.name}"
        end
      end

      def native_type
        FFI::Type::POINTER
      end
    end

    def members
      self.class.layout.members
    end

    def to_s
      values = members.map { |m| "#{m}: #{self[m]}" }
      "#{self.class.name}(#{values.join(', ')})"
    end
  end
end
