module TigerBeetle
  Request = Struct.new(:packet, :result_type, :block) do
    def initialize(packet, result_type, &block)
      super(packet, result_type, block)
    end
  end
end
