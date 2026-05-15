require_relative "tigerbeetle/version"
require_relative "tigerbeetle/bindings"
require_relative "tigerbeetle/id"
require_relative "tigerbeetle/completion_dispatcher"
require_relative "tigerbeetle/client"
require "tigerbeetle/tigerbeetle"

module TigerBeetle
  private_constant :NativeClient
  private_constant :Request

  @id_generator = ID.new

  # Generates a 128-bit, time-based, monotonically increasing ID.
  def self.id
    @id_generator.generate
  end
end
