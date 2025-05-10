# frozen_string_literal: true

require "ffi"

require_relative "tigerbeetle/client"
require_relative "tigerbeetle/bindings"
require_relative "tigerbeetle/request"
require_relative "tigerbeetle/shared_lib"
require_relative "tigerbeetle/struct_converter"
require_relative "tigerbeetle/types"
require_relative "tigerbeetle/version"

module TigerBeetle
  class Error < StandardError; end

  def self.connect(addresses:, cluster_id:, client_id: 1)
    Client.new(addresses:, cluster_id:, client_id:).tap(&:connect)
  end
end
