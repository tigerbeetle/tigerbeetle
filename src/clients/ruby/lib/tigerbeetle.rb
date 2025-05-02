# frozen_string_literal: true

require_relative "tigerbeetle/version"
require_relative "tigerbeetle/bindings"
require_relative "tigerbeetle/client"

require "tigerbeetle/tigerbeetle"

module TigerBeetle
  class Error < StandardError; end

  def TigerBeetle.connect(addresses: "3000", cluster_id: 0)
    Client.new(addresses:, cluster_id:).tap(&:connect)
  end
end
