# frozen_string_literal: true

require_relative "tigerbeetle/version"
require_relative "tigerbeetle/bindings"

require "tigerbeetle/tigerbeetle"

module TigerBeetle
  class Error < StandardError; end
end
