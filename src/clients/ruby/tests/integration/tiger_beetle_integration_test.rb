require "minitest/autorun"
require "tigerbeetle"

class TigerBeetleIntegrationTest < Minitest::Test
  def self.client
    @client ||= begin
      client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: ENV.fetch("TB_ADDRESS", "3000"))
      Minitest.after_run { client.close }
      client
    end
  end

  def setup
    @tb_address = ENV.fetch("TB_ADDRESS", "3000")
    @client = TigerBeetleIntegrationTest.client
  end
end
