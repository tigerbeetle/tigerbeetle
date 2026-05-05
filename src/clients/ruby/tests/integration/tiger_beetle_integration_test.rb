require "minitest/autorun"
require "tigerbeetle"

class TigerBeetleIntegrationTest < Minitest::Test
  def setup
    @tb_address = ENV.fetch("TB_ADDRESS", "3000")
    @client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
  end

  def teardown
    @client.close if @client && !@client.closed?
  end
end
