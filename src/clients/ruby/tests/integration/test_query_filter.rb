require_relative "tiger_beetle_integration_test"

class TestQueryFilter < TigerBeetleIntegrationTest
  BATCH_MAX = 8_189

  def test_invalid_query_filters
    filter = TigerBeetle::QueryFilter.new(limit: BATCH_MAX, flags: 0xFFFF)
    assert_empty(@client.query_accounts(filter))
    assert_empty(@client.query_transfers(filter))

    too_much_data = TigerBeetle::QueryFilter.new(limit: 10_000)
    assert_raises(TigerBeetle::BatchError) { @client.query_accounts(too_much_data) }
    assert_raises(TigerBeetle::BatchError) { @client.query_transfers(too_much_data) }
  end

  def test_query_operations_raise_after_close
    client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
    client.close
    filter = TigerBeetle::QueryFilter.new(limit: 1)

    assert_raises(TigerBeetle::ClientClosedError) { client.query_accounts(filter) }
    assert_raises(TigerBeetle::ClientClosedError) { client.query_transfers(filter) }
  end
end
