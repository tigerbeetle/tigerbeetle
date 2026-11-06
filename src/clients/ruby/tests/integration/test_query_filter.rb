require_relative "tiger_beetle_integration_test"

class TestQueryFilter < TigerBeetleIntegrationTest
  BATCH_MAX = 8_189

  def test_invalid_query_filters
    filter = TigerBeetle::QueryFilter.new(limit: BATCH_MAX, flags: 0xFFFF)
    assert_empty(@client.query_accounts(filter))
    assert_empty(@client.query_transfers(filter))
  end
end
