require_relative "tiger_beetle_integration_test"

class TestAccountFilter < TigerBeetleIntegrationTest
  def test_get_account_balances_empty_result
    results = @client.get_account_balances(
      TigerBeetle::AccountFilter.new(account_id: TigerBeetle.id, limit: 10)
    )

    assert_empty(results)
  end

  def test_account_filter_operations_raise_after_close
    client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: @tb_address)
    client.close
    filter = TigerBeetle::AccountFilter.new(account_id: TigerBeetle.id, limit: 1)

    assert_raises(TigerBeetle::ClientClosedError) { client.get_account_transfers(filter) }
    assert_raises(TigerBeetle::ClientClosedError) { client.get_account_balances(filter) }
  end
end
