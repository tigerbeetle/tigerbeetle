require_relative "tiger_beetle_integration_test"

class TestIntegerTypes < TigerBeetleIntegrationTest
  def test_u128_field_rejects_float
    transfer = TigerBeetle::Transfer.new(id: 1, amount: 1.5, ledger: 1, code: 1)

    assert_type_error("amount must be an Integer, got Float") do
      @client.create_transfers([transfer])
    end
  end

  def test_u16_field_rejects_float
    account = TigerBeetle::Account.new(id: 1, ledger: 1, code: 1.5)

    assert_type_error("code must be an Integer, got Float") do
      @client.create_accounts([account])
    end
  end

  def test_id_rejects_float
    assert_type_error("id must be an Integer, got Float") do
      @client.lookup_accounts([1.5])
    end
  end

  def test_cluster_id_rejects_float
    assert_type_error("cluster_id must be an Integer, got Float") do
      TigerBeetle::Client.new(cluster_id: 1.5, replica_addresses: @tb_address)
    end
  end

  private

  def assert_type_error(message, &block)
    error = assert_raises(TypeError, &block)
    assert_equal(message, error.message)
  end
end
