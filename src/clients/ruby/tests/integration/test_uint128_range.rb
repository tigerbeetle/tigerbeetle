require_relative "tiger_beetle_integration_test"

class TestUInt128Range < TigerBeetleIntegrationTest
  UINT128_OVERFLOW = 1 << 128
  UINT128_RANGE_ERROR = "integer must be between 0 and 2**128 - 1"

  def test_range_check_u128_cluster_id_cannot_exceed
    assert_u128_range_error do
      TigerBeetle::Client.new(cluster_id: UINT128_OVERFLOW, replica_addresses: @tb_address)
    end
  end

  private

  def assert_u128_range_error(&block)
    error = assert_raises(RangeError, &block)
    assert_equal(UINT128_RANGE_ERROR, error.message)
  end
end
