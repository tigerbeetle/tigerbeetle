require_relative "tiger_beetle_integration_test"

class TestUInt128Range < TigerBeetleIntegrationTest
  UINT128_MAX = (1 << 128) - 1
  UINT128_OVERFLOW = 1 << 128
  UINT128_TOO_BIG = 9_999_999_999_999_999_999_999_999_999_999_999_999_999
  UINT128_RANGE_ERROR = "integer must be between 0 and 2**128 - 1"

  def test_range_check_u128_accepts_max
    assert_equal([], @client.lookup_accounts([UINT128_MAX]))
  end

  def test_range_check_u128_cannot_exceed
    assert_u128_range_error do
      @client.lookup_accounts([UINT128_OVERFLOW])
    end
  end

  def test_range_check_u128_cannot_be_too_big
    assert_u128_range_error do
      @client.lookup_accounts([UINT128_TOO_BIG])
    end
  end

  def test_range_check_u128_cannot_be_negative
    assert_u128_range_error do
      @client.lookup_accounts([-1])
    end
  end

  def test_range_check_u128_struct_field_cannot_exceed
    account = TigerBeetle::Account.new(id: UINT128_OVERFLOW, ledger: 1, code: 1)

    assert_u128_range_error do
      @client.create_accounts([account])
    end
  end

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
