require "minitest/autorun"
require "tigerbeetle"

class TestFlags < Minitest::Test
  def test_account_flags_accept_integer
    account = TigerBeetle::Account.new(
      id: 1,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::AccountFlags::LINKED | TigerBeetle::AccountFlags::HISTORY
    )

    assert_equal(
      TigerBeetle::AccountFlags::LINKED | TigerBeetle::AccountFlags::HISTORY,
      account.flags
    )
  end

  def test_account_flags_accept_lowercase_symbols
    account = TigerBeetle::Account.new(id: 1, ledger: 1, code: 1, flags: [:linked, :history])

    assert_equal(
      TigerBeetle::AccountFlags::LINKED | TigerBeetle::AccountFlags::HISTORY,
      account.flags
    )
  end

  def test_transfer_flags_accept_uppercase_symbols
    transfer = TigerBeetle::Transfer.new(id: 1, flags: [:LINKED, :PENDING])

    assert_equal(
      TigerBeetle::TransferFlags::LINKED | TigerBeetle::TransferFlags::PENDING,
      transfer.flags
    )
  end

  def test_flag_writer_coerces
    account = TigerBeetle::Account.new(id: 1, ledger: 1, code: 1)

    account.flags = [:imported, :linked]

    assert_equal(
      TigerBeetle::AccountFlags::IMPORTED | TigerBeetle::AccountFlags::LINKED,
      account.flags
    )
  end

  def test_empty_flag_array_is_none
    filter = TigerBeetle::AccountFilter.new(account_id: 1, limit: 1, flags: [])

    assert_equal(TigerBeetle::AccountFilterFlags::NONE, filter.flags)
  end

  def test_unknown_flag_raises_argument_error_with_dynamic_module_name
    error = assert_raises(ArgumentError) do
      TigerBeetle::QueryFilter.new(limit: 1, flags: [:unknown])
    end

    assert_equal("unknown flag for TigerBeetle::QueryFilterFlags: [:unknown]", error.message)
  end

  def test_invalid_flag_type_raises_type_error_with_dynamic_module_name
    error = assert_raises(TypeError) do
      TigerBeetle::Account.new(id: 1, ledger: 1, code: 1, flags: "linked")
    end

    assert_equal(
      "expected Integer or Array[Symbol] for TigerBeetle::AccountFlags",
      error.message
    )
  end
end
