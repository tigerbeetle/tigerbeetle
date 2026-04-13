require "minitest/autorun"
require "tigerbeetle"

class TestAccounts < Minitest::Test
  def setup
    tb_address = ENV.fetch("TB_ADDRESS", "3000")
    @client = TigerBeetle::Client.new(cluster_id: 0, replica_addresses: tb_address)
  end

  def teardown
    @client.close
  end

  def test_create_account
    account = TigerBeetle::Account.new do |a|
      a.id = TigerBeetle.generate_id
      a.ledger = 1
      a.code = 1
    end

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_operator(results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateAccountStatus::CREATED, results[0].status)
  end

  def test_create_account_duplicate
    account = TigerBeetle::Account.new do |a|
      a.id = TigerBeetle.generate_id
      a.ledger = 1
      a.code = 1
    end

    @client.create_accounts([account])

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::EXISTS, results[0].status)
  end

  def test_create_account_id_zero
    account = TigerBeetle::Account.new do |a|
      a.id = 0
      a.ledger = 1
      a.code = 1
    end

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::ID_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_account_ledger_zero
    account = TigerBeetle::Account.new do |a|
      a.id = TigerBeetle.generate_id
      a.ledger = 0
      a.code = 1
    end

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::LEDGER_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_account_code_zero
    account = TigerBeetle::Account.new do |a|
      a.id = TigerBeetle.generate_id
      a.ledger = 1
      a.code = 0
    end

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::CODE_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_account_mutually_exclusive_flags
    account = TigerBeetle::Account.new do |a|
      a.id = TigerBeetle.generate_id
      a.ledger = 1
      a.code = 1
      a.flags = TigerBeetle::AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS |
        TigerBeetle::AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS
    end

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::FLAGS_ARE_MUTUALLY_EXCLUSIVE, results[0].status)
  end

  def test_lookup_account
    id = TigerBeetle.generate_id
    account = TigerBeetle::Account.new do |a|
      a.id = id
      a.ledger = 1
      a.code = 1
    end

    @client.create_accounts([account])

    results = @client.lookup_accounts([id])
    assert_equal(1, results.length)
    assert_equal(id, results[0].id)
    assert_equal(1, results[0].ledger)
    assert_equal(1, results[0].code)
  end

  def test_lookup_account_not_found
    id = TigerBeetle.generate_id

    results = @client.lookup_accounts([id])
    assert_equal(0, results.length)
  end

  def test_lookup_accounts_multiple
    id1 = TigerBeetle.generate_id
    id2 = TigerBeetle.generate_id
    @client.create_accounts(
      [
        TigerBeetle::Account.new do |a|
          a.id = id1
          a.ledger = 1
          a.code = 1
        end,
        TigerBeetle::Account.new do |a|
          a.id = id2
          a.ledger = 2
          a.code = 2
        end
      ]
    )

    results = @client.lookup_accounts([id1, id2])
    assert_equal(2, results.length)
    r1 = results.find { it.id == id1 }
    r2 = results.find { it.id == id2 }
    refute_nil(r1)
    refute_nil(r2)
    assert_equal(1, r1.ledger)
    assert_equal(2, r2.ledger)
  end

  def test_lookup_accounts_partial_match
    existing_id = TigerBeetle.generate_id
    missing_id = TigerBeetle.generate_id
    @client.create_accounts(
      [
        TigerBeetle::Account.new do |a|
          a.id = existing_id
          a.ledger = 1
          a.code = 1
        end
      ]
    )

    results = @client.lookup_accounts([existing_id, missing_id])
    assert_equal(1, results.length)
    assert_equal(existing_id, results[0].id)
  end

  def test_lookup_accounts_empty_batch
    results = @client.lookup_accounts([])
    assert_equal(0, results.length)
  end

  def test_lookup_account_field_roundtrip
    id = TigerBeetle.generate_id
    user_data_128 = TigerBeetle.generate_id
    user_data_64 = 9_999_999_999
    user_data_32 = 12345
    @client.create_accounts(
      [
        TigerBeetle::Account.new do |a|
          a.id = id
          a.ledger = 7
          a.code = 42
          a.user_data_128 = user_data_128
          a.user_data_64 = user_data_64
          a.user_data_32 = user_data_32
          a.flags = TigerBeetle::AccountFlags::HISTORY
        end
      ]
    )

    results = @client.lookup_accounts([id])
    assert_equal(1, results.length)
    acc = results[0]
    assert_equal(id, acc.id)
    assert_equal(7, acc.ledger)
    assert_equal(42, acc.code)
    assert_equal(user_data_128, acc.user_data_128)
    assert_equal(user_data_64, acc.user_data_64)
    assert_equal(user_data_32, acc.user_data_32)
    assert_equal(TigerBeetle::AccountFlags::HISTORY, acc.flags)
    assert_operator(acc.timestamp, :>, 0)
  end
end
