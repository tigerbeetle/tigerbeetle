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
end
