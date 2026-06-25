require_relative "tiger_beetle_integration_test"

class TestAccounts < TigerBeetleIntegrationTest
  def test_create_account
    account = TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 1, code: 1)

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_operator(results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateAccountStatus::CREATED, results[0].status)
    assert_equal(:created, results[0].status_name)
    assert_equal(
      "#<TigerBeetle::CreateAccountResult timestamp=#{results[0].timestamp} status_name=created>",
      results[0].to_s
    )
  end

  def test_create_account_duplicate
    account = TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 1, code: 1)

    @client.create_accounts([account])

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::EXISTS, results[0].status)
    assert_equal(:exists, results[0].status_name)
  end

  def test_create_account_id_zero
    account = TigerBeetle::Account.new(id: 0, ledger: 1, code: 1)

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::ID_MUST_NOT_BE_ZERO, results[0].status)
    assert_equal(:id_must_not_be_zero, results[0].status_name)
  end

  def test_create_account_ledger_zero
    account = TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 0, code: 1)

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::LEDGER_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_account_code_zero
    account = TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 1, code: 0)

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::CODE_MUST_NOT_BE_ZERO, results[0].status)
  end

  def test_create_account_mutually_exclusive_flags
    account = TigerBeetle::Account.new(
      id: TigerBeetle.id,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS |
        TigerBeetle::AccountFlags::CREDITS_MUST_NOT_EXCEED_DEBITS
    )

    results = @client.create_accounts([account])
    assert_equal(1, results.length)
    assert_equal(TigerBeetle::CreateAccountStatus::FLAGS_ARE_MUTUALLY_EXCLUSIVE, results[0].status)
  end

  def test_lookup_account
    id = TigerBeetle.id
    account = TigerBeetle::Account.new(id: id, ledger: 1, code: 1)

    @client.create_accounts([account])

    results = @client.lookup_accounts([id])
    assert_equal(1, results.length)
    assert_equal(id, results[0].id)
    assert_equal(1, results[0].ledger)
    assert_equal(1, results[0].code)
  end

  def test_lookup_account_not_found
    id = TigerBeetle.id

    results = @client.lookup_accounts([id])
    assert_equal(0, results.length)
  end

  def test_lookup_accounts_multiple
    id1 = TigerBeetle.id
    id2 = TigerBeetle.id
    @client.create_accounts(
      [
        TigerBeetle::Account.new(id: id1, ledger: 1, code: 1),
        TigerBeetle::Account.new(id: id2, ledger: 2, code: 2)
      ]
    )

    results = @client.lookup_accounts([id1, id2])
    assert_equal(2, results.length)
    r1 = results.find { |result| result.id == id1 }
    r2 = results.find { |result| result.id == id2 }
    refute_nil(r1)
    refute_nil(r2)
    assert_equal(1, r1.ledger)
    assert_equal(2, r2.ledger)
  end

  def test_lookup_accounts_partial_match
    existing_id = TigerBeetle.id
    missing_id = TigerBeetle.id
    @client.create_accounts(
      [TigerBeetle::Account.new(id: existing_id, ledger: 1, code: 1)]
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
    id = TigerBeetle.id
    user_data_128 = TigerBeetle.id
    user_data_64 = 9_999_999_999
    user_data_32 = 12345
    @client.create_accounts(
      [
        TigerBeetle::Account.new(
          id: id,
          ledger: 7,
          code: 42,
          user_data_128: user_data_128,
          user_data_64: user_data_64,
          user_data_32: user_data_32,
          flags: TigerBeetle::AccountFlags::HISTORY
        )
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
