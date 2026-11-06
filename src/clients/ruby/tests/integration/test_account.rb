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

end
