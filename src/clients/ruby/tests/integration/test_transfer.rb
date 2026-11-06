require_relative "tiger_beetle_integration_test"

class TestTransfers < TigerBeetleIntegrationTest
  def setup
    super

    @a1_id = TigerBeetle.id
    @a2_id = TigerBeetle.id
    @client.create_accounts(
      [
        TigerBeetle::Account.new(id: @a1_id, ledger: 1, code: 1),
        TigerBeetle::Account.new(id: @a2_id, ledger: 1, code: 1)
      ]
    )
  end

  def test_create_transfer
    results = @client.create_transfers(
      [
        TigerBeetle::Transfer.new(
          id: TigerBeetle.id,
          debit_account_id: @a1_id,
          credit_account_id: @a2_id,
          amount: 100,
          ledger: 1,
          code: 1
        )
      ]
    )
    assert_equal(1, results.length)
    assert_operator(results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::CREATED, results[0].status)
    assert_equal(:created, results[0].status_name)
  end

end
