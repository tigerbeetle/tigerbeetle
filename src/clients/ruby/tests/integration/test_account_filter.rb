require_relative "tiger_beetle_integration_test"

class TestAccountFilter < TigerBeetleIntegrationTest
  def test_get_account_transfers_and_balances
    account_id = TigerBeetle.id
    debit_account_id = TigerBeetle.id
    credit_account_id = TigerBeetle.id
    @client.create_accounts(
      [
        account(
          id: account_id,
          flags: TigerBeetle::AccountFlags::HISTORY
        ),
        account(id: debit_account_id),
        account(id: credit_account_id)
      ]
    )

    transfer_ids = Array.new(4) { TigerBeetle.id }
    @client.create_transfers(
      [
        transfer(
          id: transfer_ids[0],
          debit_account_id: account_id,
          credit_account_id: credit_account_id,
          amount: 10
        ),
        transfer(
          id: transfer_ids[1],
          debit_account_id: debit_account_id,
          credit_account_id: account_id,
          amount: 20
        ),
        transfer(
          id: transfer_ids[2],
          debit_account_id: account_id,
          credit_account_id: credit_account_id,
          amount: 30,
          code: 2
        ),
        transfer(
          id: transfer_ids[3],
          debit_account_id: debit_account_id,
          credit_account_id: account_id,
          amount: 40,
          code: 2
        )
      ]
    )

    filter = TigerBeetle::AccountFilter.new(
      account_id: account_id,
      limit: 10,
      flags: TigerBeetle::AccountFilterFlags::DEBITS | TigerBeetle::AccountFilterFlags::CREDITS
    )

    transfers = @client.get_account_transfers(filter)
    balances = @client.get_account_balances(filter)

    assert_equal(transfer_ids, transfers.map(&:id))
    assert_equal(transfers.map(&:timestamp), balances.map(&:timestamp))
    assert_equal([10, 10, 40, 40], balances.map(&:debits_posted))
    assert_equal([0, 20, 20, 60], balances.map(&:credits_posted))
  end

  def test_get_account_transfers_filters_debits_and_credits
    account_id = TigerBeetle.id
    debit_account_id = TigerBeetle.id
    credit_account_id = TigerBeetle.id
    @client.create_accounts(
      [
        account(id: account_id),
        account(id: debit_account_id),
        account(id: credit_account_id)
      ]
    )

    debit_transfer_id = TigerBeetle.id
    credit_transfer_id = TigerBeetle.id
    @client.create_transfers(
      [
        transfer(
          id: debit_transfer_id,
          debit_account_id: account_id,
          credit_account_id: credit_account_id,
          amount: 10
        ),
        transfer(
          id: credit_transfer_id,
          debit_account_id: debit_account_id,
          credit_account_id: account_id,
          amount: 20
        )
      ]
    )

    debit_transfers = @client.get_account_transfers(
      TigerBeetle::AccountFilter.new(
        account_id: account_id,
        limit: 10,
        flags: TigerBeetle::AccountFilterFlags::DEBITS
      )
    )
    credit_transfers = @client.get_account_transfers(
      TigerBeetle::AccountFilter.new(
        account_id: account_id,
        limit: 10,
        flags: TigerBeetle::AccountFilterFlags::CREDITS
      )
    )

    assert_equal([debit_transfer_id], debit_transfers.map(&:id))
    assert_equal([credit_transfer_id], credit_transfers.map(&:id))
  end

  def test_get_account_transfers_empty_result
    results = @client.get_account_transfers(
      TigerBeetle::AccountFilter.new(account_id: TigerBeetle.id, limit: 10)
    )

    assert_empty(results)
  end

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

  private

  def account(id: TigerBeetle.id, ledger: 1, code: 1, flags: TigerBeetle::AccountFlags::NONE)
    TigerBeetle::Account.new(id:, ledger:, code:, flags:)
  end

  def transfer(
    id: TigerBeetle.id,
    debit_account_id:,
    credit_account_id:,
    amount:,
    ledger: 1,
    code: 1
  )
    TigerBeetle::Transfer.new(id:, debit_account_id:, credit_account_id:, amount:, ledger:, code:)
  end
end
