require_relative "tiger_beetle_integration_test"

class TestTwoPhaseTransfers < TigerBeetleIntegrationTest
  ACCOUNT_A_ID = 17
  ACCOUNT_B_ID = 19
  AMOUNT_MAX = (1 << 128) - 1

  # Minitest randomizes test order by default. There are two ways to match the
  # test structure of Node and Python:
  #   1. Use the `i_suck_and_my_tests_are_order_dependent!` class method.
  #   2. Have one large order-dependent test.
  # We opted for option 2 here.
  def test_two_phase_transfers
    # Node/Python: create accounts.
    account_results = @client.create_accounts(
      [
        TigerBeetle::Account.new(id: ACCOUNT_A_ID, ledger: 1, code: 718)
      ]
    )

    assert_equal(1, account_results.length)
    assert_operator(account_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateAccountStatus::CREATED, account_results[0].status)

    account_results = @client.create_accounts(
      [
        TigerBeetle::Account.new(id: ACCOUNT_A_ID, ledger: 1, code: 718),
        TigerBeetle::Account.new(id: ACCOUNT_B_ID, ledger: 1, code: 719)
      ]
    )

    assert_equal(2, account_results.length)
    assert_operator(account_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateAccountStatus::EXISTS, account_results[0].status)
    assert_operator(account_results[1].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateAccountStatus::CREATED, account_results[1].status)

    # Node/Python: can create a transfer.
    transfer = TigerBeetle::Transfer.new(
      id: 1,
      debit_account_id: ACCOUNT_B_ID,
      credit_account_id: ACCOUNT_A_ID,
      amount: 100,
      ledger: 1,
      code: 1
    )

    transfers_results = @client.create_transfers([transfer])
    assert_equal(1, transfers_results.length)
    assert_operator(transfers_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::CREATED, transfers_results[0].status)

    accounts = @client.lookup_accounts([ACCOUNT_A_ID, ACCOUNT_B_ID])
    assert_equal(2, accounts.length)
    assert_equal(100, accounts[0].credits_posted)
    assert_equal(0, accounts[0].credits_pending)
    assert_equal(0, accounts[0].debits_posted)
    assert_equal(0, accounts[0].debits_pending)

    assert_equal(0, accounts[1].credits_posted)
    assert_equal(0, accounts[1].credits_pending)
    assert_equal(100, accounts[1].debits_posted)
    assert_equal(0, accounts[1].debits_pending)

    # Node/Python: can create a two-phase transfer.
    transfer = TigerBeetle::Transfer.new(
      id: 2,
      debit_account_id: ACCOUNT_B_ID,
      credit_account_id: ACCOUNT_A_ID,
      amount: 50,
      pending_id: 0,
      timeout: 2_000_000_000,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::TransferFlags::PENDING,
      timestamp: 0
    )

    transfers_results = @client.create_transfers([transfer])
    assert_equal(1, transfers_results.length)
    assert_operator(transfers_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::CREATED, transfers_results[0].status)

    accounts = @client.lookup_accounts([ACCOUNT_A_ID, ACCOUNT_B_ID])
    assert_equal(2, accounts.length)
    assert_equal(100, accounts[0].credits_posted)
    assert_equal(50, accounts[0].credits_pending)
    assert_equal(0, accounts[0].debits_posted)
    assert_equal(0, accounts[0].debits_pending)

    assert_equal(0, accounts[1].credits_posted)
    assert_equal(0, accounts[1].credits_pending)
    assert_equal(100, accounts[1].debits_posted)
    assert_equal(50, accounts[1].debits_pending)

    transfers = @client.lookup_transfers([transfer.id])
    assert_equal(1, transfers.length)
    assert_equal(2, transfers[0].id)
    assert_equal(ACCOUNT_B_ID, transfers[0].debit_account_id)
    assert_equal(ACCOUNT_A_ID, transfers[0].credit_account_id)
    assert_equal(50, transfers[0].amount)
    assert_equal(0, transfers[0].user_data_128)
    assert_equal(0, transfers[0].user_data_64)
    assert_equal(0, transfers[0].user_data_32)
    assert_operator(transfers[0].timeout, :>, 0)
    assert_equal(1, transfers[0].code)
    assert_equal(2, transfers[0].flags)
    assert_equal(transfers_results[0].timestamp, transfers[0].timestamp)
    assert_operator(transfers[0].timestamp, :>, 0)

    # Node/Python: can post a two-phase transfer.
    commit = TigerBeetle::Transfer.new(
      id: 3,
      debit_account_id: 0,
      credit_account_id: 0,
      amount: AMOUNT_MAX,
      pending_id: 2,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::TransferFlags::POST_PENDING_TRANSFER,
      timestamp: 0
    )

    transfers_results = @client.create_transfers([commit])
    assert_equal(1, transfers_results.length)
    assert_operator(transfers_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::CREATED, transfers_results[0].status)

    accounts = @client.lookup_accounts([ACCOUNT_A_ID, ACCOUNT_B_ID])
    assert_equal(2, accounts.length)
    assert_equal(150, accounts[0].credits_posted)
    assert_equal(0, accounts[0].credits_pending)
    assert_equal(0, accounts[0].debits_posted)
    assert_equal(0, accounts[0].debits_pending)

    assert_equal(0, accounts[1].credits_posted)
    assert_equal(0, accounts[1].credits_pending)
    assert_equal(150, accounts[1].debits_posted)
    assert_equal(0, accounts[1].debits_pending)

    # Node/Python: can reject a two-phase transfer.
    transfer = TigerBeetle::Transfer.new(
      id: 4,
      debit_account_id: ACCOUNT_B_ID,
      credit_account_id: ACCOUNT_A_ID,
      amount: 50,
      pending_id: 0,
      timeout: 1_000_000_000,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::TransferFlags::PENDING,
      timestamp: 0
    )
    transfers_results = @client.create_transfers([transfer])
    assert_equal(1, transfers_results.length)
    assert_operator(transfers_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::CREATED, transfers_results[0].status)

    reject = TigerBeetle::Transfer.new(
      id: 5,
      debit_account_id: 0,
      credit_account_id: 0,
      amount: 0,
      pending_id: 4,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::TransferFlags::VOID_PENDING_TRANSFER,
      timestamp: 0
    )

    transfers_results = @client.create_transfers([reject])
    assert_equal(1, transfers_results.length)
    assert_operator(transfers_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::CREATED, transfers_results[0].status)

    accounts = @client.lookup_accounts([ACCOUNT_A_ID, ACCOUNT_B_ID])
    assert_equal(2, accounts.length)
    assert_equal(150, accounts[0].credits_posted)
    assert_equal(0, accounts[0].credits_pending)
    assert_equal(0, accounts[0].debits_posted)
    assert_equal(0, accounts[0].debits_pending)

    assert_equal(0, accounts[1].credits_posted)
    assert_equal(0, accounts[1].credits_pending)
    assert_equal(150, accounts[1].debits_posted)
    assert_equal(0, accounts[1].debits_pending)

    # Node/Python: cannot void an expired transfer.
    transfer = TigerBeetle::Transfer.new(
      id: 6,
      debit_account_id: ACCOUNT_B_ID,
      credit_account_id: ACCOUNT_A_ID,
      amount: 50,
      pending_id: 0,
      timeout: 1,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::TransferFlags::PENDING,
      timestamp: 0
    )
    transfers_results = @client.create_transfers([transfer])
    assert_equal(1, transfers_results.length)
    assert_operator(transfers_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::CREATED, transfers_results[0].status)

    accounts = @client.lookup_accounts([ACCOUNT_A_ID, ACCOUNT_B_ID])
    assert_equal(2, accounts.length)
    assert_equal(150, accounts[0].credits_posted)
    assert_equal(50, accounts[0].credits_pending)
    assert_equal(0, accounts[0].debits_posted)
    assert_equal(0, accounts[0].debits_pending)

    assert_equal(0, accounts[1].credits_posted)
    assert_equal(0, accounts[1].credits_pending)
    assert_equal(150, accounts[1].debits_posted)
    assert_equal(50, accounts[1].debits_pending)

    sleep(1.5)

    accounts = @client.lookup_accounts([ACCOUNT_A_ID, ACCOUNT_B_ID])
    assert_equal(2, accounts.length)
    assert_equal(150, accounts[0].credits_posted)
    assert_equal(0, accounts[0].credits_pending)
    assert_equal(0, accounts[0].debits_posted)
    assert_equal(0, accounts[0].debits_pending)

    assert_equal(0, accounts[1].credits_posted)
    assert_equal(0, accounts[1].credits_pending)
    assert_equal(150, accounts[1].debits_posted)
    assert_equal(0, accounts[1].debits_pending)

    reject = TigerBeetle::Transfer.new(
      id: 7,
      debit_account_id: 0,
      credit_account_id: 0,
      amount: 0,
      pending_id: 6,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::TransferFlags::VOID_PENDING_TRANSFER,
      timestamp: 0
    )

    transfers_results = @client.create_transfers([reject])
    assert_equal(1, transfers_results.length)
    assert_operator(transfers_results[0].timestamp, :>, 0)
    assert_equal(TigerBeetle::CreateTransferStatus::PENDING_TRANSFER_EXPIRED, transfers_results[0].status)
  end
end
