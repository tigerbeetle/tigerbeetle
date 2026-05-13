require "tigerbeetle"

def assert_accounts(accounts, expected)
  raise "expected #{expected.length} accounts" unless accounts.length == expected.length

  accounts.each do |account|
    values = expected.fetch(account.id) { raise "unexpected account: #{account.inspect}" }
    unless account.debits_posted == values.fetch(:debits_posted)
      raise "account #{account.id} debits_posted mismatch"
    end

    unless account.credits_posted == values.fetch(:credits_posted)
      raise "account #{account.id} credits_posted mismatch"
    end

    unless account.debits_pending == values.fetch(:debits_pending)
      raise "account #{account.id} debits_pending mismatch"
    end

    unless account.credits_pending == values.fetch(:credits_pending)
      raise "account #{account.id} credits_pending mismatch"
    end
  end
end

replica_addresses = ENV.fetch("TB_ADDRESS", "3000")

TigerBeetle::Client.open(cluster_id: 0, replica_addresses:) do |client|
  account_results = client.create_accounts(
    [
      TigerBeetle::Account.new(id: 1, ledger: 1, code: 1),
      TigerBeetle::Account.new(id: 2, ledger: 1, code: 1)
    ]
  )

  raise "expected 2 account results" unless account_results.length == 2
  account_results.each.with_index(1) do |result, index|
    unless result.status == TigerBeetle::CreateAccountStatus::CREATED
      raise "account #{index} was not created"
    end
  end

  transfers = (1..5).map do |id|
    TigerBeetle::Transfer.new(
      id: id,
      debit_account_id: 1,
      credit_account_id: 2,
      amount: id * 100,
      ledger: 1,
      code: 1,
      flags: TigerBeetle::TransferFlags::PENDING
    )
  end

  transfer_results = client.create_transfers(transfers)
  unless transfer_results.length == transfers.length
    raise "expected #{transfers.length} pending transfer results"
  end

  transfer_results.each do |result|
    unless result.status == TigerBeetle::CreateTransferStatus::CREATED
      raise "pending transfer was not created"
    end
  end

  assert_accounts(
    client.lookup_accounts([1, 2]),
    {
      1 => {
        debits_posted: 0,
        credits_posted: 0,
        debits_pending: 1500,
        credits_pending: 0
      },
      2 => {debits_posted: 0, credits_posted: 0, debits_pending: 0, credits_pending: 1500}
    }
  )

  operations = [
    [6, 1, 100, TigerBeetle::TransferFlags::POST_PENDING_TRANSFER, 100, 1400],
    [7, 2, 200, TigerBeetle::TransferFlags::VOID_PENDING_TRANSFER, 100, 1200],
    [8, 3, 300, TigerBeetle::TransferFlags::POST_PENDING_TRANSFER, 400, 900],
    [9, 4, 400, TigerBeetle::TransferFlags::VOID_PENDING_TRANSFER, 400, 500],
    [10, 5, 500, TigerBeetle::TransferFlags::POST_PENDING_TRANSFER, 900, 0]
  ]

  operations.each do |id, pending_id, amount, flags, posted, pending|
    transfer_results = client.create_transfers(
      [
        TigerBeetle::Transfer.new(
          id: id,
          debit_account_id: 1,
          credit_account_id: 2,
          amount: amount,
          pending_id: pending_id,
          ledger: 1,
          code: 1,
          flags: flags
        )
      ]
    )

    raise "expected 1 finishing transfer result" unless transfer_results.length == 1
    unless transfer_results[0].status == TigerBeetle::CreateTransferStatus::CREATED
      raise "finishing transfer #{id} was not created"
    end

    assert_accounts(
      client.lookup_accounts([1, 2]),
      {
        1 => {
          debits_posted: posted,
          credits_posted: 0,
          debits_pending: pending,
          credits_pending: 0
        },
        2 => {debits_posted: 0, credits_posted: posted, debits_pending: 0, credits_pending: pending}
      }
    )
  end

  puts("ok")
end
