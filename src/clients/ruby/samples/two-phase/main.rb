require "tigerbeetle"

replica_addresses = ENV.fetch("TB_ADDRESS", "3000")

TigerBeetle::Client.open(cluster_id: 0, replica_addresses:) do |client|
  # Create accounts.
  account_dr_id = TigerBeetle.id
  account_cr_id = TigerBeetle.id

  account_results = client.create_accounts(
    [
      TigerBeetle::Account.new(id: account_dr_id, ledger: 1, code: 1),
      TigerBeetle::Account.new(id: account_cr_id, ledger: 1, code: 1)
    ]
  )

  raise "expected 2 account results" unless account_results.length == 2
  unless account_results[0].status == TigerBeetle::CreateAccountStatus::CREATED
    raise "account DR was not created"
  end

  unless account_results[1].status == TigerBeetle::CreateAccountStatus::CREATED
    raise "account CR was not created"
  end

  # Create pending transfer.
  transfer_pending_id = TigerBeetle.id
  transfer_results = client.create_transfers(
    [
      TigerBeetle::Transfer.new(
        id: transfer_pending_id,
        debit_account_id: account_dr_id,
        credit_account_id: account_cr_id,
        amount: 500,
        ledger: 1,
        code: 1,
        flags: TigerBeetle::TransferFlags::PENDING
      )
    ]
  )

  raise "expected 1 pending transfer result" unless transfer_results.length == 1
  raise "pending transfer timestamp was not set" unless transfer_results[0].timestamp.positive?
  unless transfer_results[0].status == TigerBeetle::CreateTransferStatus::CREATED
    raise "pending transfer was not created"
  end

  # Fetch and validate pending account balances.
  accounts = client.lookup_accounts([account_dr_id, account_cr_id])
  raise "expected 2 accounts before posting" unless accounts.length == 2
  accounts.each do |account|
    case account.id
    when account_dr_id
      raise "DR debits_posted before posting mismatch" unless account.debits_posted == 0
      raise "DR credits_posted before posting mismatch" unless account.credits_posted == 0
      raise "DR debits_pending before posting mismatch" unless account.debits_pending == 500
      raise "DR credits_pending before posting mismatch" unless account.credits_pending == 0
    when account_cr_id
      raise "CR debits_posted before posting mismatch" unless account.debits_posted == 0
      raise "CR credits_posted before posting mismatch" unless account.credits_posted == 0
      raise "CR debits_pending before posting mismatch" unless account.debits_pending == 0
      raise "CR credits_pending before posting mismatch" unless account.credits_pending == 500
    else
      raise "unexpected account: #{account.inspect}"
    end
  end

  # Post pending transfer.
  transfer_results = client.create_transfers(
    [
      TigerBeetle::Transfer.new(
        id: TigerBeetle.id,
        debit_account_id: account_dr_id,
        credit_account_id: account_cr_id,
        pending_id: transfer_pending_id,
        ledger: 1,
        code: 1,
        # You can post any amount between zero and the pending transfer amount,
        # or void the two-phase transfer using the `VOID_PENDING_TRANSFER` flag.
        amount: 500,
        flags: TigerBeetle::TransferFlags::POST_PENDING_TRANSFER
      )
    ]
  )

  raise "expected 1 post-pending transfer result" unless transfer_results.length == 1
  unless transfer_results[0].status == TigerBeetle::CreateTransferStatus::CREATED
    raise "post-pending transfer was not created"
  end

  # Fetch and validate final account balances.
  accounts = client.lookup_accounts([account_dr_id, account_cr_id])
  raise "expected 2 accounts after posting" unless accounts.length == 2
  accounts.each do |account|
    case account.id
    when account_dr_id
      raise "DR debits_posted after posting mismatch" unless account.debits_posted == 500
      raise "DR credits_posted after posting mismatch" unless account.credits_posted == 0
      raise "DR debits_pending after posting mismatch" unless account.debits_pending == 0
      raise "DR credits_pending after posting mismatch" unless account.credits_pending == 0
    when account_cr_id
      raise "CR debits_posted after posting mismatch" unless account.debits_posted == 0
      raise "CR credits_posted after posting mismatch" unless account.credits_posted == 500
      raise "CR debits_pending after posting mismatch" unless account.debits_pending == 0
      raise "CR credits_pending after posting mismatch" unless account.credits_pending == 0
    else
      raise "unexpected account: #{account.inspect}"
    end
  end

  puts("ok")
end
