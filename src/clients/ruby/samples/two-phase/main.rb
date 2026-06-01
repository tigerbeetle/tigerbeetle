require "tigerbeetle"

replica_addresses = ENV.fetch("TB_ADDRESS", "3000")

TigerBeetle::Client.open(cluster_id: 0, replica_addresses:) do |client|
  account_results = client.create_accounts(
    [
      TigerBeetle::Account.new(id: 1, ledger: 1, code: 1),
      TigerBeetle::Account.new(id: 2, ledger: 1, code: 1)
    ]
  )

  raise "expected 2 account results" unless account_results.length == 2
  unless account_results[0].status == TigerBeetle::CreateAccountStatus::CREATED
    raise "account 1 was not created"
  end

  unless account_results[1].status == TigerBeetle::CreateAccountStatus::CREATED
    raise "account 2 was not created"
  end

  transfer_results = client.create_transfers(
    [
      TigerBeetle::Transfer.new(
        id: 1,
        debit_account_id: 1,
        credit_account_id: 2,
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

  accounts = client.lookup_accounts([1, 2])
  raise "expected 2 accounts before posting" unless accounts.length == 2
  accounts.each do |account|
    case account.id
    when 1
      raise "account 1 debits_posted before posting mismatch" unless account.debits_posted == 0
      raise "account 1 credits_posted before posting mismatch" unless account.credits_posted == 0
      raise "account 1 debits_pending before posting mismatch" unless account.debits_pending == 500
      raise "account 1 credits_pending before posting mismatch" unless account.credits_pending == 0
    when 2
      raise "account 2 debits_posted before posting mismatch" unless account.debits_posted == 0
      raise "account 2 credits_posted before posting mismatch" unless account.credits_posted == 0
      raise "account 2 debits_pending before posting mismatch" unless account.debits_pending == 0
      unless account.credits_pending == 500
        raise "account 2 credits_pending before posting mismatch"
      end
    else
      raise "unexpected account: #{account.inspect}"
    end
  end

  transfer_results = client.create_transfers(
    [
      TigerBeetle::Transfer.new(
        id: 2,
        debit_account_id: 1,
        credit_account_id: 2,
        amount: 500,
        pending_id: 1,
        ledger: 1,
        code: 1,
        flags: TigerBeetle::TransferFlags::POST_PENDING_TRANSFER
      )
    ]
  )

  raise "expected 1 post-pending transfer result" unless transfer_results.length == 1
  unless transfer_results[0].status == TigerBeetle::CreateTransferStatus::CREATED
    raise "post-pending transfer was not created"
  end

  transfers = client.lookup_transfers([1, 2])
  raise "expected 2 transfers" unless transfers.length == 2
  transfers.each do |transfer|
    case transfer.id
    when 1
      pending = TigerBeetle::TransferFlags::PENDING
      unless (transfer.flags & pending) == pending
        raise "transfer 1 was not pending"
      end

    when 2
      post_pending = TigerBeetle::TransferFlags::POST_PENDING_TRANSFER
      unless (transfer.flags & post_pending) == post_pending
        raise "transfer 2 was not post-pending"
      end
    else
      raise "unexpected transfer: #{transfer.inspect}"
    end
  end

  accounts = client.lookup_accounts([1, 2])
  raise "expected 2 accounts after posting" unless accounts.length == 2
  accounts.each do |account|
    case account.id
    when 1
      raise "account 1 debits_posted after posting mismatch" unless account.debits_posted == 500
      raise "account 1 credits_posted after posting mismatch" unless account.credits_posted == 0
      raise "account 1 debits_pending after posting mismatch" unless account.debits_pending == 0
      raise "account 1 credits_pending after posting mismatch" unless account.credits_pending == 0
    when 2
      raise "account 2 debits_posted after posting mismatch" unless account.debits_posted == 0
      raise "account 2 credits_posted after posting mismatch" unless account.credits_posted == 500
      raise "account 2 debits_pending after posting mismatch" unless account.debits_pending == 0
      raise "account 2 credits_pending after posting mismatch" unless account.credits_pending == 0
    else
      raise "unexpected account: #{account.inspect}"
    end
  end

  puts("ok")
end
