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

  # Create a transfer.
  transfer_results = client.create_transfers(
    [
      TigerBeetle::Transfer.new(
        id: TigerBeetle.id,
        debit_account_id: account_dr_id,
        credit_account_id: account_cr_id,
        amount: 10,
        ledger: 1,
        code: 1
      )
    ]
  )

  raise "expected 1 transfer result" unless transfer_results.length == 1
  unless transfer_results[0].status == TigerBeetle::CreateTransferStatus::CREATED
    raise "transfer was not created"
  end

  # Fetch and validate account balances.
  accounts = client.lookup_accounts([account_dr_id, account_cr_id])
  raise "expected 2 accounts" unless accounts.length == 2

  accounts.each do |account|
    case account.id
    when account_dr_id
      raise "DR debits_posted mismatch" unless account.debits_posted == 10
      raise "DR credits_posted mismatch" unless account.credits_posted == 0
    when account_cr_id
      raise "CR debits_posted mismatch" unless account.debits_posted == 0
      raise "CR credits_posted mismatch" unless account.credits_posted == 10
    else
      raise "unexpected account: #{account.inspect}"
    end
  end

  puts("ok")
end
