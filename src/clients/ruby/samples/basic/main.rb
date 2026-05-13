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

  accounts = client.lookup_accounts([1, 2])
  raise "expected 2 accounts" unless accounts.length == 2

  accounts.each do |account|
    case account.id
    when 1
      raise "account 1 debits_posted mismatch" unless account.debits_posted == 10
      raise "account 1 credits_posted mismatch" unless account.credits_posted == 0
    when 2
      raise "account 2 debits_posted mismatch" unless account.debits_posted == 0
      raise "account 2 credits_posted mismatch" unless account.credits_posted == 10
    else
      raise "unexpected account: #{account.inspect}"
    end
  end

  puts("ok")
end
