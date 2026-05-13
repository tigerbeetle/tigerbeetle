def assert_created(results, status_constant)
  results.each_with_index do |result, index|
    next if result.status == status_constant

    raise "event #{index} failed with status #{result.status}"
  end
end

# section:imports
require "tigerbeetle"

puts("Import OK!")
# endsection:imports

# section:client
replica_addresses = ENV.fetch("TB_ADDRESS", "3000")

TigerBeetle::Client.open(cluster_id: 0, replica_addresses:) do |client|
  # Use the client.
end
# endsection:client

TigerBeetle::Client.open(cluster_id: 0, replica_addresses:) do |client|
  # section:create-accounts
  account = TigerBeetle::Account.new(
    id: TigerBeetle.id,
    debits_pending: 0,
    debits_posted: 0,
    credits_pending: 0,
    credits_posted: 0,
    user_data_128: 0,
    user_data_64: 0,
    user_data_32: 0,
    ledger: 1,
    code: 718,
    flags: TigerBeetle::AccountFlags::NONE,
    timestamp: 0
  )

  account_results = client.create_accounts([account])
  # Results handling omitted.
  # endsection:create-accounts
  assert_created(account_results, TigerBeetle::CreateAccountStatus::CREATED)

  # section:account-flags
  account0 = TigerBeetle::Account.new(
    id: TigerBeetle.id,
    ledger: 1,
    code: 1,
    flags: TigerBeetle::AccountFlags::LINKED |
      TigerBeetle::AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS
  )
  account1 = TigerBeetle::Account.new(
    id: TigerBeetle.id,
    ledger: 1,
    code: 1,
    flags: TigerBeetle::AccountFlags::HISTORY
  )

  account_results = client.create_accounts([account0, account1])
  # Results handling omitted.
  # endsection:account-flags
  assert_created(account_results, TigerBeetle::CreateAccountStatus::CREATED)

  # section:create-accounts-errors
  accounts = [
    TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 1, code: 1),
    TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 1, code: 1),
    TigerBeetle::Account.new(id: TigerBeetle.id, ledger: 1, code: 1)
  ]

  account_results = client.create_accounts(accounts)
  account_results.each_with_index do |result, index|
    case result.status
    when TigerBeetle::CreateAccountStatus::CREATED
      puts("Batch account at #{index} successfully created with timestamp #{result.timestamp}.")
    when TigerBeetle::CreateAccountStatus::EXISTS
      puts("Batch account at #{index} already exists with timestamp #{result.timestamp}.")
    else
      puts("Batch account at #{index} failed to create: #{result.status}.")
    end
  end
  # endsection:create-accounts-errors

  transfer_debit_account_id = accounts[0].id
  transfer_credit_account_id = account1.id

  # section:lookup-accounts
  accounts = client.lookup_accounts([account0.id, account1.id])
  # endsection:lookup-accounts
  raise "expected 2 accounts" unless accounts.length == 2

  # section:create-transfers
  transfers = [
    TigerBeetle::Transfer.new(
      id: TigerBeetle.id,
      debit_account_id: transfer_debit_account_id,
      credit_account_id: transfer_credit_account_id,
      amount: 10,
      pending_id: 0,
      user_data_128: 0,
      user_data_64: 0,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: TigerBeetle::TransferFlags::NONE,
      timestamp: 0
    )
  ]

  transfer_results = client.create_transfers(transfers)
  # Results handling omitted.
  # endsection:create-transfers
  assert_created(transfer_results, TigerBeetle::CreateTransferStatus::CREATED)

  # section:create-transfers-errors
  batch = [
    TigerBeetle::Transfer.new(
      id: TigerBeetle.id,
      debit_account_id: transfer_debit_account_id,
      credit_account_id: transfer_credit_account_id,
      amount: 10,
      ledger: 1,
      code: 720
    ),
    TigerBeetle::Transfer.new(
      id: TigerBeetle.id,
      debit_account_id: transfer_debit_account_id,
      credit_account_id: transfer_credit_account_id,
      amount: 10,
      ledger: 1,
      code: 720
    ),
    TigerBeetle::Transfer.new(
      id: TigerBeetle.id,
      debit_account_id: transfer_debit_account_id,
      credit_account_id: transfer_credit_account_id,
      amount: 10,
      ledger: 1,
      code: 720
    )
  ]

  transfer_results = client.create_transfers(batch)
  transfer_results.each_with_index do |result, index|
    case result.status
    when TigerBeetle::CreateTransferStatus::CREATED
      puts("Batch transfer at #{index} successfully created with timestamp #{result.timestamp}.")
    when TigerBeetle::CreateTransferStatus::EXISTS
      puts("Batch transfer at #{index} already exists with timestamp #{result.timestamp}.")
    else
      puts("Batch transfer at #{index} failed to create: #{result.status}.")
    end
  end
  # endsection:create-transfers-errors

  # section:batch
  # Array of transfers to create.
  batch = []
  batch.each_slice(8189) do |slice|
    transfer_results = client.create_transfers(slice)
    # Results handling omitted.
  end
  # endsection:batch

  # section:transfer-flags-link
  transfer0 = TigerBeetle::Transfer.new(
    id: TigerBeetle.id,
    debit_account_id: transfer_debit_account_id,
    credit_account_id: transfer_credit_account_id,
    amount: 10,
    ledger: 1,
    code: 720,
    flags: TigerBeetle::TransferFlags::LINKED
  )
  transfer1 = TigerBeetle::Transfer.new(
    id: TigerBeetle.id,
    debit_account_id: transfer_debit_account_id,
    credit_account_id: transfer_credit_account_id,
    amount: 10,
    ledger: 1,
    code: 720
  )

  transfer_results = client.create_transfers([transfer0, transfer1])
  # Results handling omitted.
  # endsection:transfer-flags-link
  assert_created(transfer_results, TigerBeetle::CreateTransferStatus::CREATED)

  # section:transfer-flags-post
  pending_transfer = TigerBeetle::Transfer.new(
    id: TigerBeetle.id,
    debit_account_id: transfer_debit_account_id,
    credit_account_id: transfer_credit_account_id,
    amount: 10,
    ledger: 1,
    code: 720,
    flags: TigerBeetle::TransferFlags::PENDING
  )

  transfer_results = client.create_transfers([pending_transfer])
  # Results handling omitted.

  post_transfer = TigerBeetle::Transfer.new(
    id: TigerBeetle.id,
    debit_account_id: transfer_debit_account_id,
    credit_account_id: transfer_credit_account_id,
    amount: 10,
    pending_id: pending_transfer.id,
    ledger: 1,
    code: 720,
    flags: TigerBeetle::TransferFlags::POST_PENDING_TRANSFER
  )

  transfer_results = client.create_transfers([post_transfer])
  # Results handling omitted.
  # endsection:transfer-flags-post
  assert_created(transfer_results, TigerBeetle::CreateTransferStatus::CREATED)

  # section:transfer-flags-void
  pending_transfer = TigerBeetle::Transfer.new(
    id: TigerBeetle.id,
    debit_account_id: transfer_debit_account_id,
    credit_account_id: transfer_credit_account_id,
    amount: 10,
    ledger: 1,
    code: 720,
    flags: TigerBeetle::TransferFlags::PENDING
  )

  transfer_results = client.create_transfers([pending_transfer])
  # Results handling omitted.

  void_transfer = TigerBeetle::Transfer.new(
    id: TigerBeetle.id,
    debit_account_id: transfer_debit_account_id,
    credit_account_id: transfer_credit_account_id,
    amount: 0,
    pending_id: pending_transfer.id,
    ledger: 1,
    code: 720,
    flags: TigerBeetle::TransferFlags::VOID_PENDING_TRANSFER
  )

  transfer_results = client.create_transfers([void_transfer])
  # Results handling omitted.
  # endsection:transfer-flags-void
  assert_created(transfer_results, TigerBeetle::CreateTransferStatus::CREATED)

  # section:lookup-transfers
  transfers = client.lookup_transfers([transfer0.id, transfer1.id])
  # endsection:lookup-transfers
  raise "expected 2 transfers" unless transfers.length == 2

  # section:get-account-transfers
  filter = TigerBeetle::AccountFilter.new(
    account_id: account1.id,
    user_data_128: 0,
    user_data_64: 0,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0,
    timestamp_max: 0,
    limit: 10,
    flags: TigerBeetle::AccountFilterFlags::DEBITS |
      TigerBeetle::AccountFilterFlags::CREDITS |
      TigerBeetle::AccountFilterFlags::REVERSED
  )

  account_transfers = client.get_account_transfers(filter)
  # endsection:get-account-transfers
  raise "expected account transfers" if account_transfers.empty?

  # section:get-account-balances
  filter = TigerBeetle::AccountFilter.new(
    account_id: account1.id,
    user_data_128: 0,
    user_data_64: 0,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0,
    timestamp_max: 0,
    limit: 10,
    flags: TigerBeetle::AccountFilterFlags::DEBITS |
      TigerBeetle::AccountFilterFlags::CREDITS |
      TigerBeetle::AccountFilterFlags::REVERSED
  )

  account_balances = client.get_account_balances(filter)
  # endsection:get-account-balances
  raise "expected account balances" if account_balances.empty?

  # section:query-accounts
  query_filter = TigerBeetle::QueryFilter.new(
    user_data_128: 0,
    user_data_64: 0,
    user_data_32: 0,
    ledger: 1,
    code: 1,
    timestamp_min: 0,
    timestamp_max: 0,
    limit: 10,
    flags: TigerBeetle::QueryFilterFlags::REVERSED
  )

  query_accounts = client.query_accounts(query_filter)
  # endsection:query-accounts
  raise "expected query accounts" if query_accounts.empty?

  # section:query-transfers
  query_filter = TigerBeetle::QueryFilter.new(
    user_data_128: 0,
    user_data_64: 0,
    user_data_32: 0,
    ledger: 1,
    code: 720,
    timestamp_min: 0,
    timestamp_max: 0,
    limit: 10,
    flags: TigerBeetle::QueryFilterFlags::REVERSED
  )

  query_transfers = client.query_transfers(query_filter)
  # endsection:query-transfers
  raise "expected query transfers" if query_transfers.empty?

  # section:linked-events
  linked_flag = TigerBeetle::TransferFlags::LINKED
  batch = [
    TigerBeetle::Transfer.new(
      id: TigerBeetle.id,
      debit_account_id: transfer_debit_account_id,
      credit_account_id: transfer_credit_account_id,
      amount: 1,
      ledger: 1,
      code: 720,
      flags: linked_flag
    ),
    TigerBeetle::Transfer.new(
      id: TigerBeetle.id,
      debit_account_id: transfer_debit_account_id,
      credit_account_id: transfer_credit_account_id,
      amount: 1,
      ledger: 1,
      code: 720
    )
  ]

  transfer_results = client.create_transfers(batch)
  # Results handling omitted.
  # endsection:linked-events
  assert_created(transfer_results, TigerBeetle::CreateTransferStatus::CREATED)

  # section:imported-events
  historical_timestamp = 0
  # Loaded from an external source.
  historical_accounts = []
  # Loaded from an external source.
  historical_transfers = []

  accounts_to_import = historical_accounts.map.with_index do |historical_account, index|
    historical_timestamp += 1
    historical_account.timestamp = historical_timestamp
    historical_account.flags = TigerBeetle::AccountFlags::IMPORTED
    if index < historical_accounts.length - 1
      historical_account.flags |= TigerBeetle::AccountFlags::LINKED
    end

    historical_account
  end

  account_results = client.create_accounts(accounts_to_import)
  # Results handling omitted.

  transfers_to_import = historical_transfers.map.with_index do |historical_transfer, index|
    historical_timestamp += 1
    historical_transfer.timestamp = historical_timestamp
    historical_transfer.flags = TigerBeetle::TransferFlags::IMPORTED
    if index < historical_transfers.length - 1
      historical_transfer.flags |= TigerBeetle::TransferFlags::LINKED
    end

    historical_transfer
  end

  transfer_results = client.create_transfers(transfers_to_import)
  # Results handling omitted.
  # endsection:imported-events

  puts("ok")
end
