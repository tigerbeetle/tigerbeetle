// section:imports
const { id } = require("tigerbeetle-node");
const { createClient } = require("tigerbeetle-node");
const process = require("process");

console.log("Import ok!");
// endsection:imports

const {
  AccountFlags,
  TransferFlags,
  CreateTransferError,
  CreateAccountError,
  AccountFilterFlags,
  QueryFilterFlags,
  amount_max,
} = require("tigerbeetle-node");

async function main() {
  // section:client
  const client = createClient({
    cluster_id: 0n,
    replica_addresses: [process.env.TB_ADDRESS || "3000"],
  });
  // endsection:client

  // The examples currently throws because the batch is actually invalid (most of fields are
  // undefined). Ideally, we prepare a correct batch here while keeping the syntax compact,
  // for the example, but for the time being lets prioritize a readable example and just
  // swallow the error.

  try {
    // section:create-accounts
    const account = {
      id: id(), // TigerBeetle time-based ID.
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      reserved: 0,
      ledger: 1,
      code: 718,
      flags: 0,
      timestamp: 0n,
    };

    const account_errors = await client.createAccounts([account]);
    // Error handling omitted.
    // endsection:create-accounts
  } catch (exception) {}

  try {
    // section:account-flags
    const account0 = {
      id: 100n,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      reserved: 0,
      ledger: 1,
      code: 1,
      timestamp: 0n,
      flags: AccountFlags.linked | AccountFlags.debits_must_not_exceed_credits,
    };
    const account1 = {
      id: 101n,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      reserved: 0,
      ledger: 1,
      code: 1,
      timestamp: 0n,
      flags: AccountFlags.history,
    };

    const account_errors = await client.createAccounts([account0, account1]);
    // Error handling omitted.
    // endsection:account-flags
  } catch (exception) {}

  try {
    // section:create-accounts-errors
    const account0 = {
      id: 102n,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      reserved: 0,
      ledger: 1,
      code: 1,
      timestamp: 0n,
      flags: 0,
    };
    const account1 = {
      id: 103n,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      reserved: 0,
      ledger: 1,
      code: 1,
      timestamp: 0n,
      flags: 0,
    };
    const account2 = {
      id: 104n,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      reserved: 0,
      ledger: 1,
      code: 1,
      timestamp: 0n,
      flags: 0,
    };

    const account_errors = await client.createAccounts([account0, account1, account2]);
    for (const error of account_errors) {
      switch (error.result) {
        case CreateAccountError.exists:
          console.error(`Batch account at ${error.index} already exists.`);
          break;
        default:
          console.error(
            `Batch account at ${error.index} failed to create: ${
              CreateAccountError[error.result]
            }.`,
          );
      }
    }
    // endsection:create-accounts-errors
  } catch (exception) {}

  try {
    // section:lookup-accounts
    const accounts = await client.lookupAccounts([100n, 101n]);
    // endsection:lookup-accounts
   } catch (exception) {}

   try {
    // section:create-transfers
    const transfers = [{
      id: id(), // TigerBeetle time-based ID.
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: 0,
      timestamp: 0n,
    }];

    const transfer_errors = await client.createTransfers(transfers);
    // Error handling omitted.
    // endsection:create-transfers
  } catch (exception) {}

  try {
    // section:create-transfers-errors
    const transfers = [{
      id: 1n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: 0,
      timestamp: 0n,
    },
    {
      id: 2n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: 0,
      timestamp: 0n,
    },
    {
      id: 3n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: 0,
      timestamp: 0n,
    }];

    const transfer_errors = await client.createTransfers(batch);
    for (const error of transfer_errors) {
      switch (error.result) {
        case CreateTransferError.exists:
          console.error(`Batch transfer at ${error.index} already exists.`);
          break;
        default:
          console.error(
            `Batch transfer at ${error.index} failed to create: ${
              CreateTransferError[error.result]
            }.`,
          );
      }
    }
    // endsection:create-transfers-errors
  } catch (exception) {}

  try {
    // section:no-batch
    const batch = []; // Array of transfer to create.
    for (let i = 0; i < batch.len; i++) {
      const transfer_errors = await client.createTransfers(batch[i]);
      // Error handling omitted.
    }
    // endsection:no-batch
  } catch (exception) {}

  try {
    // section:batch
    const batch = []; // Array of transfer to create.
    const BATCH_SIZE = 8189;
    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const transfer_errors = await client.createTransfers(
        batch.slice(i, Math.min(batch.length, BATCH_SIZE)),
      );
      // Error handling omitted.
    }
    // endsection:batch
  } catch (exception) {}

  try {
    // section:transfer-flags-link
    const transfer0 = {
      id: 4n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: TransferFlags.linked,
      timestamp: 0n,
    };
    const transfer1 = {
      id: 5n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: 0,
      timestamp: 0n,
    };

    // Create the transfer
    const transfer_errors = await client.createTransfers([transfer0, transfer1]);
    // Error handling omitted.
    // endsection:transfer-flags-link
  } catch (exception) {}

  try {
    // section:transfer-flags-post
    const transfer0 = {
      id: 6n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: TransferFlags.pending,
      timestamp: 0n,
    };

    let transfer_errors = await client.createTransfers([transfer0]);
    // Error handling omitted.

    const transfer1 = {
      id: 7n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      // Post the entire pending amount.
      amount: amount_max,
      pending_id: 6n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: TransferFlags.post_pending_transfer,
      timestamp: 0n,
    };

    transfer_errors = await client.createTransfers([transfer1]);
    // Error handling omitted.
    // endsection:transfer-flags-post
  } catch (exception) {}

  try {
    // section:transfer-flags-void
    const transfer0 = {
      id: 8n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: TransferFlags.pending,
      timestamp: 0n,
    };

    let transfer_errors = await client.createTransfers([transfer0]);
    // Error handling omitted.

    const transfer1 = {
      id: 9n,
      debit_account_id: 102n,
      credit_account_id: 103n,
      amount: 0n,
      pending_id: 8n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 720,
      flags: TransferFlags.void_pending_transfer,
      timestamp: 0n,
    };

    transfer_errors = await client.createTransfers([transfer1]);
    // Error handling omitted.
    // endsection:transfer-flags-void
  } catch (exception) {}

  try {
    // section:lookup-transfers
    const transfers = await client.lookupTransfers([1n, 2n]);
    // endsection:lookup-transfers
  } catch (exception) {}

  try {
    // section:get-account-transfers
    const filter = {
      account_id: 2n,
      user_data_128: 0n, // No filter by UserData.
      user_data_64: 0n,
      user_data_32: 0,
      code: 0, // No filter by Code.
      timestamp_min: 0n, // No filter by Timestamp.
      timestamp_max: 0n, // No filter by Timestamp.
      limit: 10, // Limit to ten transfers at most.
      flags: AccountFilterFlags.debits | // Include transfer from the debit side.
        AccountFilterFlags.credits | // Include transfer from the credit side.
        AccountFilterFlags.reversed, // Sort by timestamp in reverse-chronological order.
    };

    const account_transfers = await client.getAccountTransfers(filter);
    // endsection:get-account-transfers
  } catch (exception) {}

  try {
    // section:get-account-balances
    const filter = {
      account_id: 2n,
      user_data_128: 0n, // No filter by UserData.
      user_data_64: 0n,
      user_data_32: 0,
      code: 0, // No filter by Code.
      timestamp_min: 0n, // No filter by Timestamp.
      timestamp_max: 0n, // No filter by Timestamp.
      limit: 10, // Limit to ten balances at most.
      flags: AccountFilterFlags.debits | // Include transfer from the debit side.
        AccountFilterFlags.credits | // Include transfer from the credit side.
        AccountFilterFlags.reversed, // Sort by timestamp in reverse-chronological order.
    };

    const account_balances = await client.getAccountBalances(filter);
    // endsection:get-account-balances
  } catch (exception) {}

  try {
    // section:query-accounts
    const query_filter = {
      user_data_128: 1000n, // Filter by UserData.
      user_data_64: 100n,
      user_data_32: 10,
      code: 1, // Filter by Code.
      ledger: 0, // No filter by Ledger.
      timestamp_min: 0n, // No filter by Timestamp.
      timestamp_max: 0n, // No filter by Timestamp.
      limit: 10, // Limit to ten accounts at most.
      flags: QueryFilterFlags.reversed, // Sort by timestamp in reverse-chronological order.
    };

    const query_accounts = await client.queryAccounts(query_filter);
    // endsection:query-accounts
  } catch (exception) {}

  try {
    // section:query-transfers
    const query_filter = {
      user_data_128: 1000n, // Filter by UserData.
      user_data_64: 100n,
      user_data_32: 10,
      code: 1, // Filter by Code.
      ledger: 0, // No filter by Ledger.
      timestamp_min: 0n, // No filter by Timestamp.
      timestamp_max: 0n, // No filter by Timestamp.
      limit: 10, // Limit to ten transfers at most.
      flags: QueryFilterFlags.reversed, // Sort by timestamp in reverse-chronological order.
    };

    const query_transfers = await client.queryTransfers(query_filter);
    // endsection:query-transfers
  } catch (exception) {}

  try {
      // section:linked-events
      const batch = []; // Array of transfer to create.
      let linkedFlag = 0;
      linkedFlag |= TransferFlags.linked;

      // An individual transfer (successful):
      batch.push({ id: 1n /* , ... */ });

      // A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
      batch.push({ id: 2n, /* ..., */ flags: linkedFlag }); // Commit/rollback.
      batch.push({ id: 3n, /* ..., */ flags: linkedFlag }); // Commit/rollback.
      batch.push({ id: 2n, /* ..., */ flags: linkedFlag }); // Fail with exists
      batch.push({ id: 4n, /* ..., */ flags: 0 }); // Fail without committing.

      // An individual transfer (successful):
      // This should not see any effect from the failed chain above.
      batch.push({ id: 2n, /* ..., */ flags: 0 });

      // A chain of 2 transfers (the first transfer fails the chain):
      batch.push({ id: 2n, /* ..., */ flags: linkedFlag });
      batch.push({ id: 3n, /* ..., */ flags: 0 });

      // A chain of 2 transfers (successful):
      batch.push({ id: 3n, /* ..., */ flags: linkedFlag });
      batch.push({ id: 4n, /* ..., */ flags: 0 });

      const transfer_errors = await client.createTransfers(batch);
      // Error handling omitted.
      // endsection:linked-events
    } catch (exception) {}

    try {
      // section:imported-events
      // External source of time.
      let historical_timestamp = 0n
      // Events loaded from an external source.
      const historical_accounts = []; // Loaded from an external source.
      const historical_transfers = []; // Loaded from an external source.

      // First, load and import all accounts with their timestamps from the historical source.
      const accounts = [];
      for (let index = 0; i < historical_accounts.length; i++) {
        let account = historical_accounts[i];
        // Set a unique and strictly increasing timestamp.
        historical_timestamp += 1;
        account.timestamp = historical_timestamp;
        // Set the account as `imported`.
        account.flags = AccountFlags.imported;
        // To ensure atomicity, the entire batch (except the last event in the chain)
        // must be `linked`.
        if (index < historical_accounts.length - 1) {
          account.flags |= AccountFlags.linked;
        }

        accounts.push(account);
      }

      const account_errors = await client.createAccounts(accounts);
      // Error handling omitted.

      // Then, load and import all transfers with their timestamps from the historical source.
      const transfers = [];
      for (let index = 0; i < historical_transfers.length; i++) {
        let transfer = historical_transfers[i];
        // Set a unique and strictly increasing timestamp.
        historical_timestamp += 1;
        transfer.timestamp = historical_timestamp;
        // Set the account as `imported`.
        transfer.flags = TransferFlags.imported;
        // To ensure atomicity, the entire batch (except the last event in the chain)
        // must be `linked`.
        if (index < historical_transfers.length - 1) {
          transfer.flags |= TransferFlags.linked;
        }

        transfers.push(transfer);
      }

      const transfer_errors = await client.createTransfers(transfers);
      // Error handling omitted.

      // Since it is a linked chain, in case of any error the entire batch is rolled back and can be retried
      // with the same historical timestamps without regressing the cluster timestamp.
      // endsection:imported-events
    } catch (exception) {}
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
