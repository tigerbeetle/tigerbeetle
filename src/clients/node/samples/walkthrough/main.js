// section:imports
const { createClient } = require("tigerbeetle-node");

console.log("Import ok!");
// endsection:imports

const {
  AccountFlags,
  TransferFlags,
  CreateTransferError,
  CreateAccountError,
  GetAccountTransfersFlags,
} = require("tigerbeetle-node");

async function main() {
  // section:client
  const client = createClient({
    cluster_id: 0n,
    replica_addresses: [process.env.TB_ADDRESS || "3000"],
  });
  // endsection:client

  // section:create-accounts
  let account = {
    id: 137n,
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

  let accountErrors = await client.createAccounts([account]);
  // endsection:create-accounts

  // section:account-flags
  let account0 = {
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
    flags: 0,
  };
  let account1 = {
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
    flags: 0,
  };
  account0.flags = AccountFlags.linked |
    AccountFlags.debits_must_not_exceed_credits;
  accountErrors = await client.createAccounts([account0, account1]);
  // endsection:account-flags

  // section:create-accounts-errors
  let account2 = {
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
  let account3 = {
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
  let account4 = {
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
  accountErrors = await client.createAccounts([account2, account3, account4]);
  for (const error of accountErrors) {
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

  // section:lookup-accounts
  const accounts = await client.lookupAccounts([137n, 138n]);
  console.log(accounts);
  /*
   * [{
   *   id: 137n,
   *   debits_pending: 0n,
   *   debits_posted: 0n,
   *   credits_pending: 0n,
   *   credits_posted: 0n,
   *   user_data_128: 0n,
   *   user_data_64: 0n,
   *   user_data_32: 0,
   *   reserved: 0,
   *   ledger: 1,
   *   code: 718,
   *   flags: 0,
   *   timestamp: 1623062009212508993n,
   * }]
   */
  // endsection:lookup-accounts

  // section:create-transfers
  let transfers = [{
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
  }];
  let transferErrors = await client.createTransfers(transfers);
  // endsection:create-transfers

  // section:create-transfers-errors
  for (const error of transferErrors) {
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

  // section:no-batch
  for (let i = 0; i < transfers.len; i++) {
    const transferErrors = await client.createTransfers(transfers[i]);
    // error handling omitted
  }
  // endsection:no-batch

  // section:batch
  const BATCH_SIZE = 8190;
  for (let i = 0; i < transfers.length; i += BATCH_SIZE) {
    const transferErrors = await client.createTransfers(
      transfers.slice(i, Math.min(transfers.length, BATCH_SIZE)),
    );
    // error handling omitted
  }
  // endsection:batch

  // section:transfer-flags-link
  let transfer0 = {
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
  };
  let transfer1 = {
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
  };
  transfer0.flags = TransferFlags.linked;
  // Create the transfer
  transferErrors = await client.createTransfers([transfer0, transfer1]);
  // endsection:transfer-flags-link

  // section:transfer-flags-post
  let transfer2 = {
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
    flags: TransferFlags.pending,
    timestamp: 0n,
  };
  transferErrors = await client.createTransfers([transfer2]);

  let transfer3 = {
    id: 5n,
    debit_account_id: 102n,
    credit_account_id: 103n,
    amount: 10n,
    pending_id: 4n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    timeout: 0,
    ledger: 1,
    code: 720,
    flags: TransferFlags.post_pending_transfer,
    timestamp: 0n,
  };
  transferErrors = await client.createTransfers([transfer3]);
  // endsection:transfer-flags-post

  // section:transfer-flags-void
  let transfer4 = {
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
    flags: TransferFlags.pending,
    timestamp: 0n,
  };
  transferErrors = await client.createTransfers([transfer4]);

  let transfer5 = {
    id: 7n,
    debit_account_id: 102n,
    credit_account_id: 103n,
    amount: 10n,
    pending_id: 6n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    timeout: 0,
    ledger: 1,
    code: 720,
    flags: TransferFlags.void_pending_transfer,
    timestamp: 0n,
  };
  transferErrors = await client.createTransfers([transfer5]);
  // endsection:transfer-flags-void

  // section:lookup-transfers
  transfers = await client.lookupTransfers([1n, 2n]);
  console.log(transfers);
  /*
   * [{
   *   id: 1n,
   *   debit_account_id: 102n,
   *   credit_account_id: 103n,
   *   amount: 10n,
   *   pending_id: 0n,
   *   user_data_128: 0n,
   *   user_data_64: 0n,
   *   user_data_32: 0,
   *   timeout: 0,
   *   ledger: 1,
   *   code: 720,
   *   flags: 0,
   *   timestamp: 1623062009212508993n,
   * }]
   */
  // endsection:lookup-transfers

  // section:get-account-transfers
  let filter = {
    account_id: 2n,
    timestamp: 0n, // No filter by Timestamp.
    limit: 10, // Limit to ten transfers at most.
    flags: GetAccountTransfersFlags.debits | // Include transfer from the debit side.
      GetAccountTransfersFlags.credits | // Include transfer from the credit side.
      GetAccountTransfersFlags.reversed, // Sort by timestamp in reverse-chronological order.
  };
  const account_transfers = await client.getAccountTransfers(filter);
  // endsection:get-account-transfers

    try {
        // section:linked-events
        const batch = [];
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

        const errors = await client.createTransfers(batch);

        /**
         * console.log(errors);
         * [
         *  { index: 1, error: 1 },  // linked_event_failed
         *  { index: 2, error: 1 },  // linked_event_failed
         *  { index: 3, error: 25 }, // exists
         *  { index: 4, error: 1 },  // linked_event_failed
         *
         *  { index: 6, error: 17 }, // exists_with_different_flags
         *  { index: 7, error: 1 },  // linked_event_failed
         * ]
         */
        // endsection:linked-events
    } catch (exception) {
        // This currently throws because the batch is actually invalid (most of fields are
        // undefined). Ideally, we prepare a correct batch here while keeping the syntax compact,
        // for the example, but for the time being lets prioritize a readable example and just
        // swallow the error.
    }
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
