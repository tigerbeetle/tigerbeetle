const assert = require("assert");
const process = require("process");

const {
    createClient,
    CreateAccountResult,
    CreateTransferResult,
    TransferFlags,
} = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0n,
  replica_addresses: [process.env.TB_ADDRESS || '3000'],
});

async function main() {
  // Create two accounts
  let accountsResults = await client.createAccounts([
    {
      id: 1n,
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
      flags: 0,
      timestamp: 0n,
    },
    {
      id: 2n,
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
      flags: 0,
      timestamp: 0n,
    },
  ]);
  assert.strictEqual(accountsResults.length, 2);
  assert.strictEqual(accountsResults[0].result, CreateAccountResult.ok);
  assert.strictEqual(accountsResults[1].result, CreateAccountResult.ok);

  // Start a pending transfer
  let transfersResults = await client.createTransfers([
    {
      id: 1n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 500n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: TransferFlags.pending,
      timestamp: 0n,
    },
  ]);
  assert.strictEqual(transfersResults.length, 1);
  assert.strictEqual(transfersResults[0].result, CreateTransferResult.ok);

  // Validate accounts pending and posted debits/credits before finishing the two-phase transfer
  let accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 500n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 500n);
    } else {
      assert.fail("Unexpected account: " + JSON.stringify(account, null, 2));
    }
  }

  // Create a second transfer simply posting the first transfer
  transfersResults = await client.createTransfers([
    {
      id: 2n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 500n,
      pending_id: 1n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: TransferFlags.post_pending_transfer,
      timestamp: 0n,
    },
  ]);
  assert.strictEqual(transfersResults.length, 1);
  assert.strictEqual(transfersResults[0].result, CreateTransferResult.ok);

  // Validate the contents of all transfers
  let transfers = await client.lookupTransfers([1n, 2n]);
  assert.strictEqual(transfers.length, 2);
  for (let transfer of transfers) {
    if (transfer.id === 1n) {
      assert.strictEqual(transfer.flags & TransferFlags.pending, TransferFlags.pending);
    } else if (transfer.id === 2n) {
      assert.strictEqual(transfer.flags & TransferFlags.post_pending_transfer, TransferFlags.post_pending_transfer);
    } else {
      assert.fail("Unexpected transfer: " + transfer.id);
    }
  }

  // Validate accounts pending and posted debits/credits after finishing the two-phase transfer
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 500n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 500n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 0n);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }
}

main().then(() => {
  process.exit(0);
}).catch((e) => {
  console.error(e);
  process.exit(1);
});
