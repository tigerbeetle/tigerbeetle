const assert = require("assert");
const process = require("process");

const {
    createClient,
    id,
    CreateAccountStatus,
    CreateTransferStatus,
    TransferFlags,
} = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0n,
  replica_addresses: process.env.TB_ADDRESS || '3000',
});

async function main() {
  // Create accounts.
  const accountDrId = id();
  const accountCrId = id();

  let accountResults = await client.createAccounts([
    {
      id: accountDrId,
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
      id: accountCrId,
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
  assert.strictEqual(accountResults.length, 2);
  assert.strictEqual(accountResults[0].status, CreateAccountStatus.created);
  assert.strictEqual(accountResults[1].status, CreateAccountStatus.created);

  // Create pending transfer.
  const transferPendingId = id();
  let transferResults = await client.createTransfers([
    {
      id: transferPendingId,
      debit_account_id: accountDrId,
      credit_account_id: accountCrId,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      amount: 500n,
      flags: TransferFlags.pending,
      timestamp: 0n,
    },
  ]);
  assert.strictEqual(transferResults.length, 1);
  assert.strictEqual(transferResults[0].status, CreateTransferStatus.created);

  // Fetch and validate pending account balances.
  let accounts = await client.lookupAccounts([accountDrId, accountCrId]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === accountDrId) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 500n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === accountCrId) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 500n);
    } else {
      assert.fail("Unexpected account: " + JSON.stringify(account, null, 2));
    }
  }

  // Post pending transfer.
  transferResults = await client.createTransfers([
    {
      id: id(),
      debit_account_id: accountDrId,
      credit_account_id: accountCrId,
      pending_id: transferPendingId,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      // You can post any amount between zero and the pending transfer amount,
      // or void the two-phase transfer using the `void_pending_transfer` flag.
      amount: 500n,
      flags: TransferFlags.post_pending_transfer,
      timestamp: 0n,
    },
  ]);
  assert.strictEqual(transferResults.length, 1);
  assert.strictEqual(transferResults[0].status, CreateTransferStatus.created);

  // Fetch and validate final account balances.
  accounts = await client.lookupAccounts([accountDrId, accountCrId]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === accountDrId) {
      assert.strictEqual(account.debits_posted, 500n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === accountCrId) {
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
