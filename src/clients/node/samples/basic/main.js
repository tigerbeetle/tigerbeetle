const assert = require("assert");
const process = require("process");

const {
    createClient,
    CreateAccountResult,
    CreateTransferResult,
} = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0n,
  replica_addresses: [process.env.TB_ADDRESS || '3000'],
});

async function main() {
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

  let transfersResults = await client.createTransfers([
    {
      id: 1n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 10n,
      pending_id: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: 0,
      timestamp: 0n,
    },
  ]);
  assert.strictEqual(transfersResults.length, 1);
  assert.strictEqual(transfersResults[0].result, CreateTransferResult.ok);

  let accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 10n);
      assert.strictEqual(account.credits_posted, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 10n);
    } else {
      assert.fail("Unexpected account: " + JSON.stringify(account, null, 2));
    }
  }
}

main().then(() => {
  process.exit(0);
}).catch((e) => {
  console.error(e);
  process.exit(1);
});
