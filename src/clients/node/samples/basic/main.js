const assert = require("assert");

const { createClient } = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0,
  replica_addresses: [process.env.TB_PORT || '3000'],
});

async function main() {
  let accountErrors = await client.createAccounts([
    {
      id: 1n,
      user_data: 0n,
      reserved: Buffer.alloc(48, 0),
      ledger: 1,
      code: 1,
      flags: 0,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      timestamp: 0n,
    },
    {
      id: 2n,
      user_data: 0n,
      reserved: Buffer.alloc(48, 0),
      ledger: 1,
      code: 1,
      flags: 0,
      debits_pending: 0n,
      debits_posted: 0n,
      credits_pending: 0n,
      credits_posted: 0n,
      timestamp: 0n,
    },
  ]);
  assert.equal(accountErrors.length, 0);

  let transferErrors = await client.createTransfers([
    {
      id: 1n,
      pending_id: 0n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: 0,
      timestamp: 0n,
      amount: 10n,
    },
  ]);
  assert.equal(transferErrors.length, 0);

  let accounts = await client.lookupAccounts([1n, 2n]);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 10n);
      assert.equal(account.credits_posted, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 10n);
    } else {
      assert.fail("Unexpected account: " + JSON.stringify(account, null, 2));
    }
  }

  console.log('ok');
}

main().then(() => {
  process.exit(0);
}).catch((e) => {
  console.error(e);
  process.exit(1);
});
