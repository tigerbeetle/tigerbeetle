const assert = require("assert");

const {
    createClient,
    CreateAccountError,
    CreateTransferError,
} = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0n,
  replica_addresses: [process.env.TB_ADDRESS || '3000'],
});

async function main() {
  let accountErrors = await client.createAccounts([
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
  for (const error of accountErrors) {
    console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.result]}.`);
  }
  assert.equal(accountErrors.length, 0);

  let transferErrors = await client.createTransfers([
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
  for (const error of transferErrors) {
    console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`);
  }
  assert.equal(transferErrors.length, 0);

  let accounts = await client.lookupAccounts([1n, 2n]);
  assert.equal(accounts.length, 2);
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
