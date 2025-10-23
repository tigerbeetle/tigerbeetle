const assert = require("assert");
const process = require("process");

const {
    createClient,
    CreateAccountStatus,
    CreateTransferStatus,
    TransferFlags,
} = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0n,
  replica_addresses: [process.env.TB_ADDRESS || '3000'],
});

async function main() {
  // Create two accounts.
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
  assert.strictEqual(accountsResults[0].status, CreateAccountStatus.created);
  assert.strictEqual(accountsResults[1].status, CreateAccountStatus.created);

  // Start five pending transfer.
  let transfers = [
    {
      id: 1n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 100n,
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
    {
      id: 2n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 200n,
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
    {
      id: 3n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 300n,
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
    {
      id: 4n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 400n,
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
    {
      id: 5n,
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
  ];
  let transfersResults = await client.createTransfers(transfers);
  assert.strictEqual(transfersResults.length, transfers.length);
  for (const result of transfersResults) {
    assert.strictEqual(result.status, CreateTransferStatus.created);
  }

  // Validate accounts pending and posted debits/credits before
  // finishing the two-phase transfer.
  let accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 1500n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 1500n);
    } else {
      assert.fail("Unexpected account: " + JSON.stringify(account, null, 2));
    }
  }

  // Create a 6th transfer posting the 1st transfer.
  transfersResults = await client.createTransfers([
    {
      id: 6n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 100n,
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
  assert.strictEqual(transfersResults[0].status, CreateTransferStatus.created);

  // Validate account balances after posting 1st pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 100n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 1400n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 100n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 1400n);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }

  // Create a 7th transfer voiding the 2nd transfer.
  transfersResults = await client.createTransfers([
    {
      id: 7n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 200n,
      pending_id: 2n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: TransferFlags.void_pending_transfer,
      timestamp: 0n,
    },
  ]);
  assert.strictEqual(transfersResults.length, 1);
  assert.strictEqual(transfersResults[0].status, CreateTransferStatus.created);

  // Validate account balances after voiding 2nd pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 100n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 1200n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 100n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 1200n);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }

  // Create a 8th transfer posting the 3rd transfer.
  transfersResults = await client.createTransfers([
    {
      id: 8n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 300n,
      pending_id: 3n,
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
  assert.strictEqual(transfersResults[0].status, CreateTransferStatus.created);

  // Validate account balances after posting 3rd pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 400n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 900n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 400n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 900n);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }

  // Create a 9th transfer voiding the 4th transfer.
  transfersResults = await client.createTransfers([
    {
      id: 9n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 400n,
      pending_id: 4n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: TransferFlags.void_pending_transfer,
      timestamp: 0n,
    },
  ]);
  assert.strictEqual(transfersResults.length, 1);
  assert.strictEqual(transfersResults[0].status, CreateTransferStatus.created);

  // Validate account balances after voiding 4th pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 400n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 500n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 400n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 500n);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }

  // Create a 10th transfer posting the 5th transfer.
  transfersResults = await client.createTransfers([
    {
      id: 10n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      amount: 500n,
      pending_id: 5n,
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
  assert.strictEqual(transfersResults[0].status, CreateTransferStatus.created);

  // Validate account balances after posting 5th pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.strictEqual(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.strictEqual(account.debits_posted, 900n);
      assert.strictEqual(account.credits_posted, 0n);
      assert.strictEqual(account.debits_pending, 0n);
      assert.strictEqual(account.credits_pending, 0n);
    } else if (account.id === 2n) {
      assert.strictEqual(account.debits_posted, 0n);
      assert.strictEqual(account.credits_posted, 900n);
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
