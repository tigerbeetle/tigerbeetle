const assert = require("assert");

const {
    createClient,
    CreateAccountError,
    CreateTransferError,
    TransferFlags,
} = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0n,
  replica_addresses: [process.env.TB_ADDRESS || '3000'],
});

async function main() {
  // Create two accounts.
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
  let transferErrors = await client.createTransfers(transfers);
  for (const error of transferErrors) {
    console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`);
  }
  assert.equal(transferErrors.length, 0);

  // Validate accounts pending and posted debits/credits before
  // finishing the two-phase transfer.
  let accounts = await client.lookupAccounts([1n, 2n]);
  assert.equal(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 1500);
      assert.equal(account.credits_pending, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 1500);
    } else {
      assert.fail("Unexpected account: " + JSON.stringify(account, null, 2));
    }
  }

  // Create a 6th transfer posting the 1st transfer.
  transferErrors = await client.createTransfers([
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
  for (const error of transferErrors) {
    console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`);
  }
  assert.equal(transferErrors.length, 0);

  // Validate account balances after posting 1st pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.equal(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 100);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 1400);
      assert.equal(account.credits_pending, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 100);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 1400);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }

  // Create a 7th transfer voiding the 2nd transfer.
  transferErrors = await client.createTransfers([
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
  for (const error of transferErrors) {
    console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`);
  }
  assert.equal(transferErrors.length, 0);

  // Validate account balances after voiding 2nd pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.equal(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 100);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 1200);
      assert.equal(account.credits_pending, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 100);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 1200);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }

  // Create a 8th transfer posting the 3rd transfer.
  transferErrors = await client.createTransfers([
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
  for (const error of transferErrors) {
    console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`);
  }
  assert.equal(transferErrors.length, 0);

  // Validate account balances after posting 3rd pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.equal(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 400);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 900);
      assert.equal(account.credits_pending, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 400);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 900);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }

  // Create a 9th transfer voiding the 4th transfer.
  transferErrors = await client.createTransfers([
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
  for (const error of transferErrors) {
    console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`);
  }
  assert.equal(transferErrors.length, 0);

  // Validate account balances after voiding 4th pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.equal(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 400);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 500);
      assert.equal(account.credits_pending, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 400);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 500);
    } else {
      assert.fail("Unexpected account: " + account.id);
    }
  }

  // Create a109th transfer posting the 5th transfer.
  transferErrors = await client.createTransfers([
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
  for (const error of transferErrors) {
    console.error(`Batch transfer at ${error.index} failed to create: ${CreateTransferError[error.result]}.`);
  }
  assert.equal(transferErrors.length, 0);

  // Validate account balances after posting 5th pending transfer.
  accounts = await client.lookupAccounts([1n, 2n]);
  assert.equal(accounts.length, 2);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 900);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 900);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 0);
    } else {
      assert.fail("Unexpected account: " + account.id);
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
