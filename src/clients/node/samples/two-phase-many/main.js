const assert = require("assert");

const {
    createClient,
    CreateAccountError,
    CreateTransferError,
    TransferFlags,
} = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0,
  replica_addresses: [process.env.TB_ADDRESS || '3000'],
});

async function main() {
  // Create two accounts.
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
  for (const error of accountErrors) {
    console.error(`Batch account at ${error.index} failed to create: ${CreateAccountError[error.result]}.`);
  }
  assert.equal(accountErrors.length, 0);

  // Start five pending transfer.
  let transfers = [
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
      flags: TransferFlags.pending,
      timestamp: 0n,
      amount: 100n,
    },
    {
      id: 2n,
      pending_id: 0n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.pending,
      timestamp: 0n,
      amount: 200n,
    },
    {
      id: 3n,
      pending_id: 0n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.pending,
      timestamp: 0n,
      amount: 300n,
    },
    {
      id: 4n,
      pending_id: 0n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.pending,
      timestamp: 0n,
      amount: 400n,
    },
    {
      id: 5n,
      pending_id: 0n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.pending,
      timestamp: 0n,
      amount: 500n,
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
      pending_id: 1n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.post_pending_transfer,
      timestamp: 0n,
      amount: 100n,
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
      pending_id: 2n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.void_pending_transfer,
      timestamp: 0n,
      amount: 200n,
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
      pending_id: 3n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.post_pending_transfer,
      timestamp: 0n,
      amount: 300n,
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
      pending_id: 4n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.void_pending_transfer,
      timestamp: 0n,
      amount: 400n,
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
      pending_id: 5n,
      debit_account_id: 1n,
      credit_account_id: 2n,
      user_data: 0n,
      reserved: 0n,
      timeout: 0n,
      ledger: 1,
      code: 1,
      flags: TransferFlags.post_pending_transfer,
      timestamp: 0n,
      amount: 500n,
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
