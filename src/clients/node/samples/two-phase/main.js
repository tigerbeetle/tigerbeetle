const assert = require("assert");

const { createClient, TransferFlags } = require("tigerbeetle-node");

const client = createClient({
  cluster_id: 0,
  replica_addresses: [process.env.TB_PORT || '3000'],
});

async function main() {
  // Create two accounts
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

  // Start a pending transfer
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
      flags: TransferFlags.pending,
      timestamp: 0n,
      amount: 500n,
    },
  ]);
  assert.equal(transferErrors.length, 0);

  // Validate accounts pending and posted debits/credits before finishing the two-phase transfer
  let accounts = await client.lookupAccounts([1n, 2n]);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 500);
      assert.equal(account.credits_pending, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 500);
    } else {
      assert.fail("Unexpected account: " + JSON.stringify(account, null, 2));
    }
  }

  // Create a second transfer simply posting the first transfer
  transferErrors = await client.createTransfers([
    {
      id: 2n,
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
      amount: 500n,
    },
  ]);
  assert.equal(transferErrors.length, 0);

  // Validate the contents of all transfers
  let transfers = await client.lookupTransfers([1n, 2n]);
  for (let transfer of transfers) {
    if (transfer.id === 1n) {
      assert.equal(transfer.flags, TransferFlags.pending);
    } else if (transfer.id === 2n) {
      assert.equal(transfer.flags, TransferFlags.post_pending_transfer);
    } else {
      assert.fail("Unexpected transfer: " + transfer.id);
    }
  }

  // Validate accounts pending and posted debits/credits after finishing the two-phase transfer
  accounts = await client.lookupAccounts([1n, 2n]);
  for (let account of accounts) {
    if (account.id === 1n) {
      assert.equal(account.debits_posted, 500);
      assert.equal(account.credits_posted, 0);
      assert.equal(account.debits_pending, 0);
      assert.equal(account.credits_pending, 0);
    } else if (account.id === 2n) {
      assert.equal(account.debits_posted, 0);
      assert.equal(account.credits_posted, 500);
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
