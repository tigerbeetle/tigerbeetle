import assert, { AssertionError } from 'assert'
import {
  createClient,
  Account,
  Transfer,
  TransferFlags,
  CreateAccountResult,
  CreateTransferResult,
  AccountFilter,
  AccountFilterFlags,
  AccountFlags,
  amount_max,
  id,
  QueryFilter,
  QueryFilterFlags,
} from '.'

const client = createClient({
  cluster_id: 0n,
  replica_addresses: [process.env.TB_ADDRESS || '3000']
})

// Test data
const accountA: Account = {
  id: 17n,
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
  timestamp: 0n // this will be set correctly by the TigerBeetle server
}
const accountB: Account = {
  id: 19n,
  debits_pending: 0n,
  debits_posted: 0n,
  credits_pending: 0n,
  credits_posted: 0n,
  user_data_128: 0n,
  user_data_64: 0n,
  user_data_32: 0,
  reserved: 0,
  ledger: 1,
  code: 719,
  flags: 0,
  timestamp: 0n // this will be set correctly by the TigerBeetle server
}

const BATCH_MAX = 8189;

const tests: Array<{ name: string, fn: () => Promise<void> }> = []
function test(name: string, fn: () => Promise<void>) {
  tests.push({ name, fn })
}
test.skip = (name: string, fn: () => Promise<void>) => {
  console.log(name + ': SKIPPED')
}

test('id() monotonically increasing', async (): Promise<void> => {
  let idA = id();
  for (let i = 0; i < 10_000_000; i++) {
    // Ensure ID is monotonic between milliseconds if the loop executes too fast.
    if (i % 10_000 == 0) {
      await new Promise(resolve => setTimeout(resolve, 1))
    }

    const idB = id();
    assert.ok(idB > idA, 'id() returned an id that did not monotonically increase');
    idA = idB;
  }
})

test('range check `code` on Account to be u16', async (): Promise<void> => {
  const account = { ...accountA, id: 0n }

  account.code = 65535 + 1
  const codeError = await client.createAccounts([account]).catch(error => error)
  assert.strictEqual(codeError.message, 'code must be a u16.')

  const accounts = await client.lookupAccounts([account.id])
  assert.deepStrictEqual(accounts, [])
})

test('can create accounts', async (): Promise<void> => {
  const accounts_results = await client.createAccounts([accountA])
  assert.deepStrictEqual(accounts_results.length, 1)
  assert.ok(accounts_results[0].timestamp > 0)
  assert.deepStrictEqual(accounts_results[0].result, CreateAccountResult.ok)
})

test('can return error on account', async (): Promise<void> => {
  const accounts_results = await client.createAccounts([accountA, accountB])
  assert.deepStrictEqual(accounts_results.length, 2)
  assert.ok(accounts_results[0].timestamp > 0)
  assert.deepStrictEqual(accounts_results[0].result, CreateAccountResult.exists)
  assert.ok(accounts_results[1].timestamp > 0)
  assert.deepStrictEqual(accounts_results[1].result, CreateAccountResult.ok)
})

test('error if timestamp is not set to 0n on account', async (): Promise<void> => {
  const account = { ...accountA, timestamp: 2n, id: 3n }
  const accounts_results = await client.createAccounts([account])
  assert.deepStrictEqual(accounts_results.length, 1)
  assert.ok(accounts_results[0].timestamp > 0)
  assert.deepStrictEqual(accounts_results[0].result, CreateAccountResult.timestamp_must_be_zero)
})

test('batch max size', async (): Promise<void> => {
  const BATCH_SIZE = 10_000;
  var transfers: Transfer[] = [];
  for (var i=0; i<BATCH_SIZE;i++) {
    transfers.push({
      id: 0n,
      debit_account_id: 0n,
      credit_account_id: 0n,
      amount: 0n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      pending_id: 0n,
      timeout: 0,
      ledger: 0,
      code: 0,
      flags: 0,
      timestamp: 0n,
    });
  }

  try {
    const results = await client.createTransfers(transfers);
    assert.fail();
  } catch (error) {
    assert.strictEqual(error.message, "Too much data provided on this batch.");
  }
})

test('can lookup accounts', async (): Promise<void> => {
  const accounts = await client.lookupAccounts([accountA.id, accountB.id])

  assert.strictEqual(accounts.length, 2)
  const account1 = accounts[0]
  assert.strictEqual(account1.id, 17n)
  assert.strictEqual(account1.credits_posted, 0n)
  assert.strictEqual(account1.credits_pending, 0n)
  assert.strictEqual(account1.debits_posted, 0n)
  assert.strictEqual(account1.debits_pending, 0n)
  assert.strictEqual(account1.user_data_128, 0n)
  assert.strictEqual(account1.user_data_64, 0n)
  assert.strictEqual(account1.user_data_32, 0)
  assert.strictEqual(account1.code, 718)
  assert.strictEqual(account1.ledger, 1)
  assert.strictEqual(account1.flags, 0)
  assert.ok(account1.timestamp > 0n)

  const account2 = accounts[1]
  assert.strictEqual(account2.id, 19n)
  assert.strictEqual(account2.credits_posted, 0n)
  assert.strictEqual(account2.credits_pending, 0n)
  assert.strictEqual(account2.debits_posted, 0n)
  assert.strictEqual(account2.debits_pending, 0n)
  assert.strictEqual(account2.user_data_128, 0n)
  assert.strictEqual(account2.user_data_64, 0n)
  assert.strictEqual(account2.user_data_32, 0)
  assert.strictEqual(account2.code, 719)
  assert.strictEqual(account2.ledger, 1)
  assert.strictEqual(account2.flags, 0)
  assert.ok(account2.timestamp > 0n)
})

test('can create a transfer', async (): Promise<void> => {
  const transfer: Transfer = {
    id: 1n,
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    amount: 100n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 0n,
    timeout: 0,
    ledger: 1,
    code: 1,
    flags: 0,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const transfers_results = await client.createTransfers([transfer])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_posted, 100n)
  assert.strictEqual(accounts[0].credits_pending, 0n)
  assert.strictEqual(accounts[0].debits_posted, 0n)
  assert.strictEqual(accounts[0].debits_pending, 0n)

  assert.strictEqual(accounts[1].credits_posted, 0n)
  assert.strictEqual(accounts[1].credits_pending, 0n)
  assert.strictEqual(accounts[1].debits_posted, 100n)
  assert.strictEqual(accounts[1].debits_pending, 0n)
})

test('can create a two-phase transfer', async (): Promise<void> => {
  let flags = 0
  flags |= TransferFlags.pending
  const transfer: Transfer = {
    id: 2n,
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    amount: 50n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 0n,
    timeout: 2e9,
    ledger: 1,
    code: 1,
    flags,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const transfers_results = await client.createTransfers([transfer])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_posted, 100n)
  assert.strictEqual(accounts[0].credits_pending, 50n)
  assert.strictEqual(accounts[0].debits_posted, 0n)
  assert.strictEqual(accounts[0].debits_pending, 0n)

  assert.strictEqual(accounts[1].credits_posted, 0n)
  assert.strictEqual(accounts[1].credits_pending, 0n)
  assert.strictEqual(accounts[1].debits_posted, 100n)
  assert.strictEqual(accounts[1].debits_pending, 50n)

  // Lookup the transfer:
  const transfers = await client.lookupTransfers([transfer.id])
  assert.strictEqual(transfers.length, 1)
  assert.strictEqual(transfers[0].id, 2n)
  assert.strictEqual(transfers[0].debit_account_id, accountB.id)
  assert.strictEqual(transfers[0].credit_account_id, accountA.id)
  assert.strictEqual(transfers[0].amount, 50n)
  assert.strictEqual(transfers[0].user_data_128, 0n)
  assert.strictEqual(transfers[0].user_data_64, 0n)
  assert.strictEqual(transfers[0].user_data_32, 0)
  assert.strictEqual(transfers[0].timeout > 0, true)
  assert.strictEqual(transfers[0].code, 1)
  assert.strictEqual(transfers[0].flags, 2)
  assert.strictEqual(transfers[0].timestamp > 0, true)
})

test('can post a two-phase transfer', async (): Promise<void> => {
  let flags = 0
  flags |= TransferFlags.post_pending_transfer

  const commit: Transfer = {
    id: 3n,
    debit_account_id: BigInt(0),
    credit_account_id: BigInt(0),
    amount: amount_max,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 2n,// must match the id of the pending transfer
    timeout: 0,
    ledger: 1,
    code: 1,
    flags: flags,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const transfers_results = await client.createTransfers([commit])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_posted, 150n)
  assert.strictEqual(accounts[0].credits_pending, 0n)
  assert.strictEqual(accounts[0].debits_posted, 0n)
  assert.strictEqual(accounts[0].debits_pending, 0n)

  assert.strictEqual(accounts[1].credits_posted, 0n)
  assert.strictEqual(accounts[1].credits_pending, 0n)
  assert.strictEqual(accounts[1].debits_posted, 150n)
  assert.strictEqual(accounts[1].debits_pending, 0n)
})

test('can reject a two-phase transfer', async (): Promise<void> => {
  // Create a two-phase transfer:
  const transfer: Transfer = {
    id: 4n,
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    amount: 50n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 0n,
    timeout: 1e9,
    ledger: 1,
    code: 1,
    flags: TransferFlags.pending,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }
  let transfers_results = await client.createTransfers([transfer])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  // send in the reject
  const reject: Transfer = {
    id: 5n,
    debit_account_id: BigInt(0),
    credit_account_id: BigInt(0),
    amount: 0n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 4n, // must match the id of the pending transfer
    timeout: 0,
    ledger: 1,
    code: 1,
    flags: TransferFlags.void_pending_transfer,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  transfers_results = await client.createTransfers([reject])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_posted, 150n)
  assert.strictEqual(accounts[0].credits_pending, 0n)
  assert.strictEqual(accounts[0].debits_posted, 0n)
  assert.strictEqual(accounts[0].debits_pending, 0n)

  assert.strictEqual(accounts[1].credits_posted, 0n)
  assert.strictEqual(accounts[1].credits_pending, 0n)
  assert.strictEqual(accounts[1].debits_posted, 150n)
  assert.strictEqual(accounts[1].debits_pending, 0n)
})

test('can link transfers', async (): Promise<void> => {
  const transfer1: Transfer = {
    id: 6n,
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    amount: 100n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 0n,
    timeout: 0,
    ledger: 1,
    code: 1,
    flags: TransferFlags.linked, // points to transfer2
    timestamp: 0n, // will be set correctly by the TigerBeetle server
  }
  const transfer2: Transfer = {
    id: 6n,
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    amount: 100n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 0n,
    timeout: 0,
    ledger: 1,
    code: 1,
    // Does not have linked flag as it is the end of the chain.
    // This will also cause it to fail as this is now a duplicate with different flags
    flags: 0,
    timestamp: 0n, // will be set correctly by the TigerBeetle server
  }

  const transfers_results = await client.createTransfers([transfer1, transfer2])
  assert.deepStrictEqual(transfers_results.length, 2)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.linked_event_failed)
  assert.ok(transfers_results[1].timestamp > 0)
  assert.deepStrictEqual(transfers_results[1].result, CreateTransferResult.exists_with_different_flags)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_posted, 150n)
  assert.strictEqual(accounts[0].credits_pending, 0n)
  assert.strictEqual(accounts[0].debits_posted, 0n)
  assert.strictEqual(accounts[0].debits_pending, 0n)

  assert.strictEqual(accounts[1].credits_posted, 0n)
  assert.strictEqual(accounts[1].credits_pending, 0n)
  assert.strictEqual(accounts[1].debits_posted, 150n)
  assert.strictEqual(accounts[1].debits_pending, 0n)
})

test('cannot void an expired transfer', async (): Promise<void> => {
  // Create a two-phase transfer:
  const transfer: Transfer = {
    id: 6n,
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    amount: 50n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 0n,
    timeout: 1,
    ledger: 1,
    code: 1,
    flags: TransferFlags.pending,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }
  let transfers_results = await client.createTransfers([transfer])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  var accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_posted, 150n)
  assert.strictEqual(accounts[0].credits_pending, 50n)
  assert.strictEqual(accounts[0].debits_posted, 0n)
  assert.strictEqual(accounts[0].debits_pending, 0n)

  assert.strictEqual(accounts[1].credits_posted, 0n)
  assert.strictEqual(accounts[1].credits_pending, 0n)
  assert.strictEqual(accounts[1].debits_posted, 150n)
  assert.strictEqual(accounts[1].debits_pending, 50n)

  // We need to wait 1s for the server to expire the transfer, however the
  // server can pulse the expiry operation anytime after the timeout,
  // so adding an extra delay to avoid flaky tests.
  // TODO: Use `await setTimeout(1000)` when upgrade to Node.js > 15.
  const extra_wait_time = 250;
  await new Promise(_ => setTimeout(_, (transfer.timeout * 1000) + extra_wait_time));

  // Looking up the accounts again for the updated balance.
  accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_posted, 150n)
  assert.strictEqual(accounts[0].credits_pending, 0n)
  assert.strictEqual(accounts[0].debits_posted, 0n)
  assert.strictEqual(accounts[0].debits_pending, 0n)

  assert.strictEqual(accounts[1].credits_posted, 0n)
  assert.strictEqual(accounts[1].credits_pending, 0n)
  assert.strictEqual(accounts[1].debits_posted, 150n)
  assert.strictEqual(accounts[1].debits_pending, 0n)

  // send in the reject
  const reject: Transfer = {
    id: 7n,
    debit_account_id: BigInt(0),
    credit_account_id: BigInt(0),
    amount: 0n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 6n, // must match the id of the pending transfer
    timeout: 0,
    ledger: 1,
    code: 1,
    flags: TransferFlags.void_pending_transfer,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  transfers_results = await client.createTransfers([reject])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.pending_transfer_expired)
})

test('can close accounts', async (): Promise<void> => {
  const closing_transfer: Transfer = {
    id: id(),
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    amount: 0n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 0n,
    timeout: 0,
    ledger: 1,
    code: 1,
    flags: TransferFlags.closing_debit | TransferFlags.closing_credit | TransferFlags.pending,
    timestamp: 0n, // will be set correctly by the TigerBeetle server
  }
  let transfers_results = await client.createTransfers([closing_transfer])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  let accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.ok(accountA.flags != accounts[0].flags)
  assert.ok((accounts[0].flags & AccountFlags.closed) != 0)

  assert.ok(accountB.flags != accounts[1].flags)
  assert.ok((accounts[1].flags & AccountFlags.closed) != 0)

  const voiding_transfer: Transfer = {
    id: id(),
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    amount: 0n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    timeout: 0,
    ledger: 1,
    code: 1,
    flags: TransferFlags.void_pending_transfer,
    pending_id: closing_transfer.id,
    timestamp: 0n, // will be set correctly by the TigerBeetle server
  }

  transfers_results = await client.createTransfers([voiding_transfer])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accountA.flags, accounts[0].flags)
  assert.ok((accounts[0].flags & AccountFlags.closed) == 0)

  assert.strictEqual(accountB.flags, accounts[1].flags)
  assert.ok((accounts[1].flags & AccountFlags.closed) == 0)
})

test('can get account transfers', async (): Promise<void> => {
  const accountC: Account = {
    id: 21n,
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
    flags: AccountFlags.history,
    timestamp: 0n
  }
  const accounts_results = await client.createAccounts([accountC])
  assert.deepStrictEqual(accounts_results.length, 1)
  assert.ok(accounts_results[0].timestamp > 0)
  assert.deepStrictEqual(accounts_results[0].result, CreateAccountResult.ok)

  var transfers_created : Transfer[] = [];
  // Create transfers where the new account is either the debit or credit account:
  for (var i=0; i<10;i++) {
    transfers_created.push({
      id: BigInt(i + 10000),
      debit_account_id: i % 2 == 0 ? accountC.id : accountA.id,
      credit_account_id: i % 2 == 0 ? accountB.id : accountC.id,
      amount: 100n,
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      pending_id: 0n,
      timeout: 0,
      ledger: 1,
      code: 1,
      flags: 0,
      timestamp: 0n,
    });
  }

  const transfers_results = await client.createTransfers(transfers_created)
  assert.deepStrictEqual(transfers_results.length, transfers_created.length)
  for (const item of transfers_results) {
    assert.ok(item.timestamp > 0)
    assert.deepStrictEqual(item.result, CreateTransferResult.ok)
  }

  // Query all transfers for accountC:
  var filter: AccountFilter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: BATCH_MAX,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  var transfers = await client.getAccountTransfers(filter)
  var account_balances = await client.getAccountBalances(filter)
  assert.strictEqual(transfers.length, transfers_created.length)
  assert.strictEqual(account_balances.length, transfers.length)

  var timestamp = 0n;
  var i = 0;
  for (var transfer of transfers) {
    assert.ok(timestamp < transfer.timestamp);
    timestamp = transfer.timestamp;

    assert.ok(account_balances[i].timestamp == transfer.timestamp);
    i++;
  }

  // Query only the debit transfers for accountC, descending:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: BATCH_MAX,
    flags: AccountFilterFlags.debits |  AccountFilterFlags.reversed,
  }
  transfers = await client.getAccountTransfers(filter)
  account_balances = await client.getAccountBalances(filter)

  assert.strictEqual(transfers.length, transfers_created.length / 2)
  assert.strictEqual(account_balances.length, transfers.length)

  timestamp = 1n << 64n;
  i = 0;
  for (var transfer of transfers) {
    assert.ok(transfer.timestamp < timestamp);
    timestamp = transfer.timestamp;

    assert.ok(account_balances[i].timestamp == transfer.timestamp);
    i++;
  }

  // Query only the credit transfers for accountC, descending:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: BATCH_MAX,
    flags: AccountFilterFlags.credits |  AccountFilterFlags.reversed,
  }
  transfers = await client.getAccountTransfers(filter)
  account_balances = await client.getAccountBalances(filter)

  assert.strictEqual(transfers.length, transfers_created.length / 2)
  assert.strictEqual(account_balances.length, transfers.length)

  timestamp = 1n << 64n;
  i = 0;
  for (var transfer of transfers) {
    assert.ok(transfer.timestamp < timestamp);
    timestamp = transfer.timestamp;

    assert.ok(account_balances[i].timestamp == transfer.timestamp);
    i++;
  }

  // Query the first 5 transfers for accountC:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: transfers_created.length / 2,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  transfers = await client.getAccountTransfers(filter)
  account_balances = await client.getAccountBalances(filter)

  assert.strictEqual(transfers.length, transfers_created.length / 2)
  assert.strictEqual(account_balances.length, transfers.length)

  timestamp = 0n;
  i = 0;
  for (var transfer of transfers) {
    assert.ok(timestamp < transfer.timestamp);
    timestamp = transfer.timestamp;

    assert.ok(account_balances[i].timestamp == transfer.timestamp);
    i++;
  }

  // Query the next 5 transfers for accountC, with pagination:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: timestamp + 1n,
    timestamp_max: 0n,
    limit: transfers_created.length / 2,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  transfers = await client.getAccountTransfers(filter)
  account_balances = await client.getAccountBalances(filter)

  assert.strictEqual(transfers.length, transfers_created.length / 2)
  assert.strictEqual(account_balances.length, transfers.length)

  i = 0;
  for (var transfer of transfers) {
    assert.ok(timestamp < transfer.timestamp);
    timestamp = transfer.timestamp;

    assert.ok(account_balances[i].timestamp == transfer.timestamp);
    i++;
  }

  // Query again, no more transfers should be found:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: timestamp + 1n,
    timestamp_max: 0n,
    limit: transfers_created.length / 2,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  transfers = await client.getAccountTransfers(filter)
  account_balances = await client.getAccountBalances(filter)

  assert.deepStrictEqual(transfers, [])
  assert.strictEqual(account_balances.length, transfers.length)

  // Query the first 5 transfers for accountC ORDER BY DESC:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: transfers_created.length / 2,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits | AccountFilterFlags.reversed,
  }
  transfers = await client.getAccountTransfers(filter)
  account_balances = await client.getAccountBalances(filter)

  assert.strictEqual(transfers.length, transfers_created.length / 2)
  assert.strictEqual(account_balances.length, transfers.length)

  timestamp = 1n << 64n;
  i = 0;
  for (var transfer of transfers) {
    assert.ok(timestamp > transfer.timestamp);
    timestamp = transfer.timestamp;

    assert.ok(account_balances[i].timestamp == transfer.timestamp);
    i++;
  }

  // Query the next 5 transfers for accountC, with pagination:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: timestamp - 1n,
    limit: transfers_created.length / 2,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits | AccountFilterFlags.reversed,
  }
  transfers = await client.getAccountTransfers(filter)
  account_balances = await client.getAccountBalances(filter)

  assert.strictEqual(transfers.length, transfers_created.length / 2)
  assert.strictEqual(account_balances.length, transfers.length)

  i = 0;
  for (var transfer of transfers) {
    assert.ok(timestamp > transfer.timestamp);
    timestamp = transfer.timestamp;

    assert.ok(account_balances[i].timestamp == transfer.timestamp);
    i++;
  }

  // Query again, no more transfers should be found:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: timestamp - 1n,
    limit: transfers_created.length / 2,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits | AccountFilterFlags.reversed,
  }
  transfers = await client.getAccountTransfers(filter)
  account_balances = await client.getAccountBalances(filter)

  assert.deepStrictEqual(transfers, [])
  assert.strictEqual(account_balances.length, transfers.length)

  // Invalid account:
  filter = {
    account_id: 0n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: BATCH_MAX,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  assert.deepStrictEqual((await client.getAccountTransfers(filter)), [])
  assert.deepStrictEqual((await client.getAccountBalances(filter)), [])

  // Invalid timestamp min:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: (1n << 64n) - 1n, // ulong max value
    timestamp_max: 0n,
    limit: BATCH_MAX,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  assert.deepStrictEqual((await client.getAccountTransfers(filter)), [])
  assert.deepStrictEqual((await client.getAccountBalances(filter)), [])

  // Invalid timestamp max:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: (1n << 64n) - 1n, // ulong max value
    limit: BATCH_MAX,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  assert.deepStrictEqual((await client.getAccountTransfers(filter)), [])
  assert.deepStrictEqual((await client.getAccountBalances(filter)), [])

  // Invalid timestamp range:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: (1n << 64n) - 2n, // ulong max - 1
    timestamp_max: 1n,
    limit: BATCH_MAX,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  assert.deepStrictEqual((await client.getAccountTransfers(filter)), [])
  assert.deepStrictEqual((await client.getAccountBalances(filter)), [])

  // Zero limit:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: 0,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  assert.deepStrictEqual((await client.getAccountTransfers(filter)), [])
  assert.deepStrictEqual((await client.getAccountBalances(filter)), [])

  // Empty flags:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: BATCH_MAX,
    flags: AccountFilterFlags.none,
  }
  assert.deepStrictEqual((await client.getAccountTransfers(filter)), [])
  assert.deepStrictEqual((await client.getAccountBalances(filter)), [])

  // Invalid flags:
  filter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: BATCH_MAX,
    flags: 0xFFFF,
  }
  assert.deepStrictEqual((await client.getAccountTransfers(filter)), [])
  assert.deepStrictEqual((await client.getAccountBalances(filter)), [])

})

test('can query accounts', async (): Promise<void> => {
  {
    var accounts : Account[] = [];
    // Create transfers:
    for (var i=0; i<10;i++) {
      accounts.push({
        id: id(),
        debits_pending: 0n,
        debits_posted: 0n,
        credits_pending: 0n,
        credits_posted: 0n,
        user_data_128: i % 2 == 0 ? 1000n : 2000n,
        user_data_64: i % 2 == 0 ? 100n : 200n,
        user_data_32: i % 2 == 0 ? 10 : 20,
        ledger: 1,
        code: 999,
        flags: AccountFlags.none,
        reserved: 0,
        timestamp: 0n,
      })
    }

    const accounts_results = await client.createAccounts(accounts)
    assert.deepStrictEqual(accounts_results.length, accounts.length)
    for (const item of accounts_results) {
      assert.ok(item.timestamp > 0)
      assert.deepStrictEqual(item.result, CreateAccountResult.ok)
    }
  }

  {
    // Querying accounts where:
    // `user_data_128=1000 AND user_data_64=100 AND user_data_32=10
    // AND code=999 AND ledger=1 ORDER BY timestamp ASC`.
    var filter: QueryFilter = {
      user_data_128: 1000n,
      user_data_64: 100n,
      user_data_32: 10,
      ledger: 1,
      code: 999,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: BATCH_MAX,
      flags: QueryFilterFlags.none,
    }
    var query: Account[] = await client.queryAccounts(filter)
    assert.strictEqual(query.length, 5)

    var timestamp = 0n;
    for (var account of query) {
      assert.ok(timestamp < account.timestamp);
      timestamp = account.timestamp;

      assert.strictEqual(account.user_data_128, filter.user_data_128);
      assert.strictEqual(account.user_data_64, filter.user_data_64);
      assert.strictEqual(account.user_data_32, filter.user_data_32);
      assert.strictEqual(account.ledger, filter.ledger);
      assert.strictEqual(account.code, filter.code);
    }
  }

  {
    // Querying accounts where:
    // `user_data_128=2000 AND user_data_64=200 AND user_data_32=20
    // AND code=999 AND ledger=1 ORDER BY timestamp DESC`.
    var filter: QueryFilter = {
      user_data_128: 2000n,
      user_data_64: 200n,
      user_data_32: 20,
      ledger: 1,
      code: 999,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: BATCH_MAX,
      flags: QueryFilterFlags.reversed,
    }
    var query: Account[] = await client.queryAccounts(filter)
    assert.strictEqual(query.length, 5)

    var timestamp = 1n << 64n;
    for (var account of query) {
      assert.ok(timestamp > account.timestamp);
      timestamp = account.timestamp;

      assert.strictEqual(account.user_data_128, filter.user_data_128);
      assert.strictEqual(account.user_data_64, filter.user_data_64);
      assert.strictEqual(account.user_data_32, filter.user_data_32);
      assert.strictEqual(account.ledger, filter.ledger);
      assert.strictEqual(account.code, filter.code);
    }
  }

  {
    // Querying accounts where:
    // `code=999 ORDER BY timestamp ASC`
    var filter: QueryFilter = {
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      ledger: 0,
      code: 999,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: BATCH_MAX,
      flags: QueryFilterFlags.none,
    }
    var query: Account[] = await client.queryAccounts(filter)
    assert.strictEqual(query.length, 10)

    var timestamp = 0n;
    for (var account of query) {
      assert.ok(timestamp < account.timestamp);
      timestamp = account.timestamp;

      assert.strictEqual(account.code, filter.code);
    }
  }

  {
    // Querying accounts where:
    // `code=999 ORDER BY timestamp DESC LIMIT 5`.
    var filter: QueryFilter = {
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      ledger: 0,
      code: 999,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: 5,
      flags: QueryFilterFlags.reversed,
    }

    // First 5 items:
    var query: Account[] = await client.queryAccounts(filter)
    assert.strictEqual(query.length, 5)

    var timestamp = 1n << 64n;
    for (var account of query) {
      assert.ok(timestamp > account.timestamp);
      timestamp = account.timestamp;

      assert.strictEqual(account.code, filter.code);
    }

    // Next 5 items:
    filter.timestamp_max = timestamp - 1n
    query = await client.queryAccounts(filter)
    assert.strictEqual(query.length, 5)

    for (var account of query) {
      assert.ok(timestamp > account.timestamp);
      timestamp = account.timestamp;

      assert.strictEqual(account.code, filter.code);
    }

    // No more results:
    filter.timestamp_max = timestamp - 1n
    query = await client.queryAccounts(filter)
    assert.strictEqual(query.length, 0)
  }

  {
    // Not found:
    var filter: QueryFilter = {
      user_data_128: 0n,
      user_data_64: 200n,
      user_data_32: 10,
      ledger: 0,
      code: 0,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: BATCH_MAX,
      flags: QueryFilterFlags.none,
    }
    var query: Account[] = await client.queryAccounts(filter)
    assert.strictEqual(query.length, 0)
  }
})

test('can query transfers', async (): Promise<void> => {
  {
    const account: Account = {
      id: id(),
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
      flags: AccountFlags.none,
      timestamp: 0n
    }
    const accounts_results = await client.createAccounts([account])
    assert.deepStrictEqual(accounts_results.length, 1)
    assert.ok(accounts_results[0].timestamp > 0)
    assert.deepStrictEqual(accounts_results[0].result, CreateAccountResult.ok)

    var transfers_created : Transfer[] = [];
    // Create transfers:
    for (var i=0; i<10;i++) {
      transfers_created.push({
        id: id(),
        debit_account_id: i % 2 == 0 ? account.id : accountA.id,
        credit_account_id: i % 2 == 0 ? accountB.id : account.id,
        amount: 100n,
        user_data_128: i % 2 == 0 ? 1000n : 2000n,
        user_data_64: i % 2 == 0 ? 100n : 200n,
        user_data_32: i % 2 == 0 ? 10 : 20,
        pending_id: 0n,
        timeout: 0,
        ledger: 1,
        code: 999,
        flags: 0,
        timestamp: 0n,
      })
    }

    const transfers_results = await client.createTransfers(transfers_created)
    assert.deepStrictEqual(transfers_results.length, transfers_created.length)
    for (const item of transfers_results) {
      assert.ok(item.timestamp > 0)
      assert.deepStrictEqual(item.result, CreateTransferResult.ok)
    }
  }

  {
    // Querying transfers where:
    // `user_data_128=1000 AND user_data_64=100 AND user_data_32=10
    // AND code=999 AND ledger=1 ORDER BY timestamp ASC`.
    var filter: QueryFilter = {
      user_data_128: 1000n,
      user_data_64: 100n,
      user_data_32: 10,
      ledger: 1,
      code: 999,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: BATCH_MAX,
      flags: QueryFilterFlags.none,
    }
    var query: Transfer[] = await client.queryTransfers(filter)
    assert.strictEqual(query.length, 5)

    var timestamp = 0n;
    for (var transfer of query) {
      assert.ok(timestamp < transfer.timestamp);
      timestamp = transfer.timestamp;

      assert.strictEqual(transfer.user_data_128, filter.user_data_128);
      assert.strictEqual(transfer.user_data_64, filter.user_data_64);
      assert.strictEqual(transfer.user_data_32, filter.user_data_32);
      assert.strictEqual(transfer.ledger, filter.ledger);
      assert.strictEqual(transfer.code, filter.code);
    }
  }

  {
    // Querying transfers where:
    // `user_data_128=2000 AND user_data_64=200 AND user_data_32=20
    // AND code=999 AND ledger=1 ORDER BY timestamp DESC`.
    var filter: QueryFilter = {
      user_data_128: 2000n,
      user_data_64: 200n,
      user_data_32: 20,
      ledger: 1,
      code: 999,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: BATCH_MAX,
      flags: QueryFilterFlags.reversed,
    }
    var query: Transfer[] = await client.queryTransfers(filter)
    assert.strictEqual(query.length, 5)

    var timestamp = 1n << 64n;
    for (var transfer of query) {
      assert.ok(timestamp > transfer.timestamp);
      timestamp = transfer.timestamp;

      assert.strictEqual(transfer.user_data_128, filter.user_data_128);
      assert.strictEqual(transfer.user_data_64, filter.user_data_64);
      assert.strictEqual(transfer.user_data_32, filter.user_data_32);
      assert.strictEqual(transfer.ledger, filter.ledger);
      assert.strictEqual(transfer.code, filter.code);
    }
  }

  {
    // Querying transfers where:
    // `code=999 ORDER BY timestamp ASC`
    var filter: QueryFilter = {
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      ledger: 0,
      code: 999,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: BATCH_MAX,
      flags: QueryFilterFlags.none,
    }
    var query: Transfer[] = await client.queryTransfers(filter)
    assert.strictEqual(query.length, 10)

    var timestamp = 0n;
    for (var transfer of query) {
      assert.ok(timestamp < transfer.timestamp);
      timestamp = transfer.timestamp;

      assert.strictEqual(transfer.code, filter.code);
    }
  }

  {
    // Querying transfers where:
    // `code=999 ORDER BY timestamp DESC LIMIT 5`.
    var filter: QueryFilter = {
      user_data_128: 0n,
      user_data_64: 0n,
      user_data_32: 0,
      ledger: 0,
      code: 999,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: 5,
      flags: QueryFilterFlags.reversed,
    }

    // First 5 items:
    var query: Transfer[] = await client.queryTransfers(filter)
    assert.strictEqual(query.length, 5)

    var timestamp = 1n << 64n;
    for (var transfer of query) {
      assert.ok(timestamp > transfer.timestamp);
      timestamp = transfer.timestamp;

      assert.strictEqual(transfer.code, filter.code);
    }

    // Next 5 items:
    filter.timestamp_max = timestamp - 1n
    query = await client.queryTransfers(filter)
    assert.strictEqual(query.length, 5)

    for (var transfer of query) {
      assert.ok(timestamp > transfer.timestamp);
      timestamp = transfer.timestamp;

      assert.strictEqual(transfer.code, filter.code);
    }

    // No more results:
    filter.timestamp_max = timestamp - 1n
    query = await client.queryTransfers(filter)
    assert.strictEqual(query.length, 0)
  }

  {
    // Not found:
    var filter: QueryFilter = {
      user_data_128: 0n,
      user_data_64: 200n,
      user_data_32: 10,
      ledger: 0,
      code: 0,
      timestamp_min: 0n,
      timestamp_max: 0n,
      limit: BATCH_MAX,
      flags: QueryFilterFlags.none,
    }
    var query: Transfer[] = await client.queryTransfers(filter)
    assert.strictEqual(query.length, 0)
  }
})

test('query with invalid filter', async (): Promise<void> => {
  // Invalid timestamp min:
  var filter: QueryFilter = {
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    ledger: 0,
    code: 0,
    timestamp_min: (1n << 64n) - 1n, // ulong max value
    timestamp_max: 0n,
    limit: BATCH_MAX,
    flags: QueryFilterFlags.none,
  }
  assert.deepStrictEqual((await client.queryAccounts(filter)), [])
  assert.deepStrictEqual((await client.queryTransfers(filter)), [])

  // Invalid timestamp max:
  filter = {
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    ledger: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: (1n << 64n) - 1n, // ulong max value,
    limit: BATCH_MAX,
    flags: QueryFilterFlags.none,
  }
  assert.deepStrictEqual((await client.queryAccounts(filter)), [])
  assert.deepStrictEqual((await client.queryTransfers(filter)), [])

  // Invalid timestamp range:
  filter = {
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    ledger: 0,
    code: 0,
    timestamp_min: (1n << 64n) - 2n, // ulong max - 1
    timestamp_max: 1n,
    limit: BATCH_MAX,
    flags: QueryFilterFlags.none,
  }
  assert.deepStrictEqual((await client.queryAccounts(filter)), [])
  assert.deepStrictEqual((await client.queryTransfers(filter)), [])

  // Zero limit:
  filter = {
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    ledger: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: 0,
    flags: QueryFilterFlags.none,
  }
  assert.deepStrictEqual((await client.queryAccounts(filter)), [])
  assert.deepStrictEqual((await client.queryTransfers(filter)), [])

  // Invalid flags:
  filter = {
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    ledger: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: 0,
    flags: 0xFFFF,
  }
  assert.deepStrictEqual((await client.queryAccounts(filter)), [])
  assert.deepStrictEqual((await client.queryTransfers(filter)), [])
})

test('can import accounts and transfers', async (): Promise<void> => {
  const accountTmp: Account = {
    id: id(),
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
    timestamp: 0n // this will be set correctly by the TigerBeetle server
  }
  let accounts_results = await client.createAccounts([accountTmp])
  assert.deepStrictEqual(accounts_results.length, 1)
  assert.ok(accounts_results[0].timestamp > 0)
  assert.deepStrictEqual(accounts_results[0].result, CreateAccountResult.ok)

  let accountLookup = await client.lookupAccounts([accountTmp.id])
  assert.strictEqual(accountLookup.length, 1)
  const timestampMax = accountLookup[0].timestamp

  // Wait 10 ms so we can use the account's timestamp as the reference for past time
  // after the last object inserted.
  await new Promise(_ => setTimeout(_, 10));

  const accountA: Account = {
    id: id(),
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
    flags: AccountFlags.imported,
    timestamp: timestampMax + 1n // user-defined timestamp
  }
  const accountB: Account = {
    id: id(),
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
    flags: AccountFlags.imported,
    timestamp: timestampMax + 2n // user-defined timestamp
  }
  accounts_results = await client.createAccounts([accountA, accountB])
  assert.deepStrictEqual(accounts_results.length, 2)
  assert.ok(accounts_results[0].timestamp > 0)
  assert.deepStrictEqual(accounts_results[0].result, CreateAccountResult.ok)
  assert.ok(accounts_results[1].timestamp > 0)
  assert.deepStrictEqual(accounts_results[1].result, CreateAccountResult.ok)

  accountLookup = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accountLookup.length, 2)
  assert.strictEqual(accountLookup[0].timestamp, accountA.timestamp)
  assert.strictEqual(accountLookup[1].timestamp, accountB.timestamp)

  const transfer: Transfer = {
    id: id(),
    debit_account_id: accountA.id,
    credit_account_id: accountB.id,
    amount: 100n,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    pending_id: 0n,
    timeout: 0,
    ledger: 1,
    code: 1,
    flags: TransferFlags.imported,
    timestamp: timestampMax + 3n, // user-defined timestamp.
  }

  const transfers_results = await client.createTransfers([transfer])
  assert.deepStrictEqual(transfers_results.length, 1)
  assert.ok(transfers_results[0].timestamp > 0)
  assert.deepStrictEqual(transfers_results[0].result, CreateTransferResult.ok)

  const transfers = await client.lookupTransfers([transfer.id])
  assert.strictEqual(transfers.length, 1)
  assert.strictEqual(transfers[0].timestamp, timestampMax + 3n)
})

test('accept zero-length create_accounts', async (): Promise<void> => {
  const accounts_results = await client.createAccounts([])
  assert.deepStrictEqual(accounts_results.length, 0)
})

test('accept zero-length create_transfers', async (): Promise<void> => {
  const transfers_results = await client.createTransfers([])
  assert.deepStrictEqual(transfers_results.length, 0)
})

test('accept zero-length lookup_accounts', async (): Promise<void> => {
  const accounts = await client.lookupAccounts([])
  assert.deepStrictEqual(accounts, [])
})

test('accept zero-length lookup_transfers', async (): Promise<void> => {
  const transfers = await client.lookupTransfers([])
  assert.deepStrictEqual(transfers, [])
})

test("destroy client in-flight", async (): Promise<void> => {
  // Non-existing cluster.
  const client = createClient({ cluster_id: 92n, replica_addresses: ["99"] });
  setTimeout(() => client.destroy(), 30);
  try {
    await client.lookupAccounts([0n]);
  } catch (error) {
    assert.strictEqual(error.message, "Client was shutdown.");
    return;
  }
  throw "expected an error";
});

async function main () {
  const start = new Date().getTime()
  try {
    for (let i = 0; i < tests.length; i++) {
      await tests[i].fn().then(() => {
        console.log(tests[i].name + ": PASSED")
      }).catch(error => {
        console.log(tests[i].name + ": FAILED")
        throw error
      })
    }
    const end = new Date().getTime()
    console.log('Time taken (s):', (end - start)/1000)
  } finally {
    await client.destroy()
  }
}

main().catch((error: AssertionError) => {
  console.log('operator:', error.operator)
  console.log('stack:', error.stack)
  process.exit(-1);
})
