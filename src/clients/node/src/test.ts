import assert, { AssertionError } from 'assert'
import {
  createClient,
  Account,
  Transfer,
  TransferFlags,
  CreateAccountStatus,
  CreateTransferStatus,
  AccountFilter,
  AccountFilterFlags,
  AccountFlags,
  id,
  QueryFilter,
  QueryFilterFlags,
  ErrorCodes,
  RequestError,
} from '.'

async function sleep_ms(ms: number): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, ms))
}

function range(n: number): number[] {
  return Array.from({ length: n }, (_, i) => i);
}

function random_index(array: Array<any>): number {
  return Math.floor(Math.random() * array.length);
}

const REPLICA_ADDRESSES = process.env.TB_ADDRESS || '3000';
const client = createClient({
  cluster_id: 0n,
  replica_addresses: REPLICA_ADDRESSES
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

test('Serialization: BigInt exceeds U128', async (): Promise<void> => {
  const transfer: Transfer = {
      id: 9999999999999999999999999999999999999999n,
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
  };

  assert.rejects(async() => await client.createTransfers([transfer]), (err) => {
    assert.ok(err instanceof Error)
    assert.strictEqual(err.message, "id must fit in 128 bits")
    return true
  })
})

test('Serialization: BigInt negative', async (): Promise<void> => {
  const transfer: Transfer = {
      id: -1n,
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
  };

  assert.rejects(async() => await client.createTransfers([transfer]), (err) => {
    assert.ok(err instanceof Error)
    assert.strictEqual(err.message, "id must be positive")
    return true
  })
})

test('range check `code` on Account to be u16', async (): Promise<void> => {
  const account = { ...accountA, id: 0n }

  account.code = 65535 + 1
  const codeError = await client.createAccounts([account]).catch(error => error)
  assert.strictEqual(codeError.message, 'code must be a u16.')

  const accounts = await client.lookupAccounts([account.id])
  assert.deepStrictEqual(accounts, [])
})

test('batch max size', async (): Promise<void> => {
  const BATCH_SIZE = 10_000;
  const transfers: Transfer[] = [];
  for (let i=0; i<BATCH_SIZE;i++) {
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
  assert.rejects(async() => await client.createTransfers(transfers), (err) => {
    assert.ok(err instanceof RequestError)
    assert.strictEqual(err.code,  ErrorCodes.ERR_TOO_MUCH_DATA)
    return true
  })
})

test('batch invalid size', async (): Promise<void> => {
  const transfers: Transfer[] = [];
  transfers.length = 0xffffffff;

  assert.rejects(async() => await client.createTransfers(transfers), (err) => {
    assert.ok(err instanceof RequestError)
    assert.strictEqual(err.code,  ErrorCodes.ERR_TOO_MUCH_DATA)
    return true
  })
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
  const account_results = await client.createAccounts([accountC, accountA, accountB])
  assert.deepStrictEqual(account_results.length, 3)
  for (const result of account_results) {
    assert.ok(result.timestamp > 0)
    assert.deepStrictEqual(result.status, CreateAccountStatus.created)
  }

  const transfers_created : Transfer[] = [];
  // Create transfers where the new account is either the debit or credit account:
  for (let i=0; i<10;i++) {
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
  for (const result of transfers_results) {
    assert.ok(result.timestamp > 0)
    assert.deepStrictEqual(result.status, CreateTransferStatus.created)
  }

  // TooMuchData
  let filter: AccountFilter = {
    account_id: accountC.id,
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: 10_000,
    flags: AccountFilterFlags.credits | AccountFilterFlags.debits,
  }
  assert.rejects(async() => await client.getAccountTransfers(filter), (err) => {
    assert.ok(err instanceof RequestError)
    assert.strictEqual(err.code,  ErrorCodes.ERR_TOO_MUCH_DATA)
    return true
  })
  assert.rejects(async() => await client.getAccountBalances(filter), (err) => {
    assert.ok(err instanceof RequestError)
    assert.strictEqual(err.code,  ErrorCodes.ERR_TOO_MUCH_DATA)
    return true
  })

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

test('query with invalid filter', async (): Promise<void> => {
  // TooMuchData
  var filter: QueryFilter = {
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    ledger: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: 10_000,
    flags: QueryFilterFlags.none,
  }
  assert.rejects(async() => await client.queryAccounts(filter), (err) => {
    assert.ok(err instanceof RequestError)
    assert.strictEqual(err.code,  ErrorCodes.ERR_TOO_MUCH_DATA)
    return true
  })
  assert.rejects(async() => await client.queryTransfers(filter), (err) => {
    assert.ok(err instanceof RequestError)
    assert.strictEqual(err.code,  ErrorCodes.ERR_TOO_MUCH_DATA)
    return true
  })

  // Invalid flags:
  filter = {
    user_data_128: 0n,
    user_data_64: 0n,
    user_data_32: 0,
    ledger: 0,
    code: 0,
    timestamp_min: 0n,
    timestamp_max: 0n,
    limit: BATCH_MAX,
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
  let account_results = await client.createAccounts([accountTmp])
  assert.deepStrictEqual(account_results.length, 1)
  assert.ok(account_results[0].timestamp > 0)
  assert.deepStrictEqual(account_results[0].status, CreateAccountStatus.created)

  let accountLookup = await client.lookupAccounts([accountTmp.id])
  assert.strictEqual(accountLookup.length, 1)
  const timestampMax = accountLookup[0].timestamp

  // Wait 10 ms so we can use the account's timestamp as the reference for past time
  // after the last object inserted.
  await sleep_ms(10);

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
  account_results = await client.createAccounts([accountA, accountB])
  assert.deepStrictEqual(account_results.length, 2)
  assert.ok(account_results[0].timestamp > 0)
  assert.deepStrictEqual(account_results[0].status, CreateAccountStatus.created)
  assert.ok(account_results[1].timestamp > 0)
  assert.deepStrictEqual(account_results[1].status, CreateAccountStatus.created)

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
  assert.deepStrictEqual(transfers_results[0].status, CreateTransferStatus.created)

  const transfers = await client.lookupTransfers([transfer.id])
  assert.strictEqual(transfers.length, 1)
  assert.strictEqual(transfers[0].timestamp, timestampMax + 3n)
})

test("destroy client in-flight", async (): Promise<void> => {
  const client_count = 5;
  const action_count = 50;

  const clients = range(client_count).map(() =>
    createClient({
      cluster_id: 0n,
      replica_addresses: REPLICA_ADDRESSES,
    })
  );

  const ids: Array<bigint> = [];
  const actions = range(action_count).map(() => async () => {
    await sleep_ms(Math.random() < 0.2 ? 0 : Math.random());
    const client = clients[random_index(clients)];
    if (Math.random() < 0.1) {
      client.destroy();
      return;
    }
    if (Math.random() < 0.7) {
      const id_new = id();
      ids.push(id_new);
      try {
        await client.createAccounts([{ ...accountA, id: id_new }]);
      } catch (err) {
        assert.ok(err instanceof RequestError);
        assert.strictEqual(err.code, ErrorCodes.ERR_CLIENT_CLOSED);
      }
      return;
    }
    try {
      const id_lookup = (Math.random() < 0.2 || ids.length == 0)
        ? BigInt(Math.floor(Math.random() * 10000))
        : ids[random_index(ids)];
      await client.lookupAccounts([id_lookup]);
    } catch (err) {
        assert.ok(err instanceof RequestError);
        assert.strictEqual(err.code, ErrorCodes.ERR_CLIENT_CLOSED);
    }
  });

  await Promise.all(actions.map((f) => f()));
  for (const client of clients) client.destroy();
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
