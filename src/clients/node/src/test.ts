import assert, { AssertionError } from 'assert'
import {
  createClient,
  Account,
  Transfer,
  id,
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

const tests: Array<{ name: string, fn: () => Promise<void> }> = []
function test(name: string, fn: () => Promise<void>) {
  tests.push({ name, fn })
}
test.skip = (name: string, fn: () => Promise<void>) => {
  console.log(name + ': SKIPPED')
}

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
