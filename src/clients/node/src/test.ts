import assert, { AssertionError } from 'assert'
import { CommitFlags, CommitTransfer, CreateAccount, CreateAccountError, createClient, CreateTransfer, CreateTransferFlags, CreateTransferError } from '.'

const client = createClient({
  cluster_id: 0x0a5ca1ab1ebee11en,
  replica_addresses: ['3001']
})

// Test data
const Zeroed32Bytes = Buffer.alloc(32, 0)
const Zeroed48Bytes = Buffer.alloc(48, 0)
const accountA: CreateAccount = {
  id: 17n,
  reserved: Zeroed48Bytes,
  user_data: 0n,
  code: 718,
  unit: 1,
  flags: 0,
  credits_accepted: 0n,
  credits_reserved: 0n,
  debits_accepted: 0n,
  debits_reserved: 0n,
  timestamp: 0n
}
const accountB: CreateAccount = {
  id: 19n,
  reserved: Zeroed48Bytes,
  user_data: 0n,
  code: 719,
  unit: 1,
  flags: 0,
  credits_accepted: 0n,
  credits_reserved: 0n,
  debits_accepted: 0n,
  debits_reserved: 0n,
  timestamp: 0n
}

const tests: Array<{ name: string, fn: () => Promise<void> }> = []
function test(name: string, fn: () => Promise<void>) {
  tests.push({ name, fn })
}
test.skip = (name: string, fn: () => Promise<void>) => {
  console.log(name + ': SKIPPED')
}

test('can create accounts', async (): Promise<void> => {
  const results = await client.createAccounts([accountA])

  assert.strictEqual(results.length, 0)
})

test('can return error', async (): Promise<void> => {
  const results = await client.createAccounts([accountA, accountB])

  assert.strictEqual(results.length, 1)
  assert.deepStrictEqual(results[0], { index: 0, error: CreateAccountError.exists })
})

test('can lookup accounts', async (): Promise<void> => {
  const results = await client.lookupAccounts([accountA.id, accountB.id])

  assert.strictEqual(results.length, 2)
  const result1 = results[0]
  assert.strictEqual(result1.id, 17n)
  assert.ok(result1.reserved.equals(Zeroed48Bytes))
  assert.strictEqual(result1.user_data, 0n)
  assert.strictEqual(result1.code, 718)
  assert.strictEqual(result1.unit, 1)
  assert.strictEqual(result1.flags, 0)
  assert.strictEqual(result1.credits_accepted, 0n)
  assert.strictEqual(result1.credits_reserved, 0n)
  assert.strictEqual(result1.debits_accepted, 0n)
  assert.strictEqual(result1.debits_reserved, 0n)

  const result2 = results[1]
  assert.strictEqual(result2.id, 19n)
  assert.ok(result2.reserved.equals(Zeroed48Bytes))
  assert.strictEqual(result2.user_data, 0n)
  assert.strictEqual(result2.code, 719)
  assert.strictEqual(result2.unit, 1)
  assert.strictEqual(result2.flags, 0)
  assert.strictEqual(result2.credits_accepted, 0n)
  assert.strictEqual(result2.credits_reserved, 0n)
  assert.strictEqual(result2.debits_accepted, 0n)
  assert.strictEqual(result2.debits_reserved, 0n)
})

test('can create a transfer', async (): Promise<void> => {
  const transfer: CreateTransfer = {
    id: 0n,
    amount: 100n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    flags: 0,
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: 0n,
  }

  const results = await client.createTransfers([transfer])
  assert.strictEqual(results.length, 0)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_accepted, 100n)
  assert.strictEqual(accounts[0].credits_reserved, 0n)
  assert.strictEqual(accounts[0].debits_accepted, 0n)
  assert.strictEqual(accounts[0].debits_reserved, 0n)

  assert.strictEqual(accounts[1].credits_accepted, 0n)
  assert.strictEqual(accounts[1].credits_reserved, 0n)
  assert.strictEqual(accounts[1].debits_accepted, 100n)
  assert.strictEqual(accounts[1].debits_reserved, 0n)
})

test('can create a two-phase transfer', async (): Promise<void> => {
  let flags = 0
  flags |= CreateTransferFlags.two_phase_commit
  const transfer: CreateTransfer = {
    id: 1n,
    amount: 50n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    flags,
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: BigInt(2e9),
  }

  const results = await client.createTransfers([transfer])
  assert.strictEqual(results.length, 0)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_accepted, 100n)
  assert.strictEqual(accounts[0].credits_reserved, 50n)
  assert.strictEqual(accounts[0].debits_accepted, 0n)
  assert.strictEqual(accounts[0].debits_reserved, 0n)

  assert.strictEqual(accounts[1].credits_accepted, 0n)
  assert.strictEqual(accounts[1].credits_reserved, 0n)
  assert.strictEqual(accounts[1].debits_accepted, 100n)
  assert.strictEqual(accounts[1].debits_reserved, 50n)
})

test('can commit a two-phase transfer', async (): Promise<void> => {
  const commit: CommitTransfer = {
    id: 1n, // must match the id of the create transfer
    code: 1,
    flags: 0, // defaults to accept
    reserved: Zeroed32Bytes
  }

  const results = await client.commitTransfers([commit])
  assert.strictEqual(results.length, 0)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_accepted, 150n)
  assert.strictEqual(accounts[0].credits_reserved, 0n)
  assert.strictEqual(accounts[0].debits_accepted, 0n)
  assert.strictEqual(accounts[0].debits_reserved, 0n)

  assert.strictEqual(accounts[1].credits_accepted, 0n)
  assert.strictEqual(accounts[1].credits_reserved, 0n)
  assert.strictEqual(accounts[1].debits_accepted, 150n)
  assert.strictEqual(accounts[1].debits_reserved, 0n)
})

test('can reject a two-phase transfer', async (): Promise<void> => {
  // create a two-phase transfer
  let flags = 0
  flags |= CreateTransferFlags.two_phase_commit
  const transfer: CreateTransfer = {
    id: 3n,
    amount: 50n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    flags,
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: BigInt(1e9),
  }
  const transferResults = await client.createTransfers([transfer])
  assert.strictEqual(transferResults.length, 0)

  // send in the reject
  flags = 0
  flags |= CommitFlags.reject
  const reject: CommitTransfer = {
    id: 3n,
    code: 1,
    flags,
    reserved: Zeroed32Bytes
  }

  const results = await client.commitTransfers([reject])
  assert.strictEqual(results.length, 0)

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_accepted, 150n)
  assert.strictEqual(accounts[0].credits_reserved, 0n)
  assert.strictEqual(accounts[0].debits_accepted, 0n)
  assert.strictEqual(accounts[0].debits_reserved, 0n)

  assert.strictEqual(accounts[1].credits_accepted, 0n)
  assert.strictEqual(accounts[1].credits_reserved, 0n)
  assert.strictEqual(accounts[1].debits_accepted, 150n)
  assert.strictEqual(accounts[1].debits_reserved, 0n)
})

test('can link transfers', async (): Promise<void> => {
  let flags = 0
  flags |= CreateTransferFlags.linked // points to transfer2
  const transfer1: CreateTransfer = {
    id: 4n,
    amount: 100n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    flags,
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: 0n,
  }
  const transfer2: CreateTransfer = {
    id: 4n,
    amount: 100n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    // Does not have linked flag as it is the end of the chain.
    // This will also cause it to fail as this is now a duplicate with different flags
    flags: 0,
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: 0n,
  }

  const results = await client.createTransfers([transfer1, transfer2])
  assert.strictEqual(results.length, 2)
  assert.deepStrictEqual(results[0], { index: 0, error: CreateTransferError.linked_event_failed })
  assert.deepStrictEqual(results[1], { index: 1, error: CreateTransferError.exists_with_different_flags })

  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  assert.strictEqual(accounts.length, 2)
  assert.strictEqual(accounts[0].credits_accepted, 150n)
  assert.strictEqual(accounts[0].credits_reserved, 0n)
  assert.strictEqual(accounts[0].debits_accepted, 0n)
  assert.strictEqual(accounts[0].debits_reserved, 0n)

  assert.strictEqual(accounts[1].credits_accepted, 0n)
  assert.strictEqual(accounts[1].credits_reserved, 0n)
  assert.strictEqual(accounts[1].debits_accepted, 150n)
  assert.strictEqual(accounts[1].debits_reserved, 0n)
})

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
})