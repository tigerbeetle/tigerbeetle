import assert, { AssertionError } from 'assert'
import { CommitFlags,
  createClient,
  Commit,
  Account,
  Transfer,
  TransferFlags,
  CreateAccountError,
  CreateTransferError
} from '.'

const client = createClient({
  cluster_id: 0,
  replica_addresses: ['3001']
})

// Test data
const Zeroed32Bytes = Buffer.alloc(32, 0)
const Zeroed48Bytes = Buffer.alloc(48, 0)
const accountA: Account = {
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
  timestamp: 0n // this will be set correctly by the TigerBeetle server
}
const accountB: Account = {
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
  timestamp: 0n // this will be set correctly by the TigerBeetle server
}

const tests: Array<{ name: string, fn: () => Promise<void> }> = []
function test(name: string, fn: () => Promise<void>) {
  tests.push({ name, fn })
}
test.skip = (name: string, fn: () => Promise<void>) => {
  console.log(name + ': SKIPPED')
}

test('range checks `unit` and `code` on Account to be u16', async (): Promise<void> => {
  const account = { ...accountA, id: 0n, unit: 65535 + 1 }

  const unitError = await client.createAccounts([account]).catch(error => error)
  assert.strictEqual(unitError.message, 'unit must be a u16.')

  account.unit = 0
  account.code = 65535 + 1
  const codeError = await client.createAccounts([account]).catch(error => error)
  assert.strictEqual(codeError.message, 'code must be a u16.')

  const accounts = await client.lookupAccounts([0n])
  assert.strictEqual(accounts.length, 0)
})

test('can create accounts', async (): Promise<void> => {
  const errors = await client.createAccounts([accountA])

  assert.strictEqual(errors.length, 0)
})

test('can return error', async (): Promise<void> => {
  const errors = await client.createAccounts([accountA, accountB])

  assert.strictEqual(errors.length, 1)
  assert.deepStrictEqual(errors[0], { index: 0, code: CreateAccountError.exists })
})

test('throws error if timestamp is not set to 0n', async (): Promise<void> => {
  const account = { ...accountA, timestamp: 2n, id: 3n }
  await assert.rejects(client.createAccounts([account]))
})

test('can lookup accounts', async (): Promise<void> => {
  const accounts = await client.lookupAccounts([accountA.id, accountB.id])

  assert.strictEqual(accounts.length, 2)
  const account1 = accounts[0]
  assert.strictEqual(account1.id, 17n)
  assert.ok(account1.reserved.equals(Zeroed48Bytes))
  assert.strictEqual(account1.user_data, 0n)
  assert.strictEqual(account1.code, 718)
  assert.strictEqual(account1.unit, 1)
  assert.strictEqual(account1.flags, 0)
  assert.strictEqual(account1.credits_accepted, 0n)
  assert.strictEqual(account1.credits_reserved, 0n)
  assert.strictEqual(account1.debits_accepted, 0n)
  assert.strictEqual(account1.debits_reserved, 0n)
  assert.ok(account1.timestamp > 0n)

  const account2 = accounts[1]
  assert.strictEqual(account2.id, 19n)
  assert.ok(account2.reserved.equals(Zeroed48Bytes))
  assert.strictEqual(account2.user_data, 0n)
  assert.strictEqual(account2.code, 719)
  assert.strictEqual(account2.unit, 1)
  assert.strictEqual(account2.flags, 0)
  assert.strictEqual(account2.credits_accepted, 0n)
  assert.strictEqual(account2.credits_reserved, 0n)
  assert.strictEqual(account2.debits_accepted, 0n)
  assert.strictEqual(account2.debits_reserved, 0n)
  assert.ok(account2.timestamp > 0n)
})

test('can create a transfer', async (): Promise<void> => {
  const transfer: Transfer = {
    id: 0n,
    amount: 100n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    flags: 0,
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: 0n,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const errors = await client.createTransfers([transfer])
  assert.strictEqual(errors.length, 0)

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
  flags |= TransferFlags.two_phase_commit
  const transfer: Transfer = {
    id: 1n,
    amount: 50n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    flags,
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: BigInt(2e9),
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const errors = await client.createTransfers([transfer])
  assert.strictEqual(errors.length, 0)

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
  const commit: Commit = {
    id: 1n, // must match the id of the create transfer
    code: 1,
    flags: 0, // defaults to accept
    reserved: Zeroed32Bytes,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const errors = await client.commitTransfers([commit])
  assert.strictEqual(errors.length, 0)

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
  const transfer: Transfer = {
    id: 3n,
    amount: 50n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    flags: TransferFlags.two_phase_commit,
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: BigInt(1e9),
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }
  const transferErrors = await client.createTransfers([transfer])
  assert.strictEqual(transferErrors.length, 0)

  // send in the reject
  const reject: Commit = {
    id: 3n,
    code: 1,
    flags: CommitFlags.reject,
    reserved: Zeroed32Bytes,
    timestamp: 0n,// this will be set correctly by the TigerBeetle server
  }

  const errors = await client.commitTransfers([reject])
  assert.strictEqual(errors.length, 0)

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
  const transfer1: Transfer = {
    id: 4n,
    amount: 100n,
    code: 1,
    credit_account_id: accountA.id,
    debit_account_id: accountB.id,
    flags: TransferFlags.linked, // points to transfer2
    user_data: 0n,
    reserved: Zeroed32Bytes,
    timeout: 0n,
    timestamp: 0n, // will be set correctly by the TigerBeetle server
  }
  const transfer2: Transfer = {
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
    timestamp: 0n, // will be set correctly by the TigerBeetle server
  }

  const errors = await client.createTransfers([transfer1, transfer2])
  assert.strictEqual(errors.length, 2)
  assert.deepStrictEqual(errors[0], { index: 0, code: CreateTransferError.linked_event_failed })
  assert.deepStrictEqual(errors[1], { index: 1, code: CreateTransferError.exists_with_different_flags })

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
