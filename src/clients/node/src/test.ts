import assert, { AssertionError } from 'assert'
import {
  createClient,
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
  user_data: 0n,
  reserved: Zeroed48Bytes,
  ledger: 1,
  code: 718,
  flags: 0,
  debits_pending: 0n,
  debits_posted: 0n,
  credits_pending: 0n,
  credits_posted: 0n,
  timestamp: 0n // this will be set correctly by the TigerBeetle server
}
const accountB: Account = {
  id: 19n,
  user_data: 0n,
  reserved: Zeroed48Bytes,
  ledger: 1,
  code: 719,
  flags: 0,
  debits_pending: 0n,
  debits_posted: 0n,
  credits_pending: 0n,
  credits_posted: 0n,
  timestamp: 0n // this will be set correctly by the TigerBeetle server
}

const tests: Array<{ name: string, fn: () => Promise<void> }> = []
function test(name: string, fn: () => Promise<void>) {
  tests.push({ name, fn })
}
test.skip = (name: string, fn: () => Promise<void>) => {
  console.log(name + ': SKIPPED')
}

test('range check `code` on Account to be u16', async (): Promise<void> => {
  const account = { ...accountA, id: 0n }

  account.code = 65535 + 1
  const codeError = await client.createAccounts([account]).catch(error => error)
  assert.strictEqual(codeError.message, 'code must be a u16.')

  const accounts = await client.lookupAccounts([account.id])
  assert.strictEqual(accounts.length, 0)
})

test('can create accounts', async (): Promise<void> => {
  const errors = await client.createAccounts([accountA])
  assert.strictEqual(errors.length, 0)
})

test('can return error on account', async (): Promise<void> => {
  const errors = await client.createAccounts([accountA, accountB])

  assert.strictEqual(errors.length, 1)
  assert.deepStrictEqual(errors[0], { index: 0, code: CreateAccountError.exists })
})

test('throws error if timestamp is not set to 0n on account', async (): Promise<void> => {
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
  assert.strictEqual(account1.ledger, 1)
  assert.strictEqual(account1.flags, 0)
  assert.strictEqual(account1.credits_posted, 0n)
  assert.strictEqual(account1.credits_pending, 0n)
  assert.strictEqual(account1.debits_posted, 0n)
  assert.strictEqual(account1.debits_pending, 0n)
  assert.ok(account1.timestamp > 0n)

  const account2 = accounts[1]
  assert.strictEqual(account2.id, 19n)
  assert.ok(account2.reserved.equals(Zeroed48Bytes))
  assert.strictEqual(account2.user_data, 0n)
  assert.strictEqual(account2.code, 719)
  assert.strictEqual(account2.ledger, 1)
  assert.strictEqual(account2.flags, 0)
  assert.strictEqual(account2.credits_posted, 0n)
  assert.strictEqual(account2.credits_pending, 0n)
  assert.strictEqual(account2.debits_posted, 0n)
  assert.strictEqual(account2.debits_pending, 0n)
  assert.ok(account2.timestamp > 0n)
})

test('can create a transfer', async (): Promise<void> => {
  const transfer: Transfer = {
    id: 1n,
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    user_data: 0n,
    reserved: 0n,
    pending_id: 0n,
    timeout: 0n,
    ledger: 1,
    code: 1,
    flags: 0,
    amount: 100n,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const errors = await client.createTransfers([transfer])
  assert.strictEqual(errors.length, 0)

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
    user_data: 0n,
    reserved: 0n,
    pending_id: 0n,
    timeout: BigInt(2e9),
    ledger: 1,
    code: 1,
    flags,
    amount: 50n,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const errors = await client.createTransfers([transfer])
  assert.strictEqual(errors.length, 0)

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
  assert.strictEqual(transfers[0].user_data, 0n)
  assert.notStrictEqual(transfers[0].reserved, Zeroed32Bytes)
  assert.strictEqual(transfers[0].timeout > 0, true)
  assert.strictEqual(transfers[0].code, 1)
  assert.strictEqual(transfers[0].flags, 2)
  assert.strictEqual(transfers[0].amount, 50n)
  assert.strictEqual(transfers[0].timestamp > 0, true)
})

test('can post a two-phase transfer', async (): Promise<void> => {
  let flags = 0
  flags |= TransferFlags.post_pending_transfer

  const commit: Transfer = {
    id: 3n,
    debit_account_id: BigInt(0),
    credit_account_id: BigInt(0),
    user_data: 0n,
    reserved: 0n,
    pending_id: 2n,// must match the id of the pending transfer
    timeout: 0n,
    ledger: 1,
    code: 1,
    flags: flags,
    amount: 0n,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const errors = await client.createTransfers([commit])
  assert.strictEqual(errors.length, 0)

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
    user_data: 0n,
    reserved: 0n,
    pending_id: 0n,
    timeout: BigInt(1e9),
    ledger: 1,
    code: 1,
    flags: TransferFlags.pending,
    amount: 50n,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }
  const transferErrors = await client.createTransfers([transfer])
  assert.strictEqual(transferErrors.length, 0)

  // send in the reject
  const reject: Transfer = {
    id: 5n,
    debit_account_id: BigInt(0),
    credit_account_id: BigInt(0),
    user_data: 0n,
    reserved: 0n,
    pending_id: 4n, // must match the id of the pending transfer
    timeout: 0n,
    ledger: 1,
    code: 1,
    flags: TransferFlags.void_pending_transfer,
    amount: 0n,
    timestamp: 0n, // this will be set correctly by the TigerBeetle server
  }

  const errors = await client.createTransfers([reject])
  assert.strictEqual(errors.length, 0)

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
    user_data: 0n,
    reserved: 0n,
    pending_id: 0n,
    timeout: 0n,
    ledger: 1,
    code: 1,
    flags: TransferFlags.linked, // points to transfer2
    amount: 100n,
    timestamp: 0n, // will be set correctly by the TigerBeetle server
  }
  const transfer2: Transfer = {
    id: 6n,
    debit_account_id: accountB.id,
    credit_account_id: accountA.id,
    user_data: 0n,
    reserved: 0n,
    pending_id: 0n,
    timeout: 0n,
    ledger: 1,
    code: 1,
    // Does not have linked flag as it is the end of the chain.
    // This will also cause it to fail as this is now a duplicate with different flags
    flags: 0,
    amount: 100n,
    timestamp: 0n, // will be set correctly by the TigerBeetle server
  }

  const errors = await client.createTransfers([transfer1, transfer2])
  assert.strictEqual(errors.length, 2)
  assert.deepStrictEqual(errors[0], { index: 0, code: CreateTransferError.linked_event_failed })
  assert.deepStrictEqual(errors[1], { index: 1, code: CreateTransferError.exists_with_different_flags })

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
