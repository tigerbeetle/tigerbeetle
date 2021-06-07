import assert from 'assert'
import {
  Commit,
  Account,
  createClient,
  Transfer,
  TransferFlags
} from '.'

const MAX_TRANSFERS = 1000000
const MAX_REQUEST_BATCH_SIZE = 10000

const client = createClient({
  cluster_id: 0x0a5ca1ab1ebee11en,
  replica_addresses: ['3001']
})

const Zeroed48Bytes = Buffer.alloc(48, 0)
const Zeroed32Bytes = Buffer.alloc(32, 0)
const accountA: Account = {
  id: 137n,
  user_data: 0n,
  reserved: Zeroed48Bytes,
  code: 0,
  flags: 0,
  unit: 1,
  debits_accepted: 0n,
  debits_reserved: 0n,
  credits_accepted: 0n,
  credits_reserved: 0n,
  timestamp: 0n,
}

const accountB: Account = {
  id: 138n,
  user_data: 0n,
  reserved: Zeroed48Bytes,
  code: 0,
  flags: 0,
  unit: 1,
  debits_accepted: 0n,
  debits_reserved: 0n,
  credits_accepted: 0n,
  credits_reserved: 0n,
  timestamp: 0n,
}

console.log(`pre-allocating ${MAX_TRANSFERS} transfers and commits...`)
const transfers: Transfer[][] = []
const commits: Commit[][] = []

let count = 0
while (count < MAX_TRANSFERS) {
  const transferBatch: Transfer[] = []
  const commitBatch: Commit[] = []
  for (let i = 0; i < MAX_REQUEST_BATCH_SIZE; i++) {
    if (count === MAX_TRANSFERS) break

    count += 1
    transferBatch.push({
      id: BigInt(count),
      debit_account_id: accountA.id,
      credit_account_id: accountB.id,
      code: 0,
      reserved: Zeroed32Bytes,
      user_data: 0n,
      flags: TransferFlags.two_phase_commit,
      amount: 1n,
      timeout: BigInt(2e9),
      timestamp: 0n,
    })
  
    commitBatch.push({
      id: BigInt(count),
      reserved: Buffer.alloc(32, 0),
      code: 0,
      flags: 0,
      timestamp: 0n,
    })
  }

  transfers.push(transferBatch)
  commits.push(commitBatch)
}
assert(count === MAX_TRANSFERS)

const main = async () => {
  console.log("creating the accounts...")
  await client.createAccounts([accountA, accountB])
  const accountResults = await client.lookupAccounts([accountA.id, accountB.id])
  assert(accountResults.length === 2)
  
  console.log(`starting benchmark. MAX_TRANSFERS=${MAX_TRANSFERS} REQUEST_BATCH_SIZE=${MAX_REQUEST_BATCH_SIZE} NUMBER_OF_BATCHES=${transfers.length}`)
  let maxCreateTransfersLatency = 0
  let maxCommitTransfersLatency = 0
  const start = Date.now()

  for(let i = 0; i < transfers.length; i++) {
    const ms1 = Date.now()

    const transferResults = await client.createTransfers(transfers[i])
    assert(transferResults.length === 0)

    const ms2 = Date.now()
    const createTransferLatency = ms2 - ms1
    if (createTransferLatency > maxCreateTransfersLatency) {
      maxCreateTransfersLatency = createTransferLatency
    }

    const commitResults = await client.commitTransfers(commits[i])
    assert(commitResults.length === 0)

    const ms3 = Date.now()
    const commitTransferLatency = ms3 - ms2
    if (commitTransferLatency > maxCommitTransfersLatency) {
      maxCommitTransfersLatency = commitTransferLatency
    }
  }

  const ms = Date.now() - start
  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  console.log("=============================")
  console.log(`transfers per second: ${Math.floor((1000 * MAX_TRANSFERS)/ms)}`)
  console.log(`create transfers max p100 latency per 10 000 transfers = ${maxCreateTransfersLatency}ms`)
  console.log(`commit transfers max p100 latency per 10 000 transfers = ${maxCommitTransfersLatency}ms`)
  assert(accounts.length === 2)
  assert(accounts[0].debits_accepted === BigInt(MAX_TRANSFERS))
  assert(accounts[1].credits_accepted === BigInt(MAX_TRANSFERS))
}

main().catch(error => { 
  console.log(error)
}).finally(async () => {
  await client.destroy()
})