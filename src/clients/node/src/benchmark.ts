import assert from 'assert'
import {
  Account,
  createClient,
  Transfer,
  TransferFlags,
} from '.'

const MAX_TRANSFERS = 51200
const MAX_REQUEST_BATCH_SIZE = 5120
const IS_TWO_PHASE_TRANSFER = false

const client = createClient({
  cluster_id: 0n,
  replica_addresses: [process.env.TB_ADDRESS || '3000']
})

const TRANSFER_SIZE = 128
const accountA: Account = {
  id: 137n,
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
}

const accountB: Account = {
  id: 138n,
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
}

const runBenchmark = async () => {
  console.log(`pre-allocating ${MAX_TRANSFERS} transfers and posts...`)
  const transfers: Transfer[][] = []
  const posts: Transfer[][] = []

  let count = 0
  while (count < MAX_TRANSFERS) {
    const pendingBatch: Transfer[] = []
    const postBatch: Transfer[] = []
    for (let i = 0; i < MAX_REQUEST_BATCH_SIZE; i++) {
      if (count === MAX_TRANSFERS) break

      count += 1
      pendingBatch.push({
        id: BigInt(count),
        debit_account_id: accountA.id,
        credit_account_id: accountB.id,
        amount: 1n,        
        pending_id: 0n,
        user_data_128: 0n,
        user_data_64: 0n,
        user_data_32: 0,
        timeout: IS_TWO_PHASE_TRANSFER ? 2 : 0,
        code: 1,
        ledger: 1,
        flags: IS_TWO_PHASE_TRANSFER ? TransferFlags.pending : 0,
        timestamp: 0n,
      })
    
      if (IS_TWO_PHASE_TRANSFER) {
        postBatch.push({
          id: BigInt(MAX_TRANSFERS + count),
          debit_account_id: accountA.id,
          credit_account_id: accountB.id,
          amount: 1n,
          pending_id: BigInt(count),
          user_data_128: 0n,
          user_data_64: 0n,
          user_data_32: 0,
          timeout: 0,
          code: 1,
          ledger: 1,
          flags: IS_TWO_PHASE_TRANSFER ? TransferFlags.post_pending_transfer : 0,
          timestamp: 0n,
        })
      }
    }

    transfers.push(pendingBatch)
    if (IS_TWO_PHASE_TRANSFER) posts.push(postBatch)
  }
  assert(count === MAX_TRANSFERS)

  console.log(`starting benchmark. MAX_TRANSFERS=${MAX_TRANSFERS} REQUEST_BATCH_SIZE=${MAX_REQUEST_BATCH_SIZE} NUMBER_OF_BATCHES=${transfers.length}`)
  let maxCreateTransfersLatency = 0
  let maxCommitTransfersLatency = 0
  const start = Date.now()

  for (let i = 0; i < transfers.length; i++) {
    const ms1 = Date.now()

    const transferErrors = await client.createTransfers(transfers[i])
    assert(transferErrors.length === 0)

    const ms2 = Date.now()
    const createTransferLatency = ms2 - ms1
    if (createTransferLatency > maxCreateTransfersLatency) {
      maxCreateTransfersLatency = createTransferLatency
    }

    if (IS_TWO_PHASE_TRANSFER) {
      const commitErrors = await client.createTransfers(posts[i])
      assert(commitErrors.length === 0)

      const ms3 = Date.now()
      const commitTransferLatency = ms3 - ms2
      if (commitTransferLatency > maxCommitTransfersLatency) {
        maxCommitTransfersLatency = commitTransferLatency
      }
    }
  }

  const ms = Date.now() - start

  return {
    ms,
    maxCommitTransfersLatency,
    maxCreateTransfersLatency
  }
}

const main = async () => {  
  console.log("creating the accounts...")
  await client.createAccounts([accountA, accountB])
  const accountResults = await client.lookupAccounts([accountA.id, accountB.id])
  assert(accountResults.length === 2)
  assert(accountResults[0].debits_posted === 0n)
  assert(accountResults[1].debits_posted === 0n)

  const benchmark = await runBenchmark()
  
  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  const result = Math.floor((1000 * MAX_TRANSFERS)/benchmark.ms)
  console.log("=============================")
  console.log(`${IS_TWO_PHASE_TRANSFER ? 'two-phase ' : ''}transfers per second: ${result}`)
  console.log(`create transfers max p100 latency per 10 000 transfers = ${benchmark.maxCreateTransfersLatency}ms`)
  console.log(`commit transfers max p100 latency per 10 000 transfers = ${benchmark.maxCommitTransfersLatency}ms`)
  assert(accounts.length === 2)
  assert(accounts[0].debits_posted === BigInt(MAX_TRANSFERS))
  assert(accounts[1].credits_posted === BigInt(MAX_TRANSFERS))
}

main().catch(error => { 
  console.log(error)
}).finally(async () => {
  await client.destroy()
})
