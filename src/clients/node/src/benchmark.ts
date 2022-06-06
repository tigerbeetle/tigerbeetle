import assert from 'assert'
import {
  Account,
  createClient,
  Transfer,
  TransferFlags,
  CreateTransfersError,
  Operation
} from '.'

const MAX_TRANSFERS = 1000000
const MAX_REQUEST_BATCH_SIZE = 10000
const IS_TWO_PHASE_COMMIT = false
const IS_RAW_REQUEST = false
const PREVIOUS_RAW_REQUEST_RESULT = IS_TWO_PHASE_COMMIT ? 300000 : 620000
const PREVIOUS_RESULT = IS_TWO_PHASE_COMMIT ? 150000 : 310000
const PREVIOUS_BENCHMARK = IS_RAW_REQUEST ? PREVIOUS_RAW_REQUEST_RESULT : PREVIOUS_RESULT
const TOLERANCE = 10 // percent that the benchmark is allowed to deviate from the previous benchmark

const client = createClient({
  cluster_id: 1,
  replica_addresses: ['3001']
})

const TRANSFER_SIZE = 128
const COMMIT_SIZE = 64
const Zeroed48Bytes = Buffer.alloc(48, 0)
const Zeroed32Bytes = Buffer.alloc(32, 0)
const accountA: Account = {
  id: 137n,
  user_data: 0n,
  reserved: Zeroed48Bytes,
  ledger: 1,
  code: 0,
  flags: 0,
  debits_posted: 0n,
  debits_pending: 0n,
  credits_posted: 0n,
  credits_pending: 0n,
  timestamp: 0n,
}

const accountB: Account = {
  id: 138n,
  user_data: 0n,
  reserved: Zeroed48Bytes,
  ledger: 1,
  code: 0,
  flags: 0,
  debits_posted: 0n,
  debits_pending: 0n,
  credits_posted: 0n,
  credits_pending: 0n,
  timestamp: 0n,
}

// helper function to promisify the raw_request
const rawCreateTransfers = async (batch: Buffer): Promise<CreateTransfersError[]> => {
  return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: CreateTransfersError[]) => {
        if (error) {
          reject(error)
        }
        resolve(results)
      }

      try {
        client.rawRequest(Operation.CREATE_TRANSFER, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
}

/**
 * This encoding function is only for this benchmark script.
 * 
 * ID_OFFSET = 0
 * DEBIT_ACCOUNT_ID_OFFSET = 0 + 16 = 16
 * CREDIT_ACCOUNT_ID_OFFSET = 16 + 16 = 32
 * USER_DATA_OFFSET = 32 + 16 = 48
 * RESERVED_OFFSET = 48 + 16 = 64
 * TIMEOUT_OFFSET = 64 + 32 = 96
 * CODE_OFFSET = 96 + 8 = 104
 * FLAGS_OFFSET = 104 + 4 = 108
 * AMOUNT_OFFSET = 108 + 4 = 112
 * TIMESTAMP_OFFSET = 112 + 8 = 120
 */ 
const encodeTransfer = (transfer: Transfer, offset: number, output: Buffer): void => {
   assert(offset + TRANSFER_SIZE <= output.length)

   output.writeBigUInt64LE(transfer.id, offset)
   output.writeBigUInt64LE(transfer.debit_account_id, offset + 16)
   output.writeBigUInt64LE(transfer.credit_account_id, offset + 32)
   output.writeBigUInt64LE(transfer.timeout, offset + 96)
   output.writeUInt32LE(transfer.code, offset + 104)
   output.writeUInt32LE(transfer.flags, offset + 108)
   output.writeBigUInt64LE(transfer.amount, offset + 112)
   output.writeBigUInt64LE(transfer.timestamp, offset + 120)
}

const runBenchmarkRawRequest = async () => {
  assert(
    MAX_TRANSFERS % MAX_REQUEST_BATCH_SIZE === 0,
    "The raw request benchmark requires MAX_TRANSFERS to be a multiple of MAX_REQUEST_BATCH_SIZE"
  )
  console.log(`pre-allocating ${MAX_TRANSFERS} transfers and commits...`)
  const transfers: Buffer[] = []
  const commits: Buffer[] = []

  let count = 0
  while (count < MAX_TRANSFERS) {
    const transferBatch = Buffer.alloc(MAX_REQUEST_BATCH_SIZE * TRANSFER_SIZE, 0)
    const commitBatch = Buffer.alloc(MAX_REQUEST_BATCH_SIZE * COMMIT_SIZE, 0)
    for (let i = 0; i < MAX_REQUEST_BATCH_SIZE; i++) {
      if (count === MAX_TRANSFERS) break

      count += 1
      encodeTransfer(
        {
          id: BigInt(count),
          debit_account_id: accountA.id,
          credit_account_id: accountB.id,
          code: 1,
          ledger: 1,
          reserved: 0n,
          pending_id: 0n,
          user_data: 0n,
          flags: IS_TWO_PHASE_COMMIT ? TransferFlags.pending : 0,
          amount: 1n,
          timeout: IS_TWO_PHASE_COMMIT ? BigInt(2e9) : 0n,
          timestamp: 0n,
        },
        i * TRANSFER_SIZE,
        transferBatch
      )
    
      if (IS_TWO_PHASE_COMMIT) {
        encodeTransfer(
          {
            id: BigInt(MAX_TRANSFERS - count),
            debit_account_id: accountA.id,
            credit_account_id: accountB.id,
            code: 1,
            ledger: 1,
            reserved: 0n,
            pending_id: BigInt(count),
            user_data: 0n,
            flags: TransferFlags.post_pending_transfer,
            amount: 1n,
            timeout: 0n,
            timestamp: 0n,
          },
          i * COMMIT_SIZE,
          commitBatch
        )
      }
    }

    transfers.push(transferBatch)
    if (IS_TWO_PHASE_COMMIT) commits.push(commitBatch)
  }
  assert(count === MAX_TRANSFERS)

  console.log(`starting benchmark. MAX_TRANSFERS=${MAX_TRANSFERS} REQUEST_BATCH_SIZE=${MAX_REQUEST_BATCH_SIZE} NUMBER_OF_BATCHES=${transfers.length}`)
  let maxCreateTransfersLatency = 0
  let maxCommitTransfersLatency = 0
  const start = Date.now()

  for (let i = 0; i < transfers.length; i++) {
    const ms1 = Date.now()

    const transferResults = await rawCreateTransfers(transfers[i])
    assert(transferResults.length === 0)

    const ms2 = Date.now()
    const createTransferLatency = ms2 - ms1
    if (createTransferLatency > maxCreateTransfersLatency) {
      maxCreateTransfersLatency = createTransferLatency
    }

    if (IS_TWO_PHASE_COMMIT) {
      const commitResults = await rawCreateTransfers(commits[i])
      assert(commitResults.length === 0)

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

const runBenchmark = async () => {
  console.log(`pre-allocating ${MAX_TRANSFERS} transfers and commits...`)
  const transfers: Transfer[][] = []
  const commits: Transfer[][] = []

  let count = 0
  while (count < MAX_TRANSFERS) {
    const transferBatch: Transfer[] = []
    const commitBatch: Transfer[] = []
    for (let i = 0; i < MAX_REQUEST_BATCH_SIZE; i++) {
      if (count === MAX_TRANSFERS) break

      count += 1
      transferBatch.push({
        id: BigInt(count),
        debit_account_id: accountA.id,
        credit_account_id: accountB.id,
        pending_id: 0n,
        code: 1,
        ledger: 1,
        reserved: 0n,
        user_data: 0n,
        flags: IS_TWO_PHASE_COMMIT ? TransferFlags.pending : 0,
        amount: 1n,
        timeout: IS_TWO_PHASE_COMMIT ? BigInt(2e9) : 0n,
        timestamp: 0n,
      })
    
      if (IS_TWO_PHASE_COMMIT) {
        commitBatch.push({
          id: BigInt(MAX_TRANSFERS - count),
          debit_account_id: accountA.id,
          credit_account_id: accountB.id,
          pending_id: BigInt(count),
          code: 1,
          ledger: 1,
          reserved: 0n,
          user_data: 0n,
          flags: IS_TWO_PHASE_COMMIT ? TransferFlags.post_pending_transfer : 0,
          amount: 1n,
          timeout: IS_TWO_PHASE_COMMIT ? BigInt(2e9) : 0n,
          timestamp: 0n,
        })
      }
    }

    transfers.push(transferBatch)
    if (IS_TWO_PHASE_COMMIT) commits.push(commitBatch)
  }
  assert(count === MAX_TRANSFERS)

  console.log(`starting benchmark. MAX_TRANSFERS=${MAX_TRANSFERS} REQUEST_BATCH_SIZE=${MAX_REQUEST_BATCH_SIZE} NUMBER_OF_BATCHES=${transfers.length}`)
  let maxCreateTransfersLatency = 0
  let maxCommitTransfersLatency = 0
  const start = Date.now()

  for (let i = 0; i < transfers.length; i++) {
    const ms1 = Date.now()

    const transferResults = await client.createTransfers(transfers[i])
    assert(transferResults.length === 0)

    const ms2 = Date.now()
    const createTransferLatency = ms2 - ms1
    if (createTransferLatency > maxCreateTransfersLatency) {
      maxCreateTransfersLatency = createTransferLatency
    }

    if (IS_TWO_PHASE_COMMIT) {
      const commitResults = await client.createTransfers(commits[i])
      assert(commitResults.length === 0)

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

  const benchmark = IS_RAW_REQUEST ? await runBenchmarkRawRequest() : await runBenchmark()
  
  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  const result = Math.floor((1000 * MAX_TRANSFERS)/benchmark.ms)
  console.log("=============================")
  console.log(`${IS_TWO_PHASE_COMMIT ? 'two-phase ' : ''}transfers per second: ${result}`)
  console.log(`create transfers max p100 latency per 10 000 transfers = ${benchmark.maxCreateTransfersLatency}ms`)
  console.log(`commit transfers max p100 latency per 10 000 transfers = ${benchmark.maxCommitTransfersLatency}ms`)
  assert(accounts.length === 2)
  assert(accounts[0].debits_posted === BigInt(MAX_TRANSFERS))
  assert(accounts[1].credits_posted === BigInt(MAX_TRANSFERS))

  if (result < PREVIOUS_BENCHMARK * (100 - TOLERANCE)/100) {
    console.warn(`There has been a performance regression. Previous benchmark=${PREVIOUS_BENCHMARK}`)
  }
}

main().catch(error => { 
  console.log(error)
}).finally(async () => {
  await client.destroy()
})
