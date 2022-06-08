import assert from 'assert'
import {
  Account,
  createClient,
  Transfer,
  TransferFlags,
  CreateTransfersError,
  Operation
} from '.'

const MAX_TRANSFERS = 51200
const MAX_REQUEST_BATCH_SIZE = 5120
const IS_TWO_PHASE_TRANSFER = false
const IS_RAW_REQUEST = false
const PREVIOUS_RAW_REQUEST_RESULT = IS_TWO_PHASE_TRANSFER ? 300000 : 620000
const PREVIOUS_RESULT = IS_TWO_PHASE_TRANSFER ? 150000 : 310000
const PREVIOUS_BENCHMARK = IS_RAW_REQUEST ? PREVIOUS_RAW_REQUEST_RESULT : PREVIOUS_RESULT
const TOLERANCE = 10 // percent that the benchmark is allowed to deviate from the previous benchmark

const client = createClient({
  cluster_id: 0,
  replica_addresses: ['3001']
})

const TRANSFER_SIZE = 128
const Zeroed48Bytes = Buffer.alloc(48, 0)
const accountA: Account = {
  id: 137n,
  user_data: 0n,
  reserved: Zeroed48Bytes,
  ledger: 1,
  code: 1,
  flags: 0,
  debits_pending: 0n,
  debits_posted: 0n,
  credits_pending: 0n,
  credits_posted: 0n,
  timestamp: 0n,
}

const accountB: Account = {
  id: 138n,
  user_data: 0n,
  reserved: Zeroed48Bytes,
  ledger: 1,
  code: 1,
  flags: 0,
  debits_pending: 0n,
  debits_posted: 0n,
  credits_pending: 0n,
  credits_posted: 0n,
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
 * ID_OFFSET                    = 0                 (0->16)
 * DEBIT_ACCOUNT_ID_OFFSET      = 0   + 16 = 16     (16-32)
 * CREDIT_ACCOUNT_ID_OFFSET     = 16  + 16 = 32     (32-48)
 * USER_DATA_OFFSET             = 32  + 16 = 48     (48-64)
 * RESERVED_OFFSET              = 48  + 16 = 64     (64-80)
 * PENDING_ID_OFFSET            = 64  + 16 = 80     (80-96)
 * TIMEOUT_OFFSET               = 96  + 8  = 104    (96-104)
 * LEDGER_OFFSET                = 104 + 4  = 108    (104-108)
 * CODE_OFFSET                  = 108 + 2  = 110    (108-110)
 * FLAGS_OFFSET                 = 110 + 2  = 112    (110-112)
 * AMOUNT_OFFSET                = 112 + 8  = 120    (112-120)
 * TIMESTAMP                    = 120 + 8  = 128    (120-128)
 */ 
const encodeTransfer = (transfer: Transfer, offset: number, output: Buffer): void => {
  //console.log(`offset = ${(offset + TRANSFER_SIZE)} out = ${output.length}`);
  assert(BigInt((offset + TRANSFER_SIZE)) <= BigInt(output.length), `Transfer ${transfer} exceeds buffer of ${output}!`)

  output.writeBigUInt64LE(transfer.id, offset)
  output.writeBigUInt64LE(transfer.debit_account_id, offset + 16)
  output.writeBigUInt64LE(transfer.credit_account_id, offset + 32)
  output.writeBigUInt64LE(transfer.user_data, offset + 48)
  output.writeBigUInt64LE(transfer.reserved, offset + 64)
  output.writeBigUInt64LE(transfer.pending_id, offset + 80)
  output.writeBigUInt64LE(transfer.timeout, offset + 96)
  output.writeUInt32LE(transfer.ledger, offset + 104)
  output.writeUInt32LE(transfer.code, offset + 108)
  output.writeUInt32LE(transfer.flags, offset + 110)
  output.writeBigUInt64LE(transfer.amount, offset + 112)
  output.writeBigUInt64LE(transfer.timestamp, offset + 120)
}

const runBenchmarkRawRequest = async () => {
  assert(
    MAX_TRANSFERS % MAX_REQUEST_BATCH_SIZE === 0,
    "The raw request benchmark requires MAX_TRANSFERS to be a multiple of MAX_REQUEST_BATCH_SIZE"
  )
  console.log(`pre-allocating ${MAX_TRANSFERS} transfers and posts...`)
  const transfers: Buffer[] = []
  const posts: Buffer[] = []

  let count = 0
  while (count < MAX_TRANSFERS) {
    const transferBatch = Buffer.alloc(MAX_REQUEST_BATCH_SIZE * TRANSFER_SIZE, 0)
    const postTransferBatch = Buffer.alloc(MAX_REQUEST_BATCH_SIZE * TRANSFER_SIZE, 0)
    for (let i = 0; i < MAX_REQUEST_BATCH_SIZE; i++) {
      if (count === MAX_TRANSFERS) break

      count += 1
      encodeTransfer(
        {
          id: BigInt(count),
          debit_account_id: accountA.id,
          credit_account_id: accountB.id,
          user_data: 0n,
          reserved: 0n,
          pending_id: 0n,
          timeout: IS_TWO_PHASE_TRANSFER ? BigInt(2e9) : 0n,
          ledger: 1,
          code: 1,
          flags: IS_TWO_PHASE_TRANSFER ? TransferFlags.pending : 0,
          amount: 1n,
          timestamp: 0n,
        },
        i * TRANSFER_SIZE,
        transferBatch
      )
    
      if (IS_TWO_PHASE_TRANSFER) {
        encodeTransfer(
          {
            id: BigInt((MAX_TRANSFERS + count)),
            debit_account_id: accountA.id,
            credit_account_id: accountB.id,
            user_data: 0n,
            reserved: 0n,
            pending_id: BigInt(count),
            timeout: 0n,
            ledger: 1,
            code: 1,
            flags: TransferFlags.post_pending_transfer,
            amount: 1n,
            timestamp: 0n,
          },
          i * TRANSFER_SIZE,
          postTransferBatch
        )
      }
    }

    transfers.push(transferBatch)
    if (IS_TWO_PHASE_TRANSFER) posts.push(postTransferBatch)
    if (count % 100) console.log(`${Number((count / MAX_TRANSFERS) * 100).toFixed(1)}%`)
  }
  console.log(`.`)
  assert(count === MAX_TRANSFERS)

  console.log(`starting benchmark. MAX_TRANSFERS=${MAX_TRANSFERS} REQUEST_BATCH_SIZE=${MAX_REQUEST_BATCH_SIZE} NUMBER_OF_BATCHES=${transfers.length}`)
  let maxCreateTransfersLatency = 0
  let maxCommitTransfersLatency = 0
  const start = Date.now()

  for (let i = 0; i < transfers.length; i++) {
    const ms1 = Date.now()

    const transferErrors = await rawCreateTransfers(transfers[i])
    assert(transferErrors.length === 0)

    const ms2 = Date.now()
    const createTransferLatency = ms2 - ms1
    if (createTransferLatency > maxCreateTransfersLatency) {
      maxCreateTransfersLatency = createTransferLatency
    }

    if (IS_TWO_PHASE_TRANSFER) {
      const commitErrors = await rawCreateTransfers(posts[i])
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
        pending_id: 0n,
        code: 1,
        ledger: 1,
        reserved: 0n,
        user_data: 0n,
        flags: IS_TWO_PHASE_TRANSFER ? TransferFlags.pending : 0,
        amount: 1n,
        timeout: IS_TWO_PHASE_TRANSFER ? BigInt(2e9) : 0n,
        timestamp: 0n,
      })
    
      if (IS_TWO_PHASE_TRANSFER) {
        postBatch.push({
          id: BigInt(MAX_TRANSFERS + count),
          debit_account_id: accountA.id,
          credit_account_id: accountB.id,
          pending_id: BigInt(count),
          code: 1,
          ledger: 1,
          reserved: 0n,
          user_data: 0n,
          flags: IS_TWO_PHASE_TRANSFER ? TransferFlags.post_pending_transfer : 0,
          amount: 1n,
          timeout: 0n,
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

  const benchmark = IS_RAW_REQUEST ? await runBenchmarkRawRequest() : await runBenchmark()
  
  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  const result = Math.floor((1000 * MAX_TRANSFERS)/benchmark.ms)
  console.log("=============================")
  console.log(`${IS_TWO_PHASE_TRANSFER ? 'two-phase ' : ''}transfers per second: ${result}`)
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
