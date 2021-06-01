import { CommitFlags, CommitTransfer, CreateAccount, createClient, CreateTransfer } from '.'

const REQUEST_BATCH_SIZE = 10000
const MAX_TRANSFERS = 600000

const client = createClient({
  cluster_id: 0x0a5ca1ab1ebee11en,
  replica_addresses: ['3001']
})

const accountA: CreateAccount = {
  id: 137n,
  user_data: 0n,
  reserved: Buffer.alloc(48, 0),
  code: 0,
  flags: 0,
  unit: 1,
  debits_accepted: 0n,
  debits_reserved: 0n,
  credits_accepted: 0n,
  credits_reserved: 0n,
  timestamp: 0n,
}

const accountB: CreateAccount = {
  id: 138n,
  user_data: 0n,
  reserved: Buffer.alloc(48, 0),
  code: 0,
  flags: 0,
  unit: 1,
  debits_accepted: 0n,
  debits_reserved: 0n,
  credits_accepted: 0n,
  credits_reserved: 0n,
  timestamp: 0n,
}

// pre-allocate 1 million transfers and commits
console.log(`pre-allocating ${MAX_TRANSFERS} transfers and commits...`)
const transfers: CreateTransfer[] = []
const commits: CommitTransfer[] = []

for (let i = 0; i < MAX_TRANSFERS; i++) {
  transfers.push({
    id: BigInt(i),
    debit_account_id: accountA.id,
    credit_account_id: accountB.id,
    code: 0,
    reserved: Buffer.alloc(32, 0),
    user_data: 0n,
    flags: 0,
    amount: 1n,
    timeout: 0n,
  })

  commits.push({
    id: BigInt(i),
    reserved: Buffer.alloc(32, 0),
    code: 0,
    flags: 0,
  })
}

const main = async () => {
  console.log("creating the accounts...")
  await client.createAccounts([accountA, accountB])
  
  console.log(`starting benchmark. MAX_TRANSFERS=${MAX_TRANSFERS} REQUEST_BATCH_SIZE=${REQUEST_BATCH_SIZE}`)
  let maxCreateTransfersLatency = 0
  let maxCommitTransfersLatency = 0
  const start = new Date().getTime()

  let offset = 0
  while (offset < transfers.length) {
    const ms1 = new Date().getTime()

    // create the batch
    const transferBatch = transfers.slice(offset, offset + REQUEST_BATCH_SIZE)
    await client.createTransfers(transferBatch)

    const ms2 = new Date().getTime()
    const createTransferLatency = ms2 - ms1
    if (createTransferLatency > maxCreateTransfersLatency) {
      maxCreateTransfersLatency = createTransferLatency
    }

    // commit the batch
    const commitBatch = commits.slice(offset, offset + REQUEST_BATCH_SIZE)
    await client.commitTransfers(commitBatch)

    const ms3 = new Date().getTime()
    const commitTransferLatency = ms3 - ms2
    if (commitTransferLatency > maxCommitTransfersLatency) {
      maxCommitTransfersLatency = commitTransferLatency
    }

    offset += transferBatch.length
    if (offset % 100000 === 0) {
      console.log(`${offset} transfers...`)
    }
  }

  const ms = new Date().getTime() - start
  const accounts = await client.lookupAccounts([accountA.id, accountB.id])
  console.log(accounts)
  console.log("=============================")
  console.log(`transfers per second: ${Math.floor((1000 * transfers.length)/ms)}`)
  console.log(`create transfers max p100 latency per 10 000 transfers = ${maxCreateTransfersLatency}ms`)
  console.log(`commit transfers max p100 latency per 10 000 transfers = ${maxCommitTransfersLatency}ms`)

  await client.destroy()
}

main().catch(error => { console.log(error) })