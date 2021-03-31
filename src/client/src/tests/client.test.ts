import createLogger from 'pino'
import { v4 } from 'uuid'
import { createMockServer } from './mockServer'
import { TigerBeetle } from '../client'
import { ReserveCommand, CreateAccountCommand, CommitCommand } from '../types'

describe('Client', (): void => {
  const sourceAccountId = Buffer.alloc(8)
  const targeteAccountId = Buffer.alloc(8)
  sourceAccountId.writeBigUInt64BE(BigInt(1))
  targeteAccountId.writeBigUInt64BE(BigInt(2))
  const logger = createLogger({
    name: 'Test Tiger-Beetle client',
    level: process.env.LOG_LEVEL || 'error'
  })
  const dataHandler = jest.fn()
  const server = createMockServer(dataHandler)
  let client: TigerBeetle
  beforeAll(async (): Promise<void> => {
    server.listen(3001)
  })

  afterAll(async (): Promise<void> => {
    server.close()
  })

  beforeEach(async (): Promise<void> => {
    client = new TigerBeetle({
      batchSize: 10,
      batchTimeoutWindow: 50,
      host: 'localhost',
      port: 3001,
      logger
    })
    await client.connect()
  })

  afterEach(async (): Promise<void> => {
    await client.disconnect()
  })

  test('can send a reserve command', async (): Promise<void> => {
    let dataReceivedByServer: Buffer = Buffer.alloc(0)
    dataHandler.mockImplementationOnce((data: Buffer): Buffer => {
      dataReceivedByServer = data
      // TODO: test server responses
      return Buffer.alloc(64)
    })
    const command: ReserveCommand = {
      id: Buffer.from(v4().replace(/[^a-fA-F0-9]/g, ''), 'hex'),
      source_account_id: sourceAccountId,
      target_account_id: targeteAccountId,
      amount: BigInt(10000),
      custom_1: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    await client.encodeReserveCommand(command)

    expect(dataReceivedByServer.length).toBe(64 + 128) // 64 byte header + 128 byte command
  })

  test('can send a commit command', async (): Promise<void> => {
    let dataReceivedByServer: Buffer = Buffer.alloc(0)
    dataHandler.mockImplementationOnce((data: Buffer): Buffer => {
      dataReceivedByServer = data
      // TODO: test server responses
      return Buffer.alloc(64)
    })
    const command: CommitCommand = {
      id: Buffer.from(v4().replace(/[^a-fA-F0-9]/g, ''), 'hex'),
      flags: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    await client.encodeCommitCommand(command)

    expect(dataReceivedByServer.length).toBe(64 + 80) // 64 byte header + 80 byte command
  })

  test('can send a create account command', async (): Promise<void> => {
    let dataReceivedByServer: Buffer = Buffer.alloc(0)
    dataHandler.mockImplementationOnce((data: Buffer): Buffer => {
      dataReceivedByServer = data
      // TODO: test server responses
      return Buffer.alloc(64)
    })
    const command: CreateAccountCommand = {
      id: Buffer.from(v4().replace(/[^a-fA-F0-9]/g, ''), 'hex'),
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    await client.encodeCreateAccountCommand(command)

    expect(dataReceivedByServer.length).toBe(64 + 128) // 64 byte header + 128 byte command
  })

  test('batches commands', async (): Promise<void> => {
    let dataReceivedByServer: Buffer = Buffer.alloc(0)
    dataHandler.mockImplementationOnce((data: Buffer): Buffer => {
      dataReceivedByServer = data
      // TODO: test server responses
      return Buffer.alloc(64)
    })
    const command: ReserveCommand = {
      id: Buffer.from(v4().replace(/[^a-fA-F0-9]/g, ''), 'hex'),
      source_account_id: sourceAccountId,
      target_account_id: targeteAccountId,
      amount: BigInt(10000)
    }

    await Promise.all([
      client.encodeReserveCommand(command), // server will handle idempotency
      client.encodeReserveCommand(command)
    ])

    expect(dataReceivedByServer.length).toBe(64 + 2 * 128) // 64 byte header + 2 * 128 byte command
  })

  test('sends batch when batch size has been reached', async (): Promise<void> => {
    jest.useFakeTimers()
    let dataReceivedByServer: Buffer = Buffer.alloc(0)
    dataHandler.mockImplementationOnce((data: Buffer): Buffer => {
      dataReceivedByServer = data
      // TODO: test server responses
      return Buffer.alloc(64)
    })
    const command: ReserveCommand = {
      id: Buffer.from(v4().replace(/[^a-fA-F0-9]/g, ''), 'hex'),
      source_account_id: sourceAccountId,
      target_account_id: targeteAccountId,
      amount: BigInt(10000)
    }

    const promises = []
    for (let i = 0; i < 10; i++) {
      promises.push(client.encodeReserveCommand({ ...command, id: Buffer.from(v4().replace(/[^a-fA-F0-9]/g, ''), 'hex') }))
    }
    await Promise.all(promises)

    expect(dataReceivedByServer.length).toBe(64 + 10 * 128)
  })

  test('sends batch when timeout window has been reached', async (): Promise<void> => {
    jest.useFakeTimers()
    let dataReceivedByServer: Buffer = Buffer.alloc(0)
    dataHandler.mockImplementationOnce((data: Buffer): Buffer => {
      dataReceivedByServer = data
      // TODO: test server responses
      return Buffer.alloc(64)
    })
    const command: ReserveCommand = {
      id: Buffer.from(v4().replace(/[^a-fA-F0-9]/g, ''), 'hex'),
      source_account_id: sourceAccountId,
      target_account_id: targeteAccountId,
      amount: BigInt(10000)
    }

    const promises = []
    for (let i = 0; i < 5; i++) {
      promises.push(client.encodeReserveCommand({ ...command, id: Buffer.from(v4().replace(/[^a-fA-F0-9]/g, ''), 'hex') }))
    }

    jest.runTimersToTime(51)
    await Promise.all(promises)

    expect(dataReceivedByServer.length).toBe(64 + 5 * 128)
  })
})
