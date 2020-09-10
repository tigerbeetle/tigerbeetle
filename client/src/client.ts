import { EventEmitter } from 'events'
import assert from 'assert'
import { Socket } from 'net'
import { Logger } from 'pino'

import { ReserveCommand,
  NETWORK_HEADER,
  NETWORK_HEADER_ID_OFFSET,
  MAGIC, NETWORK_HEADER_MAGIC_OFFSET,
  NETWORK_HEADER_COMMAND_OFFSET,
  NETWORK_HEADER_SIZE_OFFSET,
  NETWORK_HEADER_CHECKSUM_DATA_OFFSET,
  NETWORK_HEADER_CHECKSUM_DATA,
  NETWORK_HEADER_CHECKSUM_META,
  NETWORK_HEADER_CHECKSUM_META_OFFSET,
  CommitCommand,
  CreateAccountCommand } from './types'
import { encodeReserveCommand, encodeCommitCommand, encodeCreateAccountCommand } from './utils/encoding'

// eslint-disable-next-line
const Crypto = require('@ronomon/crypto-async')

const COMMAND_CREATE_TRANSFERS = 1
const COMMAND_COMMIT_TRANSFERS = 2
const COMMAND_CREATE_ACCOUNTS = 3
const COMMAND_ACK = 4
class Batch {
  command: number
  jobs: Array<{ id: string, data: Buffer }> = []
  timeout: NodeJS.Timeout | number = 0
  timestamp: number = 0

  constructor (command: number) {
    this.command = command
  }
}

export interface TigerBeetleConfig {
  host: string
  port: number
  logger: Logger
  batchSize: number
  batchTimeoutWindow: number // in ms
}

export class TigerBeetle extends EventEmitter {
  private _host: string
  private _port: number
  private logger: Logger
  private _batchSize: number
  private _batchTimeoutWindow: number
  private _socket: Socket
  private _checksum = Buffer.alloc(32)
  private _inFlight = new Array<(data: Buffer) => void>()
  private _reserveCommands = new Batch(COMMAND_CREATE_TRANSFERS)
  private _commitCommands = new Batch(COMMAND_COMMIT_TRANSFERS)
  private _createAccountCommands = new Batch(COMMAND_CREATE_ACCOUNTS)

  constructor (config: TigerBeetleConfig) {
    super()
    this._host = config.host
    this._port = config.port
    this.logger = config.logger
    this._batchSize = config.batchSize
    this._batchTimeoutWindow = config.batchTimeoutWindow
  }

  async connect (): Promise<void> {
    this._socket = new Socket()
    this._socket.on('data', (data: Buffer): void => {
      this.logger.debug('received data from server %o', data)
      // TODO: use Tiger Beetle request id to match callbacks
      const callback = this._inFlight.shift()
      if (callback) {
        callback(data)
        return
      }

      this.logger.error('Received data but no inflight callback registered. data: %o', data)
    })
    this._socket.on('error', (error): void => {
      this.logger.error('Socket error: %o', error)
    })

    return new Promise((resolve): void => {
      this._socket.connect({ host: this._host, port: this._port }, (): void => {
        this.logger.info('Connected to server')
        resolve()
      })
    })
  }

  async disconnect (): Promise<void> {
    this._socket.destroy()
  }

  async encodeReserveCommand (command: ReserveCommand): Promise<void> {
    const data = encodeReserveCommand(command)
    return this._push(command.id, data, this._reserveCommands)
  }

  async encodeCommitCommand (command: CommitCommand): Promise<void> {
    const data = encodeCommitCommand(command)
    return this._push(command.id, data, this._commitCommands)
  }

  async encodeCreateAccountCommand (command: CreateAccountCommand): Promise<void> {
    const data = encodeCreateAccountCommand(command)
    return this._push(command.id, data, this._createAccountCommands)
  }

  private async _push (id: string, data: Buffer, batch: Batch): Promise<void> {
    this.logger.debug('pushing data onto batch: %d, command, id: %s', batch.command, id)
    batch.jobs.push({ id, data })
    const resolver: Promise<void> = new Promise((resolve, reject): void => {
      this.logger.debug('registering listener for event: %s', id)
      this.once(id, (error?: number): void => {
        if (error) {
          reject(new Error('Tiger Beetle failed to process the command. Error code:' + error.toString()))
        }
        resolve()
      })
    })

    if (batch.timeout === 0) {
      batch.timestamp = Date.now()
      batch.timeout = setTimeout(
        function (): void {
          this._execute(batch)
        }.bind(this),
        this._batchTimeoutWindow // Wait up to N ms for a batch to be collected...
      )
    } else if (batch.jobs.length > this._batchSize) {
      clearTimeout(batch.timeout as NodeJS.Timeout)
      this._execute(batch)
    }

    return resolver
  }

  private _execute (batch: Batch): void {
    // Cache reference to jobs array so we can reset the batch:
    const jobs = batch.jobs
    // Reset the batch to start collecting a new batch:
    // We collect the new batch while committing the previous batch to TigerBeetle.
    batch.jobs = []
    batch.timestamp = 0
    batch.timeout = 0

    // TODO Use another function to do this to avoid using batch.jobs by mistake:
    const command = batch.command
    assert(
      command === COMMAND_CREATE_TRANSFERS || command === COMMAND_COMMIT_TRANSFERS || command === COMMAND_CREATE_ACCOUNTS
    )
    const size = jobs[0].data.length
    const buffer = Buffer.alloc(jobs.length * size)
    let offset = 0
    for (var index = 0; index < jobs.length; index++) {
      var copied = jobs[index].data.copy(buffer, offset)
      assert(copied === size)
      offset += copied
    }
    assert(offset === buffer.length)
    const callback = (response: Buffer): void => {
      // TODO: handle server response. assuming success for now. response will contain an array with only command ids that had errors
      this.logger.debug('Received response from Tiger Beetle Server')
      jobs.forEach((job): void => {
        this.logger.debug('emitting job id: ', job.id)
        this.emit(job.id)
      })
    }
    this._send(command, buffer, callback.bind(this))
  }

  private _send (command: number, data: Buffer, callback: () => void): void{
    assert(this._socket, 'not connected.')
    const header = Buffer.alloc(NETWORK_HEADER)
    // TODO Use a stream cipher CPRNG to write a random header ID instead of 0:
    header.writeUIntBE(0, NETWORK_HEADER_ID_OFFSET, 6)
    MAGIC.copy(header, NETWORK_HEADER_MAGIC_OFFSET)
    header.writeUInt32BE(command, NETWORK_HEADER_COMMAND_OFFSET)
    header.writeUInt32BE(data.length, NETWORK_HEADER_SIZE_OFFSET)
    assert(
      Crypto.hash(
        'sha256',
        data,
        0,
        data.length,
        this._checksum,
        0
      ) === 32
    )
    assert(
      this._checksum.copy(
        header,
        NETWORK_HEADER_CHECKSUM_DATA_OFFSET,
        0,
        NETWORK_HEADER_CHECKSUM_DATA
      ) === 16
    )
    assert(
      Crypto.hash(
        'sha256',
        header,
        NETWORK_HEADER_CHECKSUM_DATA_OFFSET,
        NETWORK_HEADER - NETWORK_HEADER_CHECKSUM_META,
        this._checksum,
        0
      ) === 32
    )
    assert(
      this._checksum.copy(
        header,
        NETWORK_HEADER_CHECKSUM_META_OFFSET,
        0,
        NETWORK_HEADER_CHECKSUM_META
      ) === 16
    )

    this._socket.write(header)
    this._socket.write(data)
    this._inFlight.push(callback)
  }
}
