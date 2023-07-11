export * from './bindings'
import {
  Account,
  Transfer,
  CreateAccountsError,
  CreateTransfersError,
  Operation,
} from './bindings'

function getBinding (): Binding {
  const { arch, platform } = process

  const archMap = {
    "arm64": "aarch64",
    "x64": "x86_64"
  }

  const platformMap = {
    "linux": "linux",
    "darwin": "macos"
  }

  if (! (arch in archMap)) {
    throw new Error(`Unsupported arch: ${arch}`)
  }

  if (! (platform in platformMap)) {
    throw new Error(`Unsupported platform: ${platform}`)
  }

  let extra = ''

  /**
   * We need to detect during runtime which libc we're running on to load the correct NAPI.
   * binary.
   *
   * Rationale: The /proc/self/map_files/ subdirectory contains entries corresponding to
   * memory-mapped files loaded by Node.
   * https://man7.org/linux/man-pages/man5/proc.5.html: We detect a musl-based distro by
   * checking if any library contains the name "musl".
   *
   * Prior art: https://github.com/xerial/sqlite-jdbc/issues/623
   */

  const fs = require('fs')
  const path = require('path')

  if (platform === 'linux') {
    extra = '-gnu'

    for (const file of fs.readdirSync("/proc/self/map_files/")) {
      const realPath = fs.readlinkSync(path.join("/proc/self/map_files/", file))
      if (realPath.includes('musl')) {
        extra = '-musl'
        break
      }
    }
  }

  const filename = `./bin/${archMap[arch]}-${platformMap[platform]}${extra}/client.node`
  return require(filename)
}

const binding = getBinding()

interface Binding {
  init: (args: BindingInitArgs) => Context
  request: (context: Context, operation: Operation, batch: Event[], result: ResultCallback) => void
  raw_request: (context: Context, operation: Operation, raw_batch: Buffer, result: ResultCallback) => void
  tick: (context: Context) => void,
  deinit: (context: Context) => void,
  tick_ms: number
}

interface BindingInitArgs {
  cluster_id: number, // u32
  replica_addresses: Buffer,
}

export interface InitArgs {
  cluster_id: number, // u32
  replica_addresses: Array<string | number>,
}

export type Context = object

export type AccountID = bigint // u128
export type TransferID = bigint // u128

export type Event = Account | Transfer | AccountID | TransferID
export type Result = CreateAccountsError | CreateTransfersError | Account | Transfer
// Note: as of #990, the error is always `undefined` here.
export type ResultCallback = (error: undefined | Error, results: Result[]) => void

export interface Client {
  createAccounts: (batch: Account[]) => Promise<CreateAccountsError[]>
  createTransfers: (batch: Transfer[]) => Promise<CreateTransfersError[]>
  lookupAccounts: (batch: AccountID[]) => Promise<Account[]>
  lookupTransfers: (batch: TransferID[]) => Promise<Transfer[]>
  request: (operation: Operation, batch: Event[], callback: ResultCallback) => void
  rawRequest: (operation: Operation, rawBatch: Buffer, callback: ResultCallback) => void
  destroy: () => void
}

let _args: InitArgs | undefined = undefined
const isSameArgs = (args: InitArgs): boolean => {
  if (typeof _args === 'undefined') {
    return false
  }

  if (_args.replica_addresses.length !== args.replica_addresses.length) {
    return false
  }

  let isSameReplicas = true
  args.replica_addresses.forEach((entry, index) => {
    if (_args?.replica_addresses[index] !== entry) {
      isSameReplicas = false
    }
  })

  return args.cluster_id === _args.cluster_id && isSameReplicas
}

let _client: Client | undefined = undefined
let _interval: NodeJS.Timeout | undefined = undefined
// Here to wait until  `ping` is sent to server so that connection is registered - temporary till client table and sessions are implemented.
let _pinged = false
// TODO: allow creation of clients if the arguments are different. Will require changes in node.zig as well.
export function createClient (args: InitArgs): Client {
  const duplicateArgs = isSameArgs(args)
  if (!duplicateArgs && typeof _client !== 'undefined'){
    throw new Error('Client has already been initialized with different arguments.')
  }

  if (duplicateArgs && typeof _client !== 'undefined'){
    throw new Error('Client has already been initialized with the same arguments.')
  }

  _args = Object.assign({}, { ...args })
  const context = binding.init({
    ...args,
    replica_addresses: Buffer.from(args.replica_addresses.join(','))
  })

  const request = (operation: Operation, batch: Event[], callback: ResultCallback) => {
    binding.request(context, operation, batch, callback)
  }

  const rawRequest = (operation: Operation, rawBatch: Buffer, callback: ResultCallback) => {
    binding.raw_request(context, operation, rawBatch, callback)
  }

  const createAccounts = async (batch: Account[]): Promise<CreateAccountsError[]> => {
    // Here to wait until `ping` is sent to server so that connection is registered - temporary till client table and sessions are implemented.
    if (!_pinged) {
      await new Promise<void>(resolve => {
        setTimeout(() => {
          _pinged = true
          resolve()
        }, 600)
      })
    }
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: CreateAccountsError[]) => {
        if (error) {
          reject(error)
          return
        }
        resolve(results)
      }

      try {
        binding.request(context, Operation.create_accounts, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const createTransfers = async (batch: Transfer[]): Promise<CreateTransfersError[]> => {
    // Here to wait until `ping` is sent to server so that connection is registered - temporary till client table and sessions are implemented.
    if (!_pinged) {
      await new Promise<void>(resolve => {
        setTimeout(() => {
          _pinged = true
          resolve()
        }, 600)
      })
    }
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: CreateTransfersError[]) => {
        if (error) {
          reject(error)
          return
        }
        resolve(results)
      }

      try {
        binding.request(context, Operation.create_transfers, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const lookupAccounts = async (batch: AccountID[]): Promise<Account[]> => {
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: Account[]) => {
        if (error) {
          reject(error)
          return
        }
        resolve(results)
      }

      try {
        binding.request(context, Operation.lookup_accounts, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const lookupTransfers = async (batch: TransferID[]): Promise<Transfer[]> => {
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: Transfer[]) => {
        if (error) {
          reject(error)
          return
        }
        resolve(results)
      }

      try {
        binding.request(context, Operation.lookup_transfers, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const destroy = (): void => {
    binding.deinit(context)
    if (_interval){
      clearInterval(_interval)
    }
    _client = undefined
  }

  _client = {
    createAccounts,
    createTransfers,
    lookupAccounts,
    lookupTransfers,
    request,
    rawRequest,
    destroy
  }

  _interval = setInterval(() => {
    binding.tick(context)
  }, binding.tick_ms)

  return _client
}
