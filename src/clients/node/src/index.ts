export * from './bindings'
import {
  Account,
  Transfer,
  CreateAccountsError,
  CreateTransfersError,
  Operation,
  GetAccountTransfers,
} from './bindings'

const binding: Binding = (() => {
  const { arch, platform } = process

  const archMap = {
    "arm64": "aarch64",
    "x64": "x86_64"
  }

  const platformMap = {
    "linux": "linux",
    "darwin": "macos",
    "win32" : "windows",
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
})()

export type Context = object // tb_client
export type AccountID = bigint // u128
export type TransferID = bigint // u128
export type Event = Account | Transfer | AccountID | TransferID | GetAccountTransfers
export type Result = CreateAccountsError | CreateTransfersError | Account | Transfer
export type ResultCallback = (error: Error | null, results: Result[] | null) => void

interface BindingInitArgs {
  cluster_id: bigint, // u128
  concurrency: number, // u32
  replica_addresses: Buffer,
}

interface Binding {
  init: (args: BindingInitArgs) => Context
  submit: (context: Context, operation: Operation, batch: Event[], callback: ResultCallback) => void
  deinit: (context: Context) => void,
}

export interface ClientInitArgs {
  cluster_id: bigint, // u128
  concurrency_max?: number, // u32
  replica_addresses: Array<string | number>,
}

export interface Client {
  createAccounts: (batch: Account[]) => Promise<CreateAccountsError[]>
  createTransfers: (batch: Transfer[]) => Promise<CreateTransfersError[]>
  lookupAccounts: (batch: AccountID[]) => Promise<Account[]>
  lookupTransfers: (batch: TransferID[]) => Promise<Transfer[]>
  getAccountTransfers: (filter: GetAccountTransfers) => Promise<Transfer[]>
  destroy: () => void
}

export function createClient (args: ClientInitArgs): Client {
  const concurrency_max_default = 256 // arbitrary
  const context = binding.init({
    cluster_id: args.cluster_id,
    concurrency: args.concurrency_max || concurrency_max_default,
    replica_addresses: Buffer.from(args.replica_addresses.join(',')),
  })

  const request = <T extends Result>(operation: Operation, batch: Event[]): Promise<T[]> => {
    return new Promise((resolve, reject) => {
      try {
        binding.submit(context, operation, batch, (error, result) => {
          if (error) {
            reject(error)
          } else if (result) {
            resolve(result as T[])
          } else {
            throw new Error("UB: Binding invoked callback without error or result")
          }
        })
      } catch (err) {
        reject(err)
      }
    })
  }

  return {
    createAccounts(batch) { return request(Operation.create_accounts, batch) },
    createTransfers(batch) { return request(Operation.create_transfers, batch) },
    lookupAccounts(batch) { return request(Operation.lookup_accounts, batch) },
    lookupTransfers(batch) { return request(Operation.lookup_transfers, batch) },
    getAccountTransfers(filter) { return request(Operation.get_account_transfers, [filter]) },
    destroy() { binding.deinit(context) },
  }
}
