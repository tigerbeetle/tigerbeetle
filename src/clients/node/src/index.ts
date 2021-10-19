const binding: Binding = require('./client.node')
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

export type Account = {
  id: bigint // u128
  user_data: bigint // u128
  reserved: Buffer // [48]u8
  unit: number // u16, unit of value
  code: number // u16, A chart of accounts code describing the type of account (e.g. clearing, settlement)
  flags: number // u32
  debits_reserved: bigint // u64
  debits_accepted: bigint // u64
  credits_reserved: bigint // u64
  credits_accepted: bigint // u64
  timestamp: bigint // u64, Set this to 0n - the actual value will be set by TigerBeetle server
}

export enum AccountFlags {
  linked = (1 << 0),
  debits_must_not_exceed_credits = (1 << 1),
  credits_must_not_exceed_debits = (1 << 2)
}

export enum CreateAccountError {
  linked_event_failed = 1,
  exists,
  exists_with_different_user_data,
  exists_with_different_reserved_field,
  exists_with_different_unit,
  exists_with_different_code,
  exists_with_different_flags,
  exceeds_credits,
  exceeds_debits,
  reserved_field,
  reserved_flag_padding,
}

export type CreateAccountsError = {
  index: number,
  code: CreateAccountError,
}

export type Transfer = {
  id: bigint, // u128
  debit_account_id: bigint, // u128
  credit_account_id: bigint, // u128
  user_data: bigint, // u128
  reserved: Buffer, // [32]u8
  timeout: bigint, // u64, in nano-seconds
  code: number, // u32 accounting system code to describe the type of transfer (e.g. settlement)
  flags: number, // u32
  amount: bigint, // u64,
  timestamp: bigint, // u64, Set this to 0n - the actual value will be set by TigerBeetle server
}

export enum TransferFlags {
  linked = (1 << 0),
  two_phase_commit = (1 << 1),
  condition = (1 << 2) // whether or not a condition will be supplied
}

export enum CreateTransferError {
  linked_event_failed = 1,
  exists,
  exists_with_different_debit_account_id,
  exists_with_different_credit_account_id,
  exists_with_different_user_data,
  exists_with_different_reserved_field,
  exists_with_different_code,
  exists_with_different_amount,
  exists_with_different_timeout,
  exists_with_different_flags,
  exists_and_already_committed_and_accepted,
  exists_and_already_committed_and_rejected,
  reserved_field,
  reserved_flag_padding,
  debit_account_not_found,
  credit_account_not_found,
  accounts_are_the_same,
  accounts_have_different_units,
  amount_is_zero,
  exceeds_credits,
  exceeds_debits,
  two_phase_commit_must_timeout,
  timeout_reserved_for_two_phase_commit,
}

export type CreateTransfersError = {
  index: number,
  code: CreateTransferError,
}

export type Commit = {
  id: bigint, // u128
  reserved: Buffer, // [32]u8
  code: number, // u32 accounting system code describing the reason for accept/reject
  flags: number, // u32
  timestamp: bigint, // u64, Set this to 0n - the actual value will be set by TigerBeetle server
}

export enum CommitFlags {
  linked = (1 << 0),
  reject = (1 << 1),
  preimage = (1 << 2) // whether or not a pre-image will be supplied
}

export enum CommitTransferError {
  linked_event_failed = 1,
  reserved_field,
  reserved_flag_padding,
  transfer_not_found,
  transfer_not_two_phase_commit,
  transfer_expired,
  already_committed,
  already_committed_but_accepted,
  already_committed_but_rejected,
  debit_account_not_found,
  credit_account_not_found,
  debit_amount_was_not_reserved,
  credit_amount_was_not_reserved,
  exceeds_credits,
  exceeds_debits,
  condition_requires_preimage,
  preimage_requires_condition,
  preimage_invalid,
}

export type CommitTransfersError = {
  index: number,
  code: CommitTransferError,
}

export type AccountID = bigint // u128

export type Event = Account | Transfer | Commit | AccountID
export type Result = CreateAccountsError | CreateTransfersError | CommitTransfersError | Account
export type ResultCallback = (error: undefined | Error, results: Result[]) => void

export enum Operation {
  CREATE_ACCOUNT = 3,
  CREATE_TRANSFER,
  COMMIT_TRANSFER,
  ACCOUNT_LOOKUP
}

export interface Client {
  createAccounts: (batch: Account[]) => Promise<CreateAccountsError[]>
  createTransfers: (batch: Transfer[]) => Promise<CreateTransfersError[]>
  commitTransfers: (batch: Commit[]) => Promise<CommitTransfersError[]>
  lookupAccounts: (batch: AccountID[]) => Promise<Account[]>
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

  return args.cluster_id === _args.cluster_id &&
          isSameReplicas
}

let _client: Client | undefined = undefined
let _interval: NodeJS.Timeout | undefined = undefined
// here to wait until  `ping` is sent to server so that connection is registered - temporary till client table and sessions are implemented.
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
    // here to wait until  `ping` is sent to server so that connection is registered - temporary till client table and sessions are implemented.
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
        }
        resolve(results)
      }

      try {
        binding.request(context, Operation.CREATE_ACCOUNT, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const createTransfers = async (batch: Transfer[]): Promise<CreateTransfersError[]> => {
    // here to wait until  `ping` is sent to server so that connection is registered - temporary till client table and sessions are implemented.
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
        }
        resolve(results)
      }

      try {
        binding.request(context, Operation.CREATE_TRANSFER, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const commitTransfers = async (batch: Commit[]): Promise<CommitTransfersError[]> => {
    // here to wait until  `ping` is sent to server so that connection is registered - temporary till client table and sessions are implemented.
    if (!_pinged) {
      await new Promise<void>(resolve => {
        setTimeout(() => {
          _pinged = true
          resolve()
        }, 600)
      })
    }
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: CommitTransfersError[]) => {
        if (error) {
          reject(error)
        }
        resolve(results)
      }

      try {
        binding.request(context, Operation.COMMIT_TRANSFER, batch, callback)
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
        }
        resolve(results)
      }

      try {
        binding.request(context, Operation.ACCOUNT_LOOKUP, batch, callback)
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
    commitTransfers,
    lookupAccounts,
    request,
    rawRequest,
    destroy
  }

  _interval = setInterval(() => {
    binding.tick(context)
  }, binding.tick_ms)

  return _client
}
