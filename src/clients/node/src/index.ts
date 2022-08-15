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
  ledger: number // u32, ledger of value
  code: number // u16, A chart of accounts code describing the type of account (e.g. clearing, settlement)
  flags: number // u16
  debits_pending: bigint // u64
  debits_posted: bigint // u64
  credits_pending: bigint // u64
  credits_posted: bigint // u64
  timestamp: bigint // u64, Set this to 0n - the actual value will be set by TigerBeetle server
}

export enum AccountFlags {
  linked = (1 << 0),
  debits_must_not_exceed_credits = (1 << 1),
  credits_must_not_exceed_debits = (1 << 2)
}

export enum CreateAccountError {
  // ok = 0 (No Error)
  linked_event_failed = 1,

  reserved_flag,
  reserved_field,

  id_must_not_be_zero,
  id_must_not_be_int_max,
  ledger_must_not_be_zero,
  code_must_not_be_zero,

  mutually_exclusive_flags,

  overflows_debits,
  overflows_credits,

  exceeds_credits,
  exceeds_debits,

  exists_with_different_flags,
  exists_with_different_user_data,
  exists_with_different_ledger,
  exists_with_different_code,
  exists_with_different_debits_pending,
  exists_with_different_debits_posted,
  exists_with_different_credits_pending,
  exists_with_different_credits_posted,
  exists,
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
  reserved: bigint, // u128
  pending_id: bigint, // u128
  timeout: bigint, // u64, In nanoseconds.
  ledger: number // u32, The ledger of value.
  code: number, // u16, A user-defined accounting code to describe the type of transfer (e.g. settlement).
  flags: number, // u16
  amount: bigint, // u64,
  timestamp: bigint, // u64, Set this to 0n - the timestamp will be set by TigerBeetle.
}

export enum TransferFlags {
  linked = (1 << 0),
  pending = (1 << 1),
  post_pending_transfer = (1 << 2),
  void_pending_transfer = (1 << 3)
}

export enum CreateTransferError {
  // ok = 0 (No Error)
  linked_event_failed = 1,

  reserved_flag,
  reserved_field,

  id_must_not_be_zero,
  id_must_not_be_int_max,
  debit_account_id_must_not_be_zero,
  debit_account_id_must_not_be_int_max,
  credit_account_id_must_not_be_zero,
  credit_account_id_must_not_be_int_max,
  accounts_must_be_different,

  pending_id_must_be_zero,
  pending_transfer_must_timeout,

  ledger_must_not_be_zero,
  code_must_not_be_zero,
  amount_must_not_be_zero,

  debit_account_not_found,
  credit_account_not_found,

  accounts_must_have_the_same_ledger,
  transfer_must_have_the_same_ledger_as_accounts,

  exists_with_different_flags,
  exists_with_different_debit_account_id,
  exists_with_different_credit_account_id,
  exists_with_different_user_data,
  exists_with_different_pending_id,
  exists_with_different_timeout,
  exists_with_different_code,
  exists_with_different_amount,
  exists,

  overflows_debits_pending,
  overflows_credits_pending,
  overflows_debits_posted,
  overflows_credits_posted,
  overflows_debits,
  overflows_credits,

  exceeds_credits,
  exceeds_debits,

  cannot_post_and_void_pending_transfer,
  pending_transfer_cannot_post_or_void_another,
  timeout_reserved_for_pending_transfer,

  pending_id_must_not_be_zero,
  pending_id_must_not_be_int_max,
  pending_id_must_be_different,

  pending_transfer_not_found,
  pending_transfer_not_pending,

  pending_transfer_has_different_debit_account_id,
  pending_transfer_has_different_credit_account_id,
  pending_transfer_has_different_ledger,
  pending_transfer_has_different_code,

  exceeds_pending_transfer_amount,
  pending_transfer_has_different_amount,

  pending_transfer_already_posted,
  pending_transfer_already_voided,

  pending_transfer_expired,
}

export type CreateTransfersError = {
  index: number,
  code: CreateTransferError,
}

export type AccountID = bigint // u128
export type TransferID = bigint // u128

export type Event = Account | Transfer | AccountID | TransferID
export type Result = CreateAccountsError | CreateTransfersError | Account | Transfer
export type ResultCallback = (error: undefined | Error, results: Result[]) => void

export enum Operation {
  CREATE_ACCOUNT = 3,
  CREATE_TRANSFER,
  ACCOUNT_LOOKUP,
  TRANSFER_LOOKUP
}

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
        binding.request(context, Operation.CREATE_ACCOUNT, batch, callback)
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
        binding.request(context, Operation.CREATE_TRANSFER, batch, callback)
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
        binding.request(context, Operation.ACCOUNT_LOOKUP, batch, callback)
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
        binding.request(context, Operation.TRANSFER_LOOKUP, batch, callback)
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
