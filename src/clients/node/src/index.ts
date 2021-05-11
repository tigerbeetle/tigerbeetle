const binding: Binding = require('./client.node')
interface Binding {
  init: (args: BindingInitArgs) => Context
  batch: (context: Context, operation: Operation, batch: Command[], result: ResultCallback) => void
  tick: (context: Context) => void,
  deinit: (context: Context) => void
}

interface BindingInitArgs {
  client_id: bigint, // u128
  cluster_id: bigint, // u128
  replica_addresses: Buffer,
}

export interface InitArgs {
  client_id: bigint, // u128
  cluster_id: bigint, // u128
  replica_addresses: Array<string | number>,
}

export type Context = object

export type Account = {
  id: bigint // u128
  custom: bigint // u128
  flags: bigint // u64
  unit: bigint // u64, unit of value
  debit_accepted: bigint // u64
  debit_reserved: bigint // u64
  credit_accepted: bigint // u64
  credit_reserved: bigint // u64
  debit_accepted_limit: bigint // u64
  debit_reserved_limit: bigint // u64
  credit_accepted_limit: bigint // u64
  credit_reserved_limit: bigint // u64
}

export type CreateAccount = Account & {
  timestamp: bigint // u64
}

export enum CreateAccountError {
    exists = 1,
    exists_with_different_unit,
    exists_with_different_limits,
    exists_with_different_custom_field,
    exists_with_different_flags,
    reserved_field_custom,
    reserved_field_padding,
    reserved_field_timestamp,
    reserved_flag_padding,
    exceeds_debit_reserved_limit,
    exceeds_debit_accepted_limit,
    exceeds_credit_reserved_limit,
    exceeds_credit_accepted_limit,
    debit_reserved_limit_exceeds_debit_accepted_limit,
    credit_reserved_limit_exceeds_credit_accepted_limit,
}

export type CreateAccountResult = {
  index: number,
  error: CreateAccountError,
}

export type CreateTransfer = {
  id: bigint, // u128
  debit_account_id: bigint, // u128
  credit_account_id: bigint, // u128
  custom_1: bigint, // u128
  custom_2: bigint, // u128
  custom_3: bigint, // u128
  flags: bigint, // u64
  amount: bigint, // u64
  timeout: bigint, // u64, in nano-seconds
}

export enum CreateTransferFlags {
  accept = (1 << 0),
  reject = (1 << 1),
  auto_commit = (1 << 2)
}

export enum CreateTransferError {
  exists = 1,
  exists_with_different_debit_account_id,
  exists_with_different_credit_account_id,
  exists_with_different_custom_fields,
  exists_with_different_amount,
  exists_with_different_timeout,
  exists_with_different_flags,
  exists_and_already_committed_and_accepted,
  exists_and_already_committed_and_rejected,
  reserved_field_custom,
  reserved_field_timestamp,
  reserved_flag_padding,
  reserved_flag_accept,
  reserved_flag_reject,
  debit_account_not_found,
  credit_account_not_found,
  accounts_are_the_same,
  accounts_have_different_units,
  amount_is_zero,
  exceeds_debit_reserved_limit,
  exceeds_debit_accepted_limit,
  exceeds_credit_reserved_limit,
  exceeds_credit_accepted_limit,
  auto_commit_must_accept,
  auto_commit_cannot_timeout,
}

export type CreateTransferResult = {
  index: number,
  error: CreateTransferError,
}

export type CommitTransfer = {
  id: bigint, // u128
  custom_1: bigint, // u128
  custom_2: bigint, // u128
  custom_3: bigint, // u128
  flags: bigint, // u64
}

export enum CommitFlags {
  accept = (1 << 0),
  reject = (1 << 1)
}

export enum CommitTransferError {
  reserved_field_custom = 1,
  reserved_field_timestamp,
  reserved_flag_padding,
  commit_must_accept_or_reject,
  commit_cannot_accept_and_reject,
  transfer_not_found,
  transfer_expired,
  already_auto_committed,
  already_committed,
  already_committed_but_accepted,
  already_committed_but_rejected,
  debit_account_not_found,
  credit_account_not_found,
  debit_amount_was_not_reserved,
  credit_amount_was_not_reserved,
  exceeds_debit_accepted_limit,
  exceeds_credit_accepted_limit,
  condition_requires_preimage,
  preimage_requires_condition,
  preimage_invalid,
}

export type CommitTransferResult = {
  index: number,
  error: CommitTransferError,
}

export type AccountLookup = bigint // u128

export enum AccountLookupError {
  not_found,
}

export type AccountLookupResult = Account | {
  index: number,
  error: AccountLookupError
}

export type Command = CreateAccount | CreateTransfer | CommitTransfer | AccountLookup
export type Result = CreateAccountResult | CreateTransferResult | CommitTransferResult | AccountLookupResult
export type ResultCallback = (error: undefined | Error, results: Result[]) => void

export enum Operation {
  CREATE_ACCOUNT = 2,
  CREATE_TRANSFER,
  COMMIT_TRANSFER,
  ACCOUNT_LOOKUP
}

export interface Client {
  createAccounts: (batch: CreateAccount[]) => Promise<CreateAccountResult[]>
  createTransfers: (batch: CreateTransfer[]) => Promise<CreateTransferResult[]>
  commitTransfers: (batch: CommitTransfer[]) => Promise<CommitTransferResult[]>
  lookupAccounts: (batch: AccountLookup[]) => Promise<AccountLookupResult[]>
  batch: (operation: Operation, batch: Command[], callback: ResultCallback) => void
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

  return args.client_id === _args.client_id &&
          args.cluster_id === _args.cluster_id &&
          isSameReplicas
}

let _client: Client | undefined = undefined
let _interval: NodeJS.Timeout | undefined = undefined
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

  const batch = (operation: Operation, batch: Command[], callback: ResultCallback) => {
    binding.batch(context, operation, batch, callback)
  }

  const createAccounts = async (batch: CreateAccount[]): Promise<CreateAccountResult[]> => {
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: CreateAccountResult[]) => {
        if (error) {
          reject(error)
        }
        resolve(results)
      }

      try {
        binding.batch(context, Operation.CREATE_ACCOUNT, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const createTransfers = async (batch: CreateTransfer[]): Promise<CreateTransferResult[]> => {
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: CreateTransferResult[]) => {
        if (error) {
          reject(error)
        }
        resolve(results)
      }

      try {
        binding.batch(context, Operation.CREATE_TRANSFER, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const commitTransfers = async (batch: CommitTransfer[]): Promise<CommitTransferResult[]> => {
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: CommitTransferResult[]) => {
        if (error) {
          reject(error)
        }
        resolve(results)
      }

      try {
        binding.batch(context, Operation.COMMIT_TRANSFER, batch, callback)
      } catch (error) {
        reject(error)
      }
    })
  }

  const lookupAccounts = async (batch: AccountLookup[]): Promise<AccountLookupResult[]> => {
    return new Promise((resolve, reject) => {
      const callback = (error: undefined | Error, results: AccountLookupResult[]) => {
        if (error) {
          reject(error)
        }
        resolve(results)
      }

      try {
        binding.batch(context, Operation.ACCOUNT_LOOKUP, batch, callback)
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
    batch,
    destroy
  }

  _interval = setInterval(() => {
    binding.tick(context)
  }, 50)

  return _client
}
