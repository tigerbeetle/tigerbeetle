export interface ReserveCommand {
  id: string
  source_account_id: string
  target_account_id: string
  custom_1?: Buffer
  custom_2?: Buffer
  custom_3?: Buffer
  flags?: Buffer
  amount: bigint
  timeout?: bigint
}

export interface CommitCommand {
  id: string
  custom_1?: Buffer
  custom_2?: Buffer
  custom_3?: Buffer
  flags: Buffer
}

export interface CreateAccountCommand {
  id: string
  custom?: Buffer
  flags?: Buffer
  unit?: bigint
  debit_accepted: bigint
  debit_reserved: bigint
  credit_accepted: bigint
  credit_reserved: bigint
  limit_debit_accepted?: bigint
  limit_debit_reserved?: bigint
  limit_credit_accepted?: bigint
  limit_credit_reserved?: bigint
  timeout?: bigint
}

// Wire protocol constants
export const MAGIC = Buffer.from('0a5ca1ab1ebee11e', 'hex') // A scalable beetle...

export const NETWORK_HEADER                      = 64
export const NETWORK_HEADER_CHECKSUM_META        = 16
export const NETWORK_HEADER_CHECKSUM_DATA        = 16
export const NETWORK_HEADER_ID                   = 16
export const NETWORK_HEADER_MAGIC                = 8
export const NETWORK_HEADER_COMMAND              = 4
export const NETWORK_HEADER_SIZE                 = 4

export const NETWORK_HEADER_CHECKSUM_META_OFFSET = 0
export const NETWORK_HEADER_CHECKSUM_DATA_OFFSET = NETWORK_HEADER_CHECKSUM_META_OFFSET + NETWORK_HEADER_CHECKSUM_META
export const NETWORK_HEADER_ID_OFFSET            = NETWORK_HEADER_CHECKSUM_DATA_OFFSET + NETWORK_HEADER_CHECKSUM_DATA
export const NETWORK_HEADER_MAGIC_OFFSET         = NETWORK_HEADER_ID_OFFSET + NETWORK_HEADER_ID
export const NETWORK_HEADER_COMMAND_OFFSET       = NETWORK_HEADER_MAGIC_OFFSET + NETWORK_HEADER_MAGIC
export const NETWORK_HEADER_SIZE_OFFSET          = NETWORK_HEADER_COMMAND_OFFSET + NETWORK_HEADER_COMMAND
