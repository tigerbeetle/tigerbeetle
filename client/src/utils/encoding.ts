import assert from 'assert'
import { ReserveCommand, CommitCommand, CreateAccountCommand } from '../types'

// Sizes of the data elements in bytes
export const TRANSFER                = 128
export const CREATE_ACCOUNT          = 128
export const COMMIT_TRANSFER         = 80
export const ID                      = 16
export const SOURCE_ACCOUNT_ID       = 16
export const TARGET_ACCOUNT_ID       = 16
export const CUSTOM                  = 16
export const FLAGS                   = 8
export const AMOUNT                  = 8
export const TIMEOUT                 = 8
export const TIMESTAMP               = 8
export const UNIT                    = 8
export const DEBIT_ACCEPTED          = 8
export const DEBIT_RESERVED          = 8
export const CREDIT_ACCEPTED         = 8
export const CREDIT_RESERVED         = 8
export const LIMIT_DEBIT_ACCEPTED    = 8
export const LIMIT_DEBIT_RESERVED    = 8
export const LIMIT_CREDIT_ACCEPTED   = 8
export const LIMIT_CREDIT_RESERVED   = 8

export const TRANSFER_ID_OFFSET                 = 0
export const TRANSFER_SOURCE_ACCOUNT_ID_OFFSET  = TRANSFER_ID_OFFSET + ID
export const TRANSFER_TARGET_ACCOUNT_ID_OFFSET  = TRANSFER_SOURCE_ACCOUNT_ID_OFFSET + SOURCE_ACCOUNT_ID
export const TRANSFER_CUSTOM_1_OFFSET           = TRANSFER_TARGET_ACCOUNT_ID_OFFSET + TARGET_ACCOUNT_ID
export const TRANSFER_CUSTOM_2_OFFSET           = TRANSFER_CUSTOM_1_OFFSET + CUSTOM
export const TRANSFER_CUSTOM_3_OFFSET           = TRANSFER_CUSTOM_2_OFFSET + CUSTOM
export const TRANSFER_FLAGS_OFFSET              = TRANSFER_CUSTOM_3_OFFSET + CUSTOM
export const TRANSFER_AMOUNT_OFFSET             = TRANSFER_FLAGS_OFFSET + FLAGS
export const TRANSFER_TIMEOUT_OFFSET            = TRANSFER_AMOUNT_OFFSET + AMOUNT
export const TRANSFER_TIMESTAMP_OFFSET          = TRANSFER_TIMEOUT_OFFSET + TIMEOUT

export const COMMIT_TRANSFER_ID_OFFSET          = 0
export const COMMIT_TRANSFER_CUSTOM_1_OFFSET    = COMMIT_TRANSFER_ID_OFFSET + ID
export const COMMIT_TRANSFER_CUSTOM_2_OFFSET    = COMMIT_TRANSFER_CUSTOM_1_OFFSET + CUSTOM
export const COMMIT_TRANSFER_CUSTOM_3_OFFSET    = COMMIT_TRANSFER_CUSTOM_2_OFFSET + CUSTOM
export const COMMIT_TRANSFER_FLAGS_OFFSET       = COMMIT_TRANSFER_CUSTOM_3_OFFSET + CUSTOM
export const COMMIT_TRANSFER_TIMESTAMP_OFFSET   = COMMIT_TRANSFER_FLAGS_OFFSET + FLAGS

export const CREATE_ACCOUNT_ID_OFFSET                    = 0
export const CREATE_ACCOUNT_CUSTOM_OFFSET                = CREATE_ACCOUNT_ID_OFFSET + ID
export const CREATE_ACCOUNT_FLAGS_OFFSET                 = CREATE_ACCOUNT_CUSTOM_OFFSET + CUSTOM
export const CREATE_ACCOUNT_UNIT_OFFSET                  = CREATE_ACCOUNT_FLAGS_OFFSET + FLAGS
export const CREATE_ACCOUNT_DEBIT_ACCEPTED_OFFSET        = CREATE_ACCOUNT_UNIT_OFFSET + UNIT
export const CREATE_ACCOUNT_DEBIT_RESERVED_OFFSET        = CREATE_ACCOUNT_DEBIT_ACCEPTED_OFFSET + DEBIT_ACCEPTED
export const CREATE_ACCOUNT_CREDIT_ACCEPTED_OFFSET       = CREATE_ACCOUNT_DEBIT_RESERVED_OFFSET + DEBIT_RESERVED
export const CREATE_ACCOUNT_CREDIT_RESERVED_OFFSET       = CREATE_ACCOUNT_CREDIT_ACCEPTED_OFFSET + CREDIT_ACCEPTED
export const CREATE_ACCOUNT_LIMIT_DEBIT_ACCEPTED_OFFSET  = CREATE_ACCOUNT_CREDIT_RESERVED_OFFSET + CREDIT_RESERVED
export const CREATE_ACCOUNT_LIMIT_DEBIT_RESERVED_OFFSET  = CREATE_ACCOUNT_LIMIT_DEBIT_ACCEPTED_OFFSET + DEBIT_ACCEPTED
export const CREATE_ACCOUNT_LIMIT_CREDIT_ACCEPTED_OFFSET = CREATE_ACCOUNT_LIMIT_DEBIT_RESERVED_OFFSET + DEBIT_RESERVED
export const CREATE_ACCOUNT_LIMIT_CREDIT_RESERVED_OFFSET = CREATE_ACCOUNT_LIMIT_CREDIT_ACCEPTED_OFFSET + CREDIT_ACCEPTED
export const CREATE_ACCOUNT_TIMEOUT_OFFSET               = CREATE_ACCOUNT_LIMIT_CREDIT_RESERVED_OFFSET + CREDIT_RESERVED
export const CREATE_ACCOUNT_TIMESTAMP_OFFSET             = CREATE_ACCOUNT_TIMEOUT_OFFSET + TIMEOUT

export function encodeReserveCommand (reserveCommand: ReserveCommand): Buffer {
  const buffer = Buffer.alloc(TRANSFER)

  assert(reserveCommand.id.length <= ID, 'Reserve command id is not 16 bytes or less.')
  buffer.fill(
    reserveCommand.id,
    TRANSFER_ID_OFFSET,
    TRANSFER_SOURCE_ACCOUNT_ID_OFFSET
  )

  assert(reserveCommand.source_account_id.length <= SOURCE_ACCOUNT_ID, 'Reserve command source account id is not 16 bytes or less.')
  buffer.fill(
    reserveCommand.source_account_id,
    TRANSFER_SOURCE_ACCOUNT_ID_OFFSET,
    TRANSFER_TARGET_ACCOUNT_ID_OFFSET
  )

  assert(reserveCommand.target_account_id.length <= TARGET_ACCOUNT_ID, 'Reserve command target account id is not 16 bytes or less.')
  buffer.fill(
    reserveCommand.target_account_id,
    TRANSFER_TARGET_ACCOUNT_ID_OFFSET,
    TRANSFER_CUSTOM_1_OFFSET
  )

  if (reserveCommand.custom_1) {
    assert(reserveCommand.custom_1.length <= CUSTOM, 'Reserve command custom slot 1 is not 16 bytes or less')
    buffer.fill(
      reserveCommand.custom_1,
      TRANSFER_CUSTOM_1_OFFSET,
      TRANSFER_CUSTOM_2_OFFSET
    )
  }

  if (reserveCommand.custom_2) {
    assert(reserveCommand.custom_2.length <= CUSTOM, 'Reserve command custom slot 2 is not 16 bytes or less')
    buffer.fill(
      reserveCommand.custom_2,
      TRANSFER_CUSTOM_2_OFFSET,
      TRANSFER_CUSTOM_3_OFFSET
    )
  }

  if (reserveCommand.custom_3) {
    assert(reserveCommand.custom_3.length <= CUSTOM, 'Reserve command custom slot 3 is not 16 bytes or less')
    buffer.fill(
      reserveCommand.custom_3,
      TRANSFER_CUSTOM_3_OFFSET,
      TRANSFER_FLAGS_OFFSET
    )
  }

  if (reserveCommand.flags) {
    assert(reserveCommand.flags.length === FLAGS, 'Reserve command flags is not 8 bytes')
    buffer.fill(
      reserveCommand.flags,
      TRANSFER_FLAGS_OFFSET,
      TRANSFER_AMOUNT_OFFSET
    )
  }

  assert(reserveCommand.amount > BigInt(0) && reserveCommand.amount < BigInt(18446744073709551615)) // 64 bit unsigned int. TODO: enforce amount should be greater than 0?
  buffer.writeBigUInt64BE(
    reserveCommand.amount,
    TRANSFER_AMOUNT_OFFSET
  )

  if (reserveCommand.timeout) {
    assert(reserveCommand.timeout > BigInt(0) && reserveCommand.timeout < BigInt(18446744073709551615)) // 64 bit unsigned int. TODO: enforce timeout should be greater than 0?
    buffer.writeBigUInt64BE(
      reserveCommand.timeout,
      TRANSFER_TIMEOUT_OFFSET
    )
  }

  return buffer
}

export function encodeCommitCommand (commitCommand: CommitCommand): Buffer {
  const buffer = Buffer.alloc(COMMIT_TRANSFER)

  assert(commitCommand.id.length, 'Commit command ID is not 16 bytes or less')
  buffer.fill(
    commitCommand.id,
    COMMIT_TRANSFER_ID_OFFSET,
    COMMIT_TRANSFER_CUSTOM_1_OFFSET
  )

  if (commitCommand.custom_1) {
    assert(commitCommand.custom_1.length <= CUSTOM, 'Commit command custom slot 1 is not 16 bytes or less')
    buffer.fill(
      commitCommand.custom_1,
      COMMIT_TRANSFER_CUSTOM_1_OFFSET,
      COMMIT_TRANSFER_CUSTOM_2_OFFSET
    )
  }

  if (commitCommand.custom_2) {
    assert(commitCommand.custom_2.length <= CUSTOM, 'Commit command custom slot 2 is not 16 bytes or less')
    buffer.fill(
      commitCommand.custom_2,
      COMMIT_TRANSFER_CUSTOM_2_OFFSET,
      COMMIT_TRANSFER_CUSTOM_3_OFFSET
    )
  }

  if (commitCommand.custom_3) {
    assert(commitCommand.custom_3.length <= CUSTOM, 'Commit command custom slot 3 is not 16 bytes or less')
    buffer.fill(
      commitCommand.custom_3,
      COMMIT_TRANSFER_CUSTOM_3_OFFSET,
      COMMIT_TRANSFER_FLAGS_OFFSET
    )
  }

  assert(commitCommand.flags.length === FLAGS, 'Commit command flags is not 8 bytes')
  buffer.fill(
    commitCommand.flags,
    COMMIT_TRANSFER_FLAGS_OFFSET,
    COMMIT_TRANSFER_TIMESTAMP_OFFSET
  )

  return buffer
}

export function encodeCreateAccountCommand (createAccountCommand: CreateAccountCommand): Buffer {
  const buffer = Buffer.alloc(CREATE_ACCOUNT)

  assert(createAccountCommand.id.length <= 16, 'Create account command ID is not 16 bytes or less') // assuming the id is a UUID
  buffer.fill(
    createAccountCommand.id,
    CREATE_ACCOUNT_ID_OFFSET,
    CREATE_ACCOUNT_CUSTOM_OFFSET
  )

  if (createAccountCommand.custom) {
    assert(createAccountCommand.custom.length <= CUSTOM, 'Create account command custom slot is not 16 bytes or less')
    buffer.fill(
      createAccountCommand.custom,
      CREATE_ACCOUNT_CUSTOM_OFFSET,
      CREATE_ACCOUNT_FLAGS_OFFSET
    )
  }

  if (createAccountCommand.flags) {
    assert(createAccountCommand.flags.length <= FLAGS, 'Create account command flags is not 8 bytes')
    buffer.fill(
      createAccountCommand.flags,
      CREATE_ACCOUNT_FLAGS_OFFSET,
      CREATE_ACCOUNT_UNIT_OFFSET
    )
  }

  if (createAccountCommand.unit) {
    assert(createAccountCommand.unit > BigInt(0) && createAccountCommand.unit < BigInt(18446744073709551615)) // 64 bit unsigned int
    buffer.writeBigUInt64BE(
      createAccountCommand.unit,
      CREATE_ACCOUNT_UNIT_OFFSET
    )
  }

  assert(createAccountCommand.debit_accepted > BigInt(0) && createAccountCommand.debit_accepted < BigInt(18446744073709551615)) // 64 bit unsigned int
  buffer.writeBigUInt64BE(
    createAccountCommand.debit_accepted,
    CREATE_ACCOUNT_DEBIT_ACCEPTED_OFFSET
  )

  assert(createAccountCommand.debit_reserved > BigInt(0) && createAccountCommand.debit_reserved < BigInt(18446744073709551615)) // 64 bit unsigned int
  buffer.writeBigUInt64BE(
    createAccountCommand.debit_reserved,
    CREATE_ACCOUNT_DEBIT_RESERVED_OFFSET
  )

  assert(createAccountCommand.credit_accepted > BigInt(0) && createAccountCommand.credit_accepted < BigInt(18446744073709551615)) // 64 bit unsigned int
  buffer.writeBigUInt64BE(
    createAccountCommand.credit_accepted,
    CREATE_ACCOUNT_CREDIT_ACCEPTED_OFFSET
  )

  assert(createAccountCommand.credit_reserved > BigInt(0) && createAccountCommand.credit_reserved < BigInt(18446744073709551615)) // 64 bit unsigned int
  buffer.writeBigUInt64BE(
    createAccountCommand.credit_reserved,
    CREATE_ACCOUNT_CREDIT_RESERVED_OFFSET
  )

  if (createAccountCommand.limit_debit_accepted) {
    assert(createAccountCommand.limit_debit_accepted > BigInt(0) && createAccountCommand.limit_debit_accepted < BigInt(18446744073709551615)) // 64 bit unsigned int
    buffer.writeBigUInt64BE(
      createAccountCommand.limit_debit_accepted,
      CREATE_ACCOUNT_LIMIT_DEBIT_ACCEPTED_OFFSET
    )
  }

  if (createAccountCommand.limit_debit_reserved) {
    assert(createAccountCommand.limit_debit_reserved > BigInt(0) && createAccountCommand.limit_debit_reserved < BigInt(18446744073709551615)) // 64 bit unsigned int
    buffer.writeBigUInt64BE(
      createAccountCommand.limit_debit_reserved,
      CREATE_ACCOUNT_LIMIT_DEBIT_RESERVED_OFFSET
    )
  }

  if (createAccountCommand.limit_credit_accepted) {
    assert(createAccountCommand.limit_credit_accepted > BigInt(0) && createAccountCommand.limit_credit_accepted < BigInt(18446744073709551615)) // 64 bit unsigned int
    buffer.writeBigUInt64BE(
      createAccountCommand.limit_credit_accepted,
      CREATE_ACCOUNT_LIMIT_CREDIT_ACCEPTED_OFFSET
    )
  }

  if (createAccountCommand.limit_credit_reserved) {
    assert(createAccountCommand.limit_credit_reserved > BigInt(0) && createAccountCommand.limit_credit_reserved < BigInt(18446744073709551615)) // 64 bit unsigned int
    buffer.writeBigUInt64BE(
      createAccountCommand.limit_credit_reserved,
      CREATE_ACCOUNT_LIMIT_CREDIT_RESERVED_OFFSET
    )
  }

  if (createAccountCommand.timeout) {
    assert(createAccountCommand.timeout > BigInt(0) && createAccountCommand.timeout < BigInt(18446744073709551615)) // 64 bit unsigned int
    buffer.writeBigUInt64BE(
      createAccountCommand.timeout,
      CREATE_ACCOUNT_TIMEOUT_OFFSET
    )
  }

  return buffer
}
