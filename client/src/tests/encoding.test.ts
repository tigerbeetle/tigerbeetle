import { encodeReserveCommand, encodeCommitCommand, encodeCreateAccountCommand } from '../utils/encoding'
import { ReserveCommand, CommitCommand, CreateAccountCommand } from '../types'

// TODO: assume the below will be UUIDS?
const sourceAccountId = 'c7bc8682-b28d-4ec5-ad9a-cf8a52e17c6c'
const targetAccountId = 'db72fe7f-4aa2-48e7-88f6-c0be94c4b9b5'
const id = '312053cb-a548-4793-95ea-ca67e4f69ab4'

describe('Encode reserve command', (): void => {
  test('strips - from id, source account id and target account id', async (): Promise<void> => {
    const command: ReserveCommand = {
      id,
      source_account_id: sourceAccountId,
      target_account_id: targetAccountId,
      amount: BigInt(10000)
    }

    const buffer = encodeReserveCommand(command)

    expect(buffer.byteLength).toBe(128)
    expect(buffer.slice(0, 16).toString('hex')).toBe('312053cba548479395eaca67e4f69ab4')
    expect(buffer.slice(16, 32).toString('hex')).toBe('c7bc8682b28d4ec5ad9acf8a52e17c6c')
    expect(buffer.slice(32, 48).toString('hex')).toBe('db72fe7f4aa248e788f6c0be94c4b9b5')
  })

  test('encodes custom_1 if present', async (): Promise<void> => {
    const command: ReserveCommand = {
      id,
      source_account_id: sourceAccountId,
      target_account_id: targetAccountId,
      amount: BigInt(10000),
      custom_1: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    const buffer = encodeReserveCommand(command)

    expect(buffer.slice(48, 64).toString('hex')).toMatch('0100010100000101')
    expect(buffer.slice(64, 80).toString('hex')).toMatch('0000000000000000')
    expect(buffer.slice(80, 96).toString('hex')).toMatch('0000000000000000')
  })

  test('encodes custom_2 if present', async (): Promise<void> => {
    const command: ReserveCommand = {
      id,
      source_account_id: sourceAccountId,
      target_account_id: targetAccountId,
      amount: BigInt(10000),
      custom_2: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    const buffer = encodeReserveCommand(command)

    expect(buffer.slice(48, 64).toString('hex')).toMatch('0000000000000000')
    expect(buffer.slice(64, 80).toString('hex')).toMatch('0100010100000101')
    expect(buffer.slice(80, 96).toString('hex')).toMatch('0000000000000000')
  })

  test('encodes custom_3 if present', async (): Promise<void> => {
    const command: ReserveCommand = {
      id,
      source_account_id: sourceAccountId,
      target_account_id: targetAccountId,
      amount: BigInt(10000),
      custom_3: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    const buffer = encodeReserveCommand(command)

    expect(buffer.slice(48, 64).toString('hex')).toMatch('0000000000000000')
    expect(buffer.slice(64, 80).toString('hex')).toMatch('0000000000000000')
    expect(buffer.slice(80, 96).toString('hex')).toMatch('0100010100000101')
  })

  test('encodes flags if present', async (): Promise<void> => {
    const command: ReserveCommand = {
      id,
      source_account_id: sourceAccountId,
      target_account_id: targetAccountId,
      amount: BigInt(10000)
    }

    const bufferWithoutFlags = encodeReserveCommand(command)
    const bufferWithFlags = encodeReserveCommand({ ...command, flags: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1]) })

    expect(bufferWithFlags.slice(96, 104).toString('hex')).toMatch('0100010100000101')
    expect(bufferWithoutFlags.slice(96, 104).toString('hex')).toMatch('0000000000000000')
  })

  test('encodes amount as BE', async (): Promise<void> => {
    const command: ReserveCommand = {
      id,
      source_account_id: sourceAccountId,
      target_account_id: targetAccountId,
      amount: BigInt(10013)
    }

    const buffer = encodeReserveCommand(command)

    expect(buffer.readBigUInt64BE(104)).toBe(BigInt(10013))
  })

  test('encodes the timeout as BE if present', async (): Promise<void> => {
    const command: ReserveCommand = {
      id,
      source_account_id: sourceAccountId,
      target_account_id: targetAccountId,
      amount: BigInt(10013)
    }

    const bufferWithOutTimeout = encodeReserveCommand(command)
    const bufferWithTimeout = encodeReserveCommand({ ...command, timeout: BigInt(17) })

    expect(bufferWithTimeout.readBigUInt64BE(112)).toBe(BigInt(17))
    expect(bufferWithOutTimeout.readBigUInt64BE(112)).toBe(BigInt(0))
  })
})

describe('Encode accept command', (): void => {
  test('strips - from id', async (): Promise<void> => {
    const commitCommand: CommitCommand = {
      id,
      flags: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    const buffer = encodeCommitCommand(commitCommand)

    expect(buffer.slice(0, 16).toString('hex')).toMatch('312053cba548479395eaca67e4f69ab4')
  })

  test('encodes custom_1 if present', async (): Promise<void> => {
    const commitCommand: CommitCommand = {
      id,
      custom_1: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1]),
      flags: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    const buffer = encodeCommitCommand(commitCommand)

    expect(buffer.slice(16, 32).toString('hex')).toMatch('0100010100000101')
    expect(buffer.slice(32, 48).toString('hex')).toMatch('0000000000000000')
    expect(buffer.slice(48, 64).toString('hex')).toMatch('0000000000000000')
  })

  test('encodes custom_2 if present', async (): Promise<void> => {
    const commitCommand: CommitCommand = {
      id,
      custom_2: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1]),
      flags: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    const buffer = encodeCommitCommand(commitCommand)

    expect(buffer.slice(16, 32).toString('hex')).toMatch('0000000000000000')
    expect(buffer.slice(32, 48).toString('hex')).toMatch('0100010100000101')
    expect(buffer.slice(48, 64).toString('hex')).toMatch('0000000000000000')
  })

  test('encodes custom_3 if present', async (): Promise<void> => {
    const commitCommand: CommitCommand = {
      id,
      custom_3: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1]),
      flags: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    const buffer = encodeCommitCommand(commitCommand)

    expect(buffer.slice(16, 32).toString('hex')).toMatch('0000000000000000')
    expect(buffer.slice(32, 48).toString('hex')).toMatch('0000000000000000')
    expect(buffer.slice(48, 64).toString('hex')).toMatch('0100010100000101')
  })

  test('encodes flags', async (): Promise<void> => {
    const commitCommand: CommitCommand = {
      id,
      flags: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1])
    }

    const buffer = encodeCommitCommand(commitCommand)

    expect(buffer.slice(64, 72).toString('hex')).toMatch('0100010100000101')
  })
})

describe('Encode create account command', (): void => {
  test('strips - from id', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const buffer = encodeCreateAccountCommand(command)

    expect(buffer.slice(0, 16).toString('hex')).toMatch('312053cba548479395eaca67e4f69ab4')
  })

  test('encodes custom if present', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const bufferWithCustom = encodeCreateAccountCommand({ ...command, custom: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1]) })
    const bufferWithOutCustom = encodeCreateAccountCommand(command)

    expect(bufferWithOutCustom.slice(16, 32).toString('hex')).toMatch('0000000000000000')
    expect(bufferWithCustom.slice(16, 32).toString('hex')).toMatch('0100010100000101')
  })

  test('encodes flags if present', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const bufferWithFlags = encodeCreateAccountCommand({ ...command, flags: Buffer.from([1, 0, 1, 1, 0, 0, 1, 1]) })
    const bufferWithOutFlags = encodeCreateAccountCommand(command)

    expect(bufferWithOutFlags.slice(32, 40).toString('hex')).toMatch('0000000000000000')
    expect(bufferWithFlags.slice(32, 40).toString('hex')).toMatch('0100010100000101')
  })

  test('encodes unit as BE if present', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const bufferWithUnit = encodeCreateAccountCommand({ ...command, unit: BigInt(10013) })
    const bufferWithOutUnit = encodeCreateAccountCommand(command)

    expect(bufferWithOutUnit.slice(40, 48).readBigUInt64BE()).toBe(BigInt(0))
    expect(bufferWithUnit.slice(40, 48).readBigUInt64BE()).toBe(BigInt(10013))
  })

  test('encodes debit_accepted as BE', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const buffer = encodeCreateAccountCommand(command)

    expect(buffer.slice(48, 56).readBigUInt64BE()).toBe(BigInt(10013))
  })

  test('encodes debit_reserved as BE', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const buffer = encodeCreateAccountCommand(command)

    expect(buffer.slice(56, 64).readBigUInt64BE()).toBe(BigInt(10017))
  })

  test('encodes credit_accepted as BE', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const buffer = encodeCreateAccountCommand(command)

    expect(buffer.slice(64, 72).readBigUInt64BE()).toBe(BigInt(9973))
  })

  test('encodes credit_reserved as BE', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const buffer = encodeCreateAccountCommand(command)

    expect(buffer.slice(72, 80).readBigUInt64BE()).toBe(BigInt(9257))
  })

  test('encodes limit_debit_accepted as BE if present', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const bufferWithLimit = encodeCreateAccountCommand({ ...command, limit_debit_accepted: BigInt(7643) })
    const bufferWithoutLimit = encodeCreateAccountCommand(command)

    expect(bufferWithLimit.slice(80, 88).readBigUInt64BE()).toBe(BigInt(7643))
    expect(bufferWithoutLimit.slice(80, 88).readBigUInt64BE()).toBe(BigInt(0))
  })

  test('encodes limit_debit_reserved as BE if present', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const bufferWithLimit = encodeCreateAccountCommand({ ...command, limit_debit_reserved: BigInt(7603) })
    const bufferWithoutLimit = encodeCreateAccountCommand(command)

    expect(bufferWithLimit.slice(88, 96).readBigUInt64BE()).toBe(BigInt(7603))
    expect(bufferWithoutLimit.slice(88, 96).readBigUInt64BE()).toBe(BigInt(0))
  })

  test('encodes limit_credit_accepted as BE if present', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const bufferWithLimit = encodeCreateAccountCommand({ ...command, limit_credit_accepted: BigInt(7753) })
    const bufferWithoutLimit = encodeCreateAccountCommand(command)

    expect(bufferWithLimit.slice(96, 104).readBigUInt64BE()).toBe(BigInt(7753))
    expect(bufferWithoutLimit.slice(96, 104).readBigUInt64BE()).toBe(BigInt(0))
  })

  test('encodes limit_credit_reserved as BE if present', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const bufferWithLimit = encodeCreateAccountCommand({ ...command, limit_credit_reserved: BigInt(6599) })
    const bufferWithoutLimit = encodeCreateAccountCommand(command)

    expect(bufferWithLimit.slice(104, 112).readBigUInt64BE()).toBe(BigInt(6599))
    expect(bufferWithoutLimit.slice(104, 112).readBigUInt64BE()).toBe(BigInt(0))
  })

  test('encodes timeout as BE if present', async (): Promise<void> => {
    const command: CreateAccountCommand = {
      id,
      debit_accepted: BigInt(10013),
      debit_reserved: BigInt(10017),
      credit_accepted: BigInt(9973),
      credit_reserved: BigInt(9257)
    }

    const bufferWithTimeout = encodeCreateAccountCommand({ ...command, timeout: BigInt(5881) })
    const bufferWithoutTimeout = encodeCreateAccountCommand(command)

    expect(bufferWithTimeout.slice(112, 120).readBigUInt64BE()).toBe(BigInt(5881))
    expect(bufferWithoutTimeout.slice(112, 120).readBigUInt64BE()).toBe(BigInt(0))
  })
})
