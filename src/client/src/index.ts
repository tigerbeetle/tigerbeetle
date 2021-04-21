export interface TigerBeetleResponse {
  index: number
  error: string
}

export enum Operation {
  CREATE_ACCOUNT,
  CREATE_TRANSFER,
  COMMIT_TRANSFER
}

export interface CreateAccountCommand {
  id: Buffer
  custom?: Buffer
  flags?: bigint
  unit: bigint
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

export type TigerBeetleCommand = CreateAccountCommand

export interface TigerBeetle {
  // createAccounts: () => Promise<TigerBeetleResponse[]>
  // createTransfers: () => Promise<TigerBeetleResponse[]>
  // commitTransfers: () => Promise<TigerBeetleResponse[]>
  // destroy: () => Promise<void>
  batch: (operation: Operation, batch: TigerBeetleCommand[], callback: (result: TigerBeetleResponse[]) => void) => void
}

export function createClient (): TigerBeetle {

  const context = 1;

  const batch = (operation: Operation, batch: TigerBeetleCommand[], callback: (result: TigerBeetleResponse[]) => void) => {

  }

  const createAccounts = async (): Promise<TigerBeetleResponse[]> => {
    return []
  }

  return {
    batch
  }
}