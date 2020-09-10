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
