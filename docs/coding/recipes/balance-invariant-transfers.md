# Balance-invariant Transfers

For some accounts, it may be useful to enforce
[`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits)
or
[`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)
balance invariants for only a subset of all transfers, rather than all transfers.

This can be achieved by having a **control** account used to test the balance invariants at the
desired points in time. The control account will have a 0 balance and the balance invariant that we
want to test on the **destination** account. At the point where we want to test the destination
account balance invariant, we can initiate a pending balancing transfer for the **opposite** side to
the control account. If the invariant is violated on the destination account, the balancing transfer
has non-zero amount, violates the control account invariant, and fails the entire chain. The
following example will make this clearer.

## Per-transfer `credits_must_not_exceed_debits`

Let's test a `credits_must_not_exceed_debits` balance invariant on a destination account after a particular transfer.

This recipe requires three accounts:
1. The **source** account, to debit.
2. The **destination** account, to credit. (With _neither_
  [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits) nor
  [`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits) set,
  since in this recipe we are only enforcing the invariant on a per-transfer basis.)
3. The **control** account, to test the balance invariant. The control account should have
  [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)
  set.

| Id | Debit Account | Credit Account | Amount | Pending Id |                                               Flags |
| -: | ------------: | -------------: | -----: | ---------: | --------------------------------------------------: |
|  1 |        Source |    Destination |    123 |          - | [`linked`](../../reference/transfer.md#flagslinked) |
|  2 |   Destination |        Control |      1 |          - | [`linked`](../../reference/transfer.md#flagslinked), [`pending`](../../reference/transfer.md#flagspending), [`balancing_debit`](../../reference/transfer.md#flagsbalancing_debit) |
|  3 |             - |              - |      0 |          2 | [`void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer) |

When the destination account's credits after transfer `1` do not exceed its debits, the chain will succeed.
When the destination account's credits after transfer `1` exceed its debits, transfer `2` will fail with `exceeds_debits`.

## Per-transfer `debits_must_not_exceed_credits`

This case is symmetric:

1. The **source** is account to credit.
2. The **destination** is account to debit. Neither
  [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits) nor
  [`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits) are set.
3. The **control** account has
  [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)
  set.

| Id | Debit Account | Credit Account | Amount | Pending Id |                                               Flags |
| -: | ------------: | -------------: | -----: | ---------: | --------------------------------------------------: |
|  1 |   Destination |         Source |    123 |          - | [`linked`](../../reference/transfer.md#flagslinked) |
|  2 |   Control     |    Destination |      1 |          - | [`linked`](../../reference/transfer.md#flagslinked), [`pending`](../../reference/transfer.md#flagspending), [`balancing_credit`](../../reference/transfer.md#flagsbalancing_credit) |
|  3 |             - |              - |      0 |          2 | [`void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer) |
