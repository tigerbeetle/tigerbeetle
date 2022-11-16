package com.tigerbeetle;

public enum CreateTransferResult {
    Ok,
    LinkedEventFailed,
    LinkedEventChainOpen,

    ReservedFlag,
    ReservedField,

    IdMustNotBeZero,
    IdMustNotBeIntMax,
    DebitAccountIdMustNotBeZero,
    DebitAccountIdMustNotBeIntMax,
    CreditAccountIdMustNotBeZero,
    CreditAccountIdMustNotBeIntMax,
    AccountsMustBeDifferent,

    PendingIdMustBeZero,
    PendingTransferMustTimeout,

    LedgerMustNotBeZero,
    CodeMustNotBeZero,
    AmountMustNotBeZero,

    DebitAccountNotFound,
    CreditAccountNotFound,

    AccountsMustHaveTheSameLedger,
    TransferMustHaveTheSameLedgerAsAccounts,

    ExistsWithDifferentFlags,
    ExistsWithDifferentDebitAccountId,
    ExistsWithDifferentCreditAccountId,
    ExistsWithDifferentUserData,
    ExistsWithDifferentPendingId,
    ExistsWithDifferentTimeout,
    ExistsWithDifferentCode,
    ExistsWithDifferentAmount,
    Exists,

    OverflowsDebitsPending,
    OverflowsCreditsPending,
    OverflowsDebitsPosted,
    OverflowsCreditsPosted,
    OverflowsDebits,
    OverflowsCredits,

    ExceedsCredits,
    ExceedsDebits,

    CannotPostAndVoidPendingTransfer,
    PendingTransferCannotPostOrVoidAnother,
    TimeoutReservedForPendingTransfer,

    PendingIdMustNotBeZero,
    PendingIdMustNotBeIntMax,
    PendingIdMustBeDifferent,

    PendingTransferNotFound,
    PendingTransferNotPending,

    PendingTransferHasDifferentDebitAccountId,
    PendingTransferHasDifferentCreditAccountId,
    PendingTransferHasDifferentLedger,
    PendingTransferHasDifferentCode,

    ExceedsPendingTransferAmount,
    PendingTransferHasDifferentAmount,

    PendingTransferAlreadyPosted,
    PendingTransferAlreadyVoided,

    PendingTransferExpired;

    public static CreateTransferResult fromValue(int value) {
        var values = CreateTransferResult.values();
        if (value < 0 || value >= values.length)
            throw new AssertionError("Invalid CreateTransferResult: value=%d", value);

        return values[value];
    }
}
