namespace TigerBeetle
{
    public enum CreateTransferResult : uint
    {
        Ok = 0,
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

        PendingTransferExpired,
    }
}
