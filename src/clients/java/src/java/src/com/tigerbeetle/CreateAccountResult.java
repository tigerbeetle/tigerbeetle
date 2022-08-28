package com.tigerbeetle;

public enum CreateAccountResult {
    Ok,
    LinkedEventFailed,

    ReservedFlag,
    ReservedField,

    IdMustNotBeZero,
    IdMustNotBeIntMax,
    LedgerMustNotBeZero,
    CodeMustNotBeZero,

    MutuallyExclusiveFlags,

    OverflowsDebits,
    OverflowsCredits,

    ExceedsCredits,
    ExceedsDebits,

    ExistsWithDifferentFlags,
    ExistsWithDifferentUser_data,
    ExistsWithDifferentLedger,
    ExistsWithDifferentCode,
    ExistsWithDifferentDebitsPending,
    ExistsWithDifferentDebitsPosted,
    ExistsWithDifferentCreditsPending,
    ExistsWithDifferentCreditsPosted,
    Exists;
}
