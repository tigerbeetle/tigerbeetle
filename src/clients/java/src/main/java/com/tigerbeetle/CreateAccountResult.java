package com.tigerbeetle;

public enum CreateAccountResult {
    Ok,
    LinkedEventFailed,
    LinkedEventChainOpen,

    ReservedFlag,
    ReservedField,

    IdMustNotBeZero,
    IdMustNotBeIntMax,
    LedgerMustNotBeZero,
    CodeMustNotBeZero,
    DebitsPendingMustBeZero,
    DebitsPostedMustBeZero,
    CreditsPendingMustBeZero,
    CreditsPostedMustBeZero,

    MutuallyExclusiveFlags,

    ExistsWithDifferentFlags,
    ExistsWithDifferentUserData,
    ExistsWithDifferentLedger,
    ExistsWithDifferentCode,
    Exists;

    public static CreateAccountResult fromValue(int value) {
        var values = CreateAccountResult.values();
        if (value < 0 || value >= values.length)
            throw new IllegalArgumentException(
                    String.format("Invalid CreateAccountResult value=%d", value));

        return values[value];
    }
}
