namespace TigerBeetle
{
    public enum CreateAccountResult : uint
    {
        Ok = 0,
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

        OverflowsDebits,
        OverflowsCredits,

        ExceedsCredits,
        ExceedsDebits,

        ExistsWithDifferentFlags,
        ExistsWithDifferentUserData,
        ExistsWithDifferentLedger,
        ExistsWithDifferentCode,
        Exists,
    }
}
