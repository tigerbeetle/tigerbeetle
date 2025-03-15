pub use super::*;

impl From<tbc::tb_account_t> for Account {
    fn from(other: tbc::tb_account_t) -> Account {
        assert_eq!(
            std::mem::size_of::<Account>(),
            std::mem::size_of::<tbc::tb_account_t>()
        );
        assert_eq!(
            std::mem::align_of::<Account>(),
            std::mem::align_of::<tbc::tb_account_t>()
        );
        AccountFlags::from_bits(other.flags).expect("account flags");
        unsafe { std::mem::transmute(other) }
    }
}

impl From<Account> for tbc::tb_account_t {
    fn from(other: Account) -> tbc::tb_account_t {
        assert_eq!(
            std::mem::size_of::<Account>(),
            std::mem::size_of::<tbc::tb_account_t>()
        );
        assert_eq!(
            std::mem::align_of::<Account>(),
            std::mem::align_of::<tbc::tb_account_t>()
        );
        unsafe { std::mem::transmute(other) }
    }
}

impl From<tbc::tb_transfer_t> for Transfer {
    fn from(other: tbc::tb_transfer_t) -> Transfer {
        assert_eq!(
            std::mem::size_of::<Transfer>(),
            std::mem::size_of::<tbc::tb_transfer_t>()
        );
        assert_eq!(
            std::mem::align_of::<Transfer>(),
            std::mem::align_of::<tbc::tb_transfer_t>()
        );
        TransferFlags::from_bits(other.flags).expect("transfer flags");
        unsafe { std::mem::transmute(other) }
    }
}

impl From<Transfer> for tbc::tb_transfer_t {
    fn from(other: Transfer) -> tbc::tb_transfer_t {
        assert_eq!(
            std::mem::size_of::<Transfer>(),
            std::mem::size_of::<tbc::tb_transfer_t>()
        );
        assert_eq!(
            std::mem::align_of::<Transfer>(),
            std::mem::align_of::<tbc::tb_transfer_t>()
        );
        unsafe { std::mem::transmute(other) }
    }
}

impl From<AccountFilter> for tbc::tb_account_filter_t {
    fn from(other: AccountFilter) -> tbc::tb_account_filter_t {
        assert_eq!(
            std::mem::size_of::<AccountFilter>(),
            std::mem::size_of::<tbc::tb_account_filter_t>()
        );
        assert_eq!(
            std::mem::align_of::<AccountFilter>(),
            std::mem::align_of::<tbc::tb_account_filter_t>()
        );
        unsafe { std::mem::transmute(other) }
    }
}

impl From<tbc::tb_account_balance_t> for AccountBalance {
    fn from(other: tbc::tb_account_balance_t) -> AccountBalance {
        assert_eq!(
            std::mem::size_of::<AccountBalance>(),
            std::mem::size_of::<tbc::tb_account_balance_t>()
        );
        assert_eq!(
            std::mem::align_of::<AccountBalance>(),
            std::mem::align_of::<tbc::tb_account_balance_t>()
        );
        unsafe { std::mem::transmute(other) }
    }
}

impl From<QueryFilter> for tbc::tb_query_filter_t {
    fn from(other: QueryFilter) -> tbc::tb_query_filter_t {
        assert_eq!(
            std::mem::size_of::<QueryFilter>(),
            std::mem::size_of::<tbc::tb_query_filter_t>()
        );
        assert_eq!(
            std::mem::align_of::<QueryFilter>(),
            std::mem::align_of::<tbc::tb_query_filter_t>()
        );
        unsafe { std::mem::transmute(other) }
    }
}

#[rustfmt::skip]
impl From<tbc::tb_create_accounts_result_t> for CreateAccountResult {
    fn from(other: tbc::tb_create_accounts_result_t) -> CreateAccountResult {
        use tbc::*;
        use CreateAccountResult::*;

        match other.result {
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_OK => panic!(),
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_LINKED_EVENT_FAILED => LinkedEventFailed,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_LINKED_EVENT_CHAIN_OPEN => LinkedEventChainOpen,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_IMPORTED_EVENT_EXPECTED => ImportedEventExpected,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_IMPORTED_EVENT_NOT_EXPECTED => ImportedEventNotExpected,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_TIMESTAMP_MUST_BE_ZERO => TimestampMustBeZero,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_IMPORTED_EVENT_TIMESTAMP_OUT_OF_RANGE => ImportedEventTimestampOutOfRange,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_IMPORTED_EVENT_TIMESTAMP_MUST_NOT_ADVANCE => ImportedEventTimestampMustNotAdvance,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_RESERVED_FIELD => ReservedField,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_RESERVED_FLAG => ReservedFlag,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_ID_MUST_NOT_BE_ZERO => IdMustNotBeZero,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_ID_MUST_NOT_BE_INT_MAX => IdMustNotBeIntMax,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_FLAGS => ExistsWithDifferentFlags,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_USER_DATA_128 => ExistsWithDifferentUserData128,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_USER_DATA_64 => ExistsWithDifferentUserData64,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_USER_DATA_32 => ExistsWithDifferentUserData32,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_LEDGER => ExistsWithDifferentLedger,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_EXISTS_WITH_DIFFERENT_CODE => ExistsWithDifferentCode,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_EXISTS => Exists,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_FLAGS_ARE_MUTUALLY_EXCLUSIVE => FlagsAreMutuallyExclusive,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_DEBITS_PENDING_MUST_BE_ZERO => DebitsPendingMustBeZero,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_DEBITS_POSTED_MUST_BE_ZERO => DebitsPostedMustBeZero,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_CREDITS_PENDING_MUST_BE_ZERO => CreditsPendingMustBeZero,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_CREDITS_POSTED_MUST_BE_ZERO => CreditsPostedMustBeZero,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_LEDGER_MUST_NOT_BE_ZERO => LedgerMustNotBeZero,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_CODE_MUST_NOT_BE_ZERO => CodeMustNotBeZero,
            TB_CREATE_ACCOUNT_RESULT_TB_CREATE_ACCOUNT_IMPORTED_EVENT_TIMESTAMP_MUST_NOT_REGRESS => ImportedEventTimestampMustNotRegress,
            v => Unknown(v),
        }
    }
}

#[rustfmt::skip]
impl From<tbc::tb_create_transfers_result_t> for CreateTransferResult {
    fn from(other: tbc::tb_create_transfers_result_t) -> CreateTransferResult {
        use tbc::*;
        use CreateTransferResult::*;

        match other.result {
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OK => panic!(),
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_LINKED_EVENT_FAILED => LinkedEventFailed,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_LINKED_EVENT_CHAIN_OPEN => LinkedEventChainOpen,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_IMPORTED_EVENT_EXPECTED => ImportedEventExpected,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_IMPORTED_EVENT_NOT_EXPECTED => ImportedEventNotExpected,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_TIMESTAMP_MUST_BE_ZERO => TimestampMustBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_IMPORTED_EVENT_TIMESTAMP_OUT_OF_RANGE => ImportedEventTimestampOutOfRange,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_IMPORTED_EVENT_TIMESTAMP_MUST_NOT_ADVANCE => ImportedEventTimestampMustNotAdvance,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_RESERVED_FLAG => ReservedFlag,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_ID_MUST_NOT_BE_ZERO => IdMustNotBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_ID_MUST_NOT_BE_INT_MAX => IdMustNotBeIntMax,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_FLAGS => ExistsWithDifferentFlags,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_PENDING_ID => ExistsWithDifferentPendingId,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_TIMEOUT => ExistsWithDifferentTimeout,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_DEBIT_ACCOUNT_ID => ExistsWithDifferentDebitAccountId,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_CREDIT_ACCOUNT_ID => ExistsWithDifferentCreditAccountId,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_AMOUNT => ExistsWithDifferentAmount,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_USER_DATA_128 => ExistsWithDifferentUserData128,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_USER_DATA_64 => ExistsWithDifferentUserData64,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_USER_DATA_32 => ExistsWithDifferentUserData32,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_LEDGER => ExistsWithDifferentLedger,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS_WITH_DIFFERENT_CODE => ExistsWithDifferentCode,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXISTS => Exists,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_ID_ALREADY_FAILED => IdAlreadyFailed,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_FLAGS_ARE_MUTUALLY_EXCLUSIVE => FlagsAreMutuallyExclusive,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_DEBIT_ACCOUNT_ID_MUST_NOT_BE_ZERO => DebitAccountIdMustNotBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_DEBIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX => DebitAccountIdMustNotBeIntMax,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_CREDIT_ACCOUNT_ID_MUST_NOT_BE_ZERO => CreditAccountIdMustNotBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_CREDIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX => CreditAccountIdMustNotBeIntMax,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_ACCOUNTS_MUST_BE_DIFFERENT => AccountsMustBeDifferent,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_ID_MUST_BE_ZERO => PendingIdMustBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_ID_MUST_NOT_BE_ZERO => PendingIdMustNotBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_ID_MUST_NOT_BE_INT_MAX => PendingIdMustNotBeIntMax,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_ID_MUST_BE_DIFFERENT => PendingIdMustBeDifferent,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_TIMEOUT_RESERVED_FOR_PENDING_TRANSFER => TimeoutReservedForPendingTransfer,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_CLOSING_TRANSFER_MUST_BE_PENDING => ClosingTransferMustBePending,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_AMOUNT_MUST_NOT_BE_ZERO => AmountMustNotBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_LEDGER_MUST_NOT_BE_ZERO => LedgerMustNotBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_CODE_MUST_NOT_BE_ZERO => CodeMustNotBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_DEBIT_ACCOUNT_NOT_FOUND => DebitAccountNotFound,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_CREDIT_ACCOUNT_NOT_FOUND => CreditAccountNotFound,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_ACCOUNTS_MUST_HAVE_THE_SAME_LEDGER => AccountsMustHaveTheSameLedger,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_TRANSFER_MUST_HAVE_THE_SAME_LEDGER_AS_ACCOUNTS => TransferMustHaveTheSameLedgerAsAccounts,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_NOT_FOUND => PendingTransferNotFound,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_NOT_PENDING => PendingTransferNotPending,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_HAS_DIFFERENT_DEBIT_ACCOUNT_ID => PendingTransferHasDifferentDebitAccountId,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_HAS_DIFFERENT_CREDIT_ACCOUNT_ID => PendingTransferHasDifferentCreditAccountId,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_HAS_DIFFERENT_LEDGER => PendingTransferHasDifferentLedger,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_HAS_DIFFERENT_CODE => PendingTransferHasDifferentCode,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXCEEDS_PENDING_TRANSFER_AMOUNT => ExceedsPendingTransferAmount,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_HAS_DIFFERENT_AMOUNT => PendingTransferHasDifferentAmount,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_ALREADY_POSTED => PendingTransferAlreadyPosted,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_ALREADY_VOIDED => PendingTransferAlreadyVoided,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_PENDING_TRANSFER_EXPIRED => PendingTransferExpired,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_IMPORTED_EVENT_TIMESTAMP_MUST_NOT_REGRESS => ImportedEventTimestampMustNotRegress,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_IMPORTED_EVENT_TIMESTAMP_MUST_POSTDATE_DEBIT_ACCOUNT => ImportedEventTimestampMustPostdateDebitAccount,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_IMPORTED_EVENT_TIMESTAMP_MUST_POSTDATE_CREDIT_ACCOUNT => ImportedEventTimestampMustPostdateCreditAccount,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_IMPORTED_EVENT_TIMEOUT_MUST_BE_ZERO => ImportedEventTimeoutMustBeZero,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_DEBIT_ACCOUNT_ALREADY_CLOSED => DebitAccountAlreadyClosed,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_CREDIT_ACCOUNT_ALREADY_CLOSED => CreditAccountAlreadyClosed,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OVERFLOWS_DEBITS_PENDING => OverflowsDebitsPending,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OVERFLOWS_CREDITS_PENDING => OverflowsCreditsPending,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OVERFLOWS_DEBITS_POSTED => OverflowsDebitsPosted,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OVERFLOWS_CREDITS_POSTED => OverflowsCreditsPosted,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OVERFLOWS_DEBITS => OverflowsDebits,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OVERFLOWS_CREDITS => OverflowsCredits,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_OVERFLOWS_TIMEOUT => OverflowsTimeout,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXCEEDS_CREDITS => ExceedsCredits,
            TB_CREATE_TRANSFER_RESULT_TB_CREATE_TRANSFER_EXCEEDS_DEBITS => ExceedsDebits,
            v => Unknown(v),
        }
    }
}

impl From<i32> for Status {
    fn from(other: i32) -> Status {
        use tbc::*;
        use Status::*;

        match other {
            TB_STATUS_TB_STATUS_SUCCESS => panic!(),
            TB_STATUS_TB_STATUS_UNEXPECTED => Unexpected,
            TB_STATUS_TB_STATUS_OUT_OF_MEMORY => OutOfMemory,
            TB_STATUS_TB_STATUS_ADDRESS_INVALID => AddressInvalid,
            TB_STATUS_TB_STATUS_ADDRESS_LIMIT_EXCEEDED => AddressLimitExceeded,
            TB_STATUS_TB_STATUS_SYSTEM_RESOURCES => SystemResources,
            TB_STATUS_TB_STATUS_NETWORK_SUBSYSTEM => NetworkSubsystem,
            v => Unknown(v),
        }
    }
}

impl From<u8> for PacketStatus {
    fn from(other: u8) -> PacketStatus {
        use tbc::*;
        use PacketStatus::*;

        match other {
            TB_PACKET_STATUS_TB_PACKET_OK => panic!(),
            TB_PACKET_STATUS_TB_PACKET_TOO_MUCH_DATA => TooMuchData,
            TB_PACKET_STATUS_TB_PACKET_CLIENT_EVICTED => ClientEvicted,
            TB_PACKET_STATUS_TB_PACKET_CLIENT_RELEASE_TOO_LOW => ClientReleaseTooLow,
            TB_PACKET_STATUS_TB_PACKET_CLIENT_RELEASE_TOO_HIGH => ClientReleaseTooHigh,
            TB_PACKET_STATUS_TB_PACKET_CLIENT_SHUTDOWN => ClientShutdown,
            TB_PACKET_STATUS_TB_PACKET_INVALID_OPERATION => InvalidOperation,
            TB_PACKET_STATUS_TB_PACKET_INVALID_DATA_SIZE => InvalidDataSize,
            v => Unknown(v),
        }
    }
}
