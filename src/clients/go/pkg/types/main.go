package types

/*
#include "../native/tb_client.h"
*/
import "C"

type Operation uint8

const (
	CREATE_ACCOUNT  Operation = 3
	CREATE_TRANSFER Operation = 4
	COMMIT_TRANSFER Operation = 5
	ACCOUNT_LOOKUP  Operation = 6
	TRANSFER_LOOKUP Operation = 7
)

type Uint128 C.tb_uint128_t

type Account struct {
	ID              Uint128
	UserData        Uint128
	Reserved        [48]uint8
	Unit            uint16
	Code            uint16
	Flags           uint32
	DebitsReserved  uint64
	DebitsAccepted  uint64
	CreditsReserved uint64
	CreditsAccepted uint64
	TimeStamp       uint64 // Set this to 0 - the actual value will be set by TigerBeetle server
}

type AccountFlags struct {
	Linked                     bool
	DebitsMustNotExceedCredits bool
	CreditsMustNotExceedDebits bool
}

func (f AccountFlags) ToUint32() uint32 {
	var ret uint32 = 0

	if f.Linked {
		ret |= (1 << 0)
	}

	if f.DebitsMustNotExceedCredits {
		ret |= (1 << 1)
	}

	if f.CreditsMustNotExceedDebits {
		ret |= (1 << 2)
	}

	return ret
}

const (
	AccountLinkedEventFailed                uint32 = 1
	AccountExists                           uint32 = 2
	AccountExistsWithDifferentUserData      uint32 = 3
	AccountExistsWithDifferentReservedField uint32 = 4
	AccountExistsWithDifferentUnit          uint32 = 5
	AccountExistsWithDifferentCode          uint32 = 6
	AccountExistsWithDifferentFlags         uint32 = 7
	AccountExceedsCredits                   uint32 = 8
	AccountExceedsDebits                    uint32 = 9
	AccountReservedField                    uint32 = 10
	AccountReservedFlagPadding              uint32 = 11
)

// An EventResult is returned from TB only when an error occurred processing it.
type EventResult struct {
	Index uint32
	Code  uint32
}

type Transfer struct {
	ID              Uint128
	DebitAccountID  Uint128
	CreditAccountID Uint128
	UserData        Uint128   // Opaque third-party identifier to link this transfer (many-to-one) to an external entity:
	Reserved        [32]uint8 // Reserved for accounting policy primitives:
	Timeout         uint64
	Code            uint32 // A chart of accounts code describing the reason for the transfer (e.g. deposit, settlement):
	Flags           uint32
	Amount          uint64
	Timestamp       uint64
}

type TransferFlags struct {
	Linked         bool
	TwoPhaseCommit bool
	Condition      bool
}

func (f TransferFlags) ToUint32() uint32 {
	var ret uint32 = 0

	if f.Linked {
		ret |= (1 << 0)
	}

	if f.TwoPhaseCommit {
		ret |= (1 << 1)
	}

	if f.Condition {
		ret |= (1 << 2)
	}

	return ret
}

const (
	TransferLinkedEventFailed                    uint32 = 1
	TransferExists                               uint32 = 2
	TransferExistsWithDifferentDebitAccountID    uint32 = 3
	TransferExistsWithDifferentCreditAccountID   uint32 = 4
	TransferExistsWithDifferentUserData          uint32 = 5
	TransferExistsWithDifferentReservedField     uint32 = 6
	TransferExistsWithDifferentCode              uint32 = 7
	TransferExistsWithDifferentAmount            uint32 = 8
	TransferExistsWithDifferentTimeout           uint32 = 9
	TransferExistsWithDifferentFlags             uint32 = 10
	TransferExistsAndAlreadyCommittedAndAccepted uint32 = 11
	TransferExistsAndAlreadyCommittedAndRejected uint32 = 12
	TransferReservedField                        uint32 = 13
	TransferReservedFlagPadding                  uint32 = 14
	TransferDebitAccount_notFound                uint32 = 15
	TransferCreditAccount_notFound               uint32 = 16
	TransferAccountsAreTheSame                   uint32 = 17
	TransferAccountsHaveDifferentUnits           uint32 = 18
	TransferAmountIsZero                         uint32 = 19
	TransferExceedsCredits                       uint32 = 20
	TransferExceedsDebits                        uint32 = 21
	TransferTwoPhaseCommitMustTimeout            uint32 = 21
	TransferTimeoutReservedForTwoPhaseCommit     uint32 = 22
)

type Commit struct {
	ID        Uint128
	Reserved  [32]uint8
	Code      uint32
	Flags     uint32
	Timestamp uint64
}

type CommitFlags struct {
	Linked   bool
	Reject   bool
	Preimage bool
}

func (f CommitFlags) ToUint32() uint32 {
	var ret uint32 = 0
	if f.Linked {
		ret |= (1 << 0)
	}

	if f.Reject {
		ret |= (1 << 1)
	}

	if f.Preimage {
		ret |= (1 << 2)
	}

	return ret
}

const (
	CommitLinkedEventFailed           uint32 = 1
	CommitReservedField               uint32 = 2
	CommitReservedFlagPadding         uint32 = 3
	CommitTransferNotFound            uint32 = 4
	CommitTransferNotTwoPhaseCommit   uint32 = 5
	CommitTransferExpired             uint32 = 6
	CommitAlreadyCommitted            uint32 = 7
	CommitAlreadyCommittedButAccepted uint32 = 8
	CommitAlreadyCommittedButRejected uint32 = 9
	CommitDebitAccountNotFound        uint32 = 10
	CommitCreditAccountNotFound       uint32 = 11
	CommitDebitAmount_wasNotReserved  uint32 = 12
	CommitCreditAmount_wasNotReserved uint32 = 13
	CommitExceedsCredits              uint32 = 14
	CommitExceedsDebits               uint32 = 15
	CommitConditionRequiresPreimage   uint32 = 16
	CommitPreimageRequiresCondition   uint32 = 17
	CommitPreimageInvalid             uint32 = 18
)
