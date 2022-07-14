package types

/*
#include "../native/tb_client.h"
*/
import "C"

type Operation uint8

const (
	CREATE_ACCOUNT  Operation = 3
	CREATE_TRANSFER Operation = 4
	ACCOUNT_LOOKUP  Operation = 5
	TRANSFER_LOOKUP Operation = 6
)

type Uint128 C.tb_uint128_t

type Account struct {
	ID             Uint128
	UserData       Uint128
	Reserved       [48]uint8
	Ledger         uint32
	Code           uint16
	Flags          uint16
	DebitsPending  uint64
	DebitsPosted   uint64
	CreditsPending uint64
	CreditsPosted  uint64
	TimeStamp      uint64 // Set this to 0 - the actual value will be set by TigerBeetle server
}

type AccountFlags struct {
	Linked                     bool
	DebitsMustNotExceedCredits bool
	CreditsMustNotExceedDebits bool
}

func (f AccountFlags) ToUint16() uint16 {
	var ret uint16 = 0

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
	AccountLinkedEventFailed                 uint32 = 1
	AccountReservedFlag                      uint32 = 2
	AccountReservedField                     uint32 = 3
	AccountIdMustNotBeZero                   uint32 = 4
	AccountLedgerMustNotBeZero               uint32 = 5
	AccountCodeMustNotBeZero                 uint32 = 6
	AccountMutuallyExclusiveFlags            uint32 = 7
	AccountOverflowsDebits                   uint32 = 8
	AccountOverflowsCredits                  uint32 = 9
	AccountExceedsCredits                    uint32 = 10
	AccountExceedsDebits                     uint32 = 11
	AccountExistsWithDifferentFlags          uint32 = 12
	AccountExistsWithDifferentUserData       uint32 = 13
	AccountExistsWithDifferentLedger         uint32 = 14
	AccountExistsWithDifferentCode           uint32 = 15
	AccountExistsWithDifferentDebitsPending  uint32 = 16
	AccountExistsWithDifferentDebitsPosted   uint32 = 17
	AccountExistsWithDifferentCreditsPending uint32 = 18
	AccountExistsWithDifferentCreditsPosted  uint32 = 19
	AccountExists                            uint32 = 20
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
	UserData        Uint128 // Opaque third-party identifier to link this transfer (many-to-one) to an external entity:
	Reserved        Uint128 // Reserved for accounting policy primitives:
	PendingID       Uint128
	Timeout         uint64
	Ledger          uint32
	Code            uint16 // A chart of accounts code describing the reason for the transfer (e.g. deposit, settlement):
	Flags           uint16
	Amount          uint64
	Timestamp       uint64
}

type TransferFlags struct {
	Linked              bool
	Pending             bool
	PostPendingTransfer bool
	VoidPendingTransfer bool
}

func (f TransferFlags) ToUint16() uint16 {
	var ret uint16 = 0

	if f.Linked {
		ret |= (1 << 0)
	}

	if f.Pending {
		ret |= (1 << 1)
	}

	if f.PostPendingTransfer {
		ret |= (1 << 2)
	}

	if f.VoidPendingTransfer {
		ret |= (1 << 3)
	}

	return ret
}

const (
	TransferLinkedEventFailed                          uint32 = 1
	TransferReservedFlag                               uint32 = 2
	TransferReservedField                              uint32 = 3
	TransferIdMustNotBeZero                            uint32 = 4
	TransferDebitAccountIdMustNotBeZero                uint32 = 5
	TransferCreditAccountIdMustNotBeZero               uint32 = 6
	TransferAccountsMustBeDifferent                    uint32 = 7
	TransferPendingIdMustBeZero                        uint32 = 8
	TransferPendingTransferMustTimeout                 uint32 = 9
	TransferLedgerMustNotBeZero                        uint32 = 10
	TransferCodeMustNotBeZero                          uint32 = 11
	TransferAmountMustNotBeZero                        uint32 = 12
	TransferDebitAccountNotFound                       uint32 = 13
	TransferCreditAccountNotFound                      uint32 = 14
	TransferAccountsMustHaveTheSameLedger              uint32 = 15
	TransferTransferMustHaveTheSameLedgerAsAccounts    uint32 = 16
	TransferExistsWithDifferentFlags                   uint32 = 17
	TransferExistsWithDifferentDebitAccountId          uint32 = 18
	TransferExistsWithDifferentCreditAccountId         uint32 = 19
	TransferExistsWithDifferentUserData                uint32 = 20
	TransferExistsWithDifferentPendingId               uint32 = 21
	TransferExistsWithDifferentTimeout                 uint32 = 22
	TransferExistsWithDifferentCode                    uint32 = 23
	TransferExistsWithDifferentAmount                  uint32 = 24
	TransferExists                                     uint32 = 25
	TransferOverflowsDebitsPending                     uint32 = 26
	TransferOverflowsCreditsPending                    uint32 = 27
	TransferOverflowsDebitsPosted                      uint32 = 28
	TransferOverflowsCreditsPosted                     uint32 = 29
	TransferOverflowsDebits                            uint32 = 30
	TransferOverflowsCredits                           uint32 = 31
	TransferExceedsCredits                             uint32 = 32
	TransferExceedsDebits                              uint32 = 33
	TransferCannotPostAndVoidPendingTransfer           uint32 = 34
	TransferPendingTransferCannotPostOrVoidAnother     uint32 = 35
	TransferTimeoutReservedForPendingTransfer          uint32 = 36
	TransferPendingIdMustNotBeZero                     uint32 = 37
	TransferPendingIdMustBeDifferent                   uint32 = 38
	TransferPendingTransferNotFound                    uint32 = 39
	TransferPendingTransferNotPending                  uint32 = 40
	TransferPendingTransferHasDifferentDebitAccountId  uint32 = 41
	TransferPendingTransferHasDifferentCreditAccountId uint32 = 42
	TransferPendingTransferHasDifferentLedger          uint32 = 43
	TransferPendingTransferHasDifferentCode            uint32 = 44
	TransferExceedsPendingTransferAmount               uint32 = 45
	TransferPendingTransferHasDifferentAmount          uint32 = 46
	TransferPendingTransferAlreadyPosted               uint32 = 47
	TransferPendingTransferAlreadyVoided               uint32 = 48
	TransferPendingTransferExpired                     uint32 = 49
)
