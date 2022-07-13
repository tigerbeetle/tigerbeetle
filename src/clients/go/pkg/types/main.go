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
	AccountLinkedEventFailed                 uint32 = 0
	AccountReservedFlag                      uint32 = 1
	AccountReservedField                     uint32 = 2
	AccountIdMustNotBeZero                   uint32 = 3
	AccountLedgerMustNotBeZero               uint32 = 4
	AccountCodeMustNotBeZero                 uint32 = 5
	AccountMutuallyExclusiveFlags            uint32 = 6
	AccountOverflowsDebits                   uint32 = 7
	AccountOverflowsCredits                  uint32 = 8
	AccountExceedsCredits                    uint32 = 9
	AccountExceedsDebits                     uint32 = 10
	AccountExistsWithDifferentFlags          uint32 = 11
	AccountExistsWithDifferentUserData       uint32 = 12
	AccountExistsWithDifferentLedger         uint32 = 13
	AccountExistsWithDifferentCode           uint32 = 14
	AccountExistsWithDifferentDebitsPending  uint32 = 15
	AccountExistsWithDifferentDebitsPosted   uint32 = 16
	AccountExistsWithDifferentCreditsPending uint32 = 17
	AccountExistsWithDifferentCreditsPosted  uint32 = 18
	AccountExists                            uint32 = 19
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
	VodiPendingTransfer bool
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

	if f.VodiPendingTransfer {
		ret |= (1 << 3)
	}

	return ret
}

const (
	TransferLinkedEventFailed                          uint32 = 0
	TransferReservedFlag                               uint32 = 1
	TransferReservedField                              uint32 = 2
	TransferIdMustNotBeZero                            uint32 = 3
	TransferDebitAccountIdMustNotBeZero                uint32 = 4
	TransferCreditAccountIdMustNotBeZero               uint32 = 5
	TransferAccountsMustBeDifferent                    uint32 = 6
	TransferPendingIdMustBeZero                        uint32 = 7
	TransferPendingTransferMustTimeout                 uint32 = 8
	TransferLedgerMustNotBeZero                        uint32 = 9
	TransferCodeMustNotBeZero                          uint32 = 10
	TransferAmountMustNotBeZero                        uint32 = 11
	TransferDebitAccountNotFound                       uint32 = 12
	TransferCreditAccountNotFound                      uint32 = 13
	TransferAccountsMustHaveTheSameLedger              uint32 = 14
	TransferTransferMustHaveTheSameLedgerAsAccounts    uint32 = 15
	TransferExistsWithDifferentFlags                   uint32 = 16
	TransferExistsWithDifferentDebitAccountId          uint32 = 17
	TransferExistsWithDifferentCreditAccountId         uint32 = 18
	TransferExistsWithDifferentUserData                uint32 = 19
	TransferExistsWithDifferentPendingId               uint32 = 20
	TransferExistsWithDifferentTimeout                 uint32 = 21
	TransferExistsWithDifferentCode                    uint32 = 22
	TransferExistsWithDifferentAmount                  uint32 = 23
	TransferExists                                     uint32 = 24
	TransferOverflowsDebitsPending                     uint32 = 25
	TransferOverflowsCreditsPending                    uint32 = 26
	TransferOverflowsDebitsPosted                      uint32 = 27
	TransferOverflowsCreditsPosted                     uint32 = 28
	TransferOverflowsDebits                            uint32 = 29
	TransferOverflowsCredits                           uint32 = 30
	TransferExceedsCredits                             uint32 = 31
	TransferExceedsDebits                              uint32 = 32
	TransferCannotPostAndVoidPendingTransfer           uint32 = 33
	TransferPendingTransferCannotPostOrVoidAnother     uint32 = 34
	TransferTimeoutReservedForPendingTransfer          uint32 = 35
	TransferPendingIdMustNotBeZero                     uint32 = 36
	TransferPendingIdMustBeDifferent                   uint32 = 37
	TransferPendingTransferNotFound                    uint32 = 38
	TransferPendingTransferNotPending                  uint32 = 39
	TransferPendingTransferHasDifferentDebitAccountId  uint32 = 40
	TransferPendingTransferHasDifferentCreditAccountId uint32 = 41
	TransferPendingTransferHasDifferentLedger          uint32 = 42
	TransferPendingTransferHasDifferentCode            uint32 = 43
	TransferExceedsPendingTransferAmount               uint32 = 44
	TransferPendingTransferHasDifferentAmount          uint32 = 45
	TransferPendingTransferAlreadyPosted               uint32 = 46
	TransferPendingTransferAlreadyVoided               uint32 = 47
	TransferPendingTransferExpired                     uint32 = 48
)
