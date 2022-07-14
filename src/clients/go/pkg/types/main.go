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

// EventResult is returned from TB only when an error occurred processing it.
type EventResult struct {
	Index uint32
	Code  AccountResult
}

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

// AccountEventResult is the wrapper of EventResult that with the Code casted to and spesified type.
type AccountEventResult struct {
	Index uint32
	Code  AccountResult
}

type AccountResult uint32

// Valid returns true if the result is known to the driver. If not it might be time to update.
func (ar AccountResult) Valid() bool {
	return ar > 0 && ar < accountResultSentinel
}

//go:generate stringer -type=AccountResult -trimprefix=Account

const (
	AccountLinkedEventFailed                 AccountResult = 1
	AccountReservedFlag                      AccountResult = 2
	AccountReservedField                     AccountResult = 3
	AccountIdMustNotBeZero                   AccountResult = 4
	AccountLedgerMustNotBeZero               AccountResult = 5
	AccountCodeMustNotBeZero                 AccountResult = 6
	AccountMutuallyExclusiveFlags            AccountResult = 7
	AccountOverflowsDebits                   AccountResult = 8
	AccountOverflowsCredits                  AccountResult = 9
	AccountExceedsCredits                    AccountResult = 10
	AccountExceedsDebits                     AccountResult = 11
	AccountExistsWithDifferentFlags          AccountResult = 12
	AccountExistsWithDifferentUserData       AccountResult = 13
	AccountExistsWithDifferentLedger         AccountResult = 14
	AccountExistsWithDifferentCode           AccountResult = 15
	AccountExistsWithDifferentDebitsPending  AccountResult = 16
	AccountExistsWithDifferentDebitsPosted   AccountResult = 17
	AccountExistsWithDifferentCreditsPending AccountResult = 18
	AccountExistsWithDifferentCreditsPosted  AccountResult = 19
	AccountExists                            AccountResult = 20
	accountResultSentinel
)

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

// TransferEventResult is returned from TB only when an error occurred processing it.
type TransferEventResult struct {
	Index uint32
	Code  AccountResult
}

type TransferResult uint32

// Valid returns true if the result is known to the driver. If not it might be time to update.
func (tr TransferResult) Valid() bool {
	return tr > 0 && tr < transferSentinel
}

//go:generate stringer -type=TransferResult -trimprefix=Transfer

const (
	TransferLinkedEventFailed                          TransferResult = 1
	TransferReservedFlag                               TransferResult = 2
	TransferReservedField                              TransferResult = 3
	TransferIdMustNotBeZero                            TransferResult = 4
	TransferDebitAccountIdMustNotBeZero                TransferResult = 5
	TransferCreditAccountIdMustNotBeZero               TransferResult = 6
	TransferAccountsMustBeDifferent                    TransferResult = 7
	TransferPendingIdMustBeZero                        TransferResult = 8
	TransferPendingTransferMustTimeout                 TransferResult = 9
	TransferLedgerMustNotBeZero                        TransferResult = 10
	TransferCodeMustNotBeZero                          TransferResult = 11
	TransferAmountMustNotBeZero                        TransferResult = 12
	TransferDebitAccountNotFound                       TransferResult = 13
	TransferCreditAccountNotFound                      TransferResult = 14
	TransferAccountsMustHaveTheSameLedger              TransferResult = 15
	TransferTransferMustHaveTheSameLedgerAsAccounts    TransferResult = 16
	TransferExistsWithDifferentFlags                   TransferResult = 17
	TransferExistsWithDifferentDebitAccountId          TransferResult = 18
	TransferExistsWithDifferentCreditAccountId         TransferResult = 19
	TransferExistsWithDifferentUserData                TransferResult = 20
	TransferExistsWithDifferentPendingId               TransferResult = 21
	TransferExistsWithDifferentTimeout                 TransferResult = 22
	TransferExistsWithDifferentCode                    TransferResult = 23
	TransferExistsWithDifferentAmount                  TransferResult = 24
	TransferExists                                     TransferResult = 25
	TransferOverflowsDebitsPending                     TransferResult = 26
	TransferOverflowsCreditsPending                    TransferResult = 27
	TransferOverflowsDebitsPosted                      TransferResult = 28
	TransferOverflowsCreditsPosted                     TransferResult = 29
	TransferOverflowsDebits                            TransferResult = 30
	TransferOverflowsCredits                           TransferResult = 31
	TransferExceedsCredits                             TransferResult = 32
	TransferExceedsDebits                              TransferResult = 33
	TransferCannotPostAndVoidPendingTransfer           TransferResult = 34
	TransferPendingTransferCannotPostOrVoidAnother     TransferResult = 35
	TransferTimeoutReservedForPendingTransfer          TransferResult = 36
	TransferPendingIdMustNotBeZero                     TransferResult = 37
	TransferPendingIdMustBeDifferent                   TransferResult = 38
	TransferPendingTransferNotFound                    TransferResult = 39
	TransferPendingTransferNotPending                  TransferResult = 40
	TransferPendingTransferHasDifferentDebitAccountId  TransferResult = 41
	TransferPendingTransferHasDifferentCreditAccountId TransferResult = 42
	TransferPendingTransferHasDifferentLedger          TransferResult = 43
	TransferPendingTransferHasDifferentCode            TransferResult = 44
	TransferExceedsPendingTransferAmount               TransferResult = 45
	TransferPendingTransferHasDifferentAmount          TransferResult = 46
	TransferPendingTransferAlreadyPosted               TransferResult = 47
	TransferPendingTransferAlreadyVoided               TransferResult = 48
	TransferPendingTransferExpired                     TransferResult = 49
	transferSentinel                                   TransferResult = 50 // Must be last for validation
)
