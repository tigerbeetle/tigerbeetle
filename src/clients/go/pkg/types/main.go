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
	Code  uint32
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

// AccountEventResult is an EventResult with typed codes for create account operations.
type AccountEventResult struct {
	Index uint32
	Code  CreateAccountResult
}

type CreateAccountResult uint32

//go:generate stringer -type=CreateAccountResult -trimprefix=Account

const (
	AccountLinkedEventFailed                 CreateAccountResult = 1
	AccountReservedFlag                      CreateAccountResult = 2
	AccountReservedField                     CreateAccountResult = 3
	AccountIdMustNotBeZero                   CreateAccountResult = 4
	AccountIdMustNotBeMaxInt                 CreateAccountResult = 5
	AccountLedgerMustNotBeZero               CreateAccountResult = 6
	AccountCodeMustNotBeZero                 CreateAccountResult = 7
	AccountMutuallyExclusiveFlags            CreateAccountResult = 8
	AccountOverflowsDebits                   CreateAccountResult = 9
	AccountOverflowsCredits                  CreateAccountResult = 10
	AccountExceedsCredits                    CreateAccountResult = 11
	AccountExceedsDebits                     CreateAccountResult = 12
	AccountExistsWithDifferentFlags          CreateAccountResult = 13
	AccountExistsWithDifferentUserData       CreateAccountResult = 14
	AccountExistsWithDifferentLedger         CreateAccountResult = 15
	AccountExistsWithDifferentCode           CreateAccountResult = 16
	AccountExistsWithDifferentDebitsPending  CreateAccountResult = 17
	AccountExistsWithDifferentDebitsPosted   CreateAccountResult = 18
	AccountExistsWithDifferentCreditsPending CreateAccountResult = 19
	AccountExistsWithDifferentCreditsPosted  CreateAccountResult = 20
	AccountExists                            CreateAccountResult = 21
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

// TransferEventResult is an EventResult with typed codes for create transfer operations.
type TransferEventResult struct {
	Index uint32
	Code  CreateTransferResult
}

type CreateTransferResult uint32

//go:generate stringer -type=CreateTransferResult -trimprefix=Transfer

const (
	TransferLinkedEventFailed                          CreateTransferResult = 1
	TransferReservedFlag                               CreateTransferResult = 2
	TransferReservedField                              CreateTransferResult = 3
	TransferIdMustNotBeZero                            CreateTransferResult = 4
	TransferIdMustNotBeMaxInt                          CreateTransferResult = 5
	TransferDebitAccountIdMustNotBeZero                CreateTransferResult = 6
	TransferDebitAccountIdMustNotBeMaxInt              CreateTransferResult = 7
	TransferCreditAccountIdMustNotBeZero               CreateTransferResult = 8
	TransferCreditAccountIdMustNotBeMaxInt             CreateTransferResult = 9
	TransferAccountsMustBeDifferent                    CreateTransferResult = 10
	TransferPendingIdMustBeZero                        CreateTransferResult = 11
	TransferPendingTransferMustTimeout                 CreateTransferResult = 12
	TransferLedgerMustNotBeZero                        CreateTransferResult = 13
	TransferCodeMustNotBeZero                          CreateTransferResult = 14
	TransferAmountMustNotBeZero                        CreateTransferResult = 15
	TransferDebitAccountNotFound                       CreateTransferResult = 16
	TransferCreditAccountNotFound                      CreateTransferResult = 17
	TransferAccountsMustHaveTheSameLedger              CreateTransferResult = 18
	TransferTransferMustHaveTheSameLedgerAsAccounts    CreateTransferResult = 19
	TransferExistsWithDifferentFlags                   CreateTransferResult = 20
	TransferExistsWithDifferentDebitAccountId          CreateTransferResult = 21
	TransferExistsWithDifferentCreditAccountId         CreateTransferResult = 22
	TransferExistsWithDifferentUserData                CreateTransferResult = 23
	TransferExistsWithDifferentPendingId               CreateTransferResult = 24
	TransferExistsWithDifferentTimeout                 CreateTransferResult = 25
	TransferExistsWithDifferentCode                    CreateTransferResult = 26
	TransferExistsWithDifferentAmount                  CreateTransferResult = 27
	TransferExists                                     CreateTransferResult = 28
	TransferOverflowsDebitsPending                     CreateTransferResult = 29
	TransferOverflowsCreditsPending                    CreateTransferResult = 30
	TransferOverflowsDebitsPosted                      CreateTransferResult = 31
	TransferOverflowsCreditsPosted                     CreateTransferResult = 32
	TransferOverflowsDebits                            CreateTransferResult = 33
	TransferOverflowsCredits                           CreateTransferResult = 34
	TransferExceedsCredits                             CreateTransferResult = 35
	TransferExceedsDebits                              CreateTransferResult = 36
	TransferCannotPostAndVoidPendingTransfer           CreateTransferResult = 37
	TransferPendingTransferCannotPostOrVoidAnother     CreateTransferResult = 38
	TransferTimeoutReservedForPendingTransfer          CreateTransferResult = 39
	TransferPendingIdMustNotBeZero                     CreateTransferResult = 40
	TransferPendingIdMustNotBeMaxInt                   CreateTransferResult = 41
	TransferPendingIdMustBeDifferent                   CreateTransferResult = 42
	TransferPendingTransferNotFound                    CreateTransferResult = 43
	TransferPendingTransferNotPending                  CreateTransferResult = 44
	TransferPendingTransferHasDifferentDebitAccountId  CreateTransferResult = 45
	TransferPendingTransferHasDifferentCreditAccountId CreateTransferResult = 46
	TransferPendingTransferHasDifferentLedger          CreateTransferResult = 47
	TransferPendingTransferHasDifferentCode            CreateTransferResult = 48
	TransferExceedsPendingTransferAmount               CreateTransferResult = 49
	TransferPendingTransferHasDifferentAmount          CreateTransferResult = 50
	TransferPendingTransferAlreadyPosted               CreateTransferResult = 51
	TransferPendingTransferAlreadyVoided               CreateTransferResult = 52
	TransferPendingTransferExpired                     CreateTransferResult = 53
)
