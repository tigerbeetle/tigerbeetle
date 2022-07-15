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
	AccountLedgerMustNotBeZero               CreateAccountResult = 5
	AccountCodeMustNotBeZero                 CreateAccountResult = 6
	AccountMutuallyExclusiveFlags            CreateAccountResult = 7
	AccountOverflowsDebits                   CreateAccountResult = 8
	AccountOverflowsCredits                  CreateAccountResult = 9
	AccountExceedsCredits                    CreateAccountResult = 10
	AccountExceedsDebits                     CreateAccountResult = 11
	AccountExistsWithDifferentFlags          CreateAccountResult = 12
	AccountExistsWithDifferentUserData       CreateAccountResult = 13
	AccountExistsWithDifferentLedger         CreateAccountResult = 14
	AccountExistsWithDifferentCode           CreateAccountResult = 15
	AccountExistsWithDifferentDebitsPending  CreateAccountResult = 16
	AccountExistsWithDifferentDebitsPosted   CreateAccountResult = 17
	AccountExistsWithDifferentCreditsPending CreateAccountResult = 18
	AccountExistsWithDifferentCreditsPosted  CreateAccountResult = 19
	AccountExists                            CreateAccountResult = 20
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
	TransferDebitAccountIdMustNotBeZero                CreateTransferResult = 5
	TransferCreditAccountIdMustNotBeZero               CreateTransferResult = 6
	TransferAccountsMustBeDifferent                    CreateTransferResult = 7
	TransferPendingIdMustBeZero                        CreateTransferResult = 8
	TransferPendingTransferMustTimeout                 CreateTransferResult = 9
	TransferLedgerMustNotBeZero                        CreateTransferResult = 10
	TransferCodeMustNotBeZero                          CreateTransferResult = 11
	TransferAmountMustNotBeZero                        CreateTransferResult = 12
	TransferDebitAccountNotFound                       CreateTransferResult = 13
	TransferCreditAccountNotFound                      CreateTransferResult = 14
	TransferAccountsMustHaveTheSameLedger              CreateTransferResult = 15
	TransferTransferMustHaveTheSameLedgerAsAccounts    CreateTransferResult = 16
	TransferExistsWithDifferentFlags                   CreateTransferResult = 17
	TransferExistsWithDifferentDebitAccountId          CreateTransferResult = 18
	TransferExistsWithDifferentCreditAccountId         CreateTransferResult = 19
	TransferExistsWithDifferentUserData                CreateTransferResult = 20
	TransferExistsWithDifferentPendingId               CreateTransferResult = 21
	TransferExistsWithDifferentTimeout                 CreateTransferResult = 22
	TransferExistsWithDifferentCode                    CreateTransferResult = 23
	TransferExistsWithDifferentAmount                  CreateTransferResult = 24
	TransferExists                                     CreateTransferResult = 25
	TransferOverflowsDebitsPending                     CreateTransferResult = 26
	TransferOverflowsCreditsPending                    CreateTransferResult = 27
	TransferOverflowsDebitsPosted                      CreateTransferResult = 28
	TransferOverflowsCreditsPosted                     CreateTransferResult = 29
	TransferOverflowsDebits                            CreateTransferResult = 30
	TransferOverflowsCredits                           CreateTransferResult = 31
	TransferExceedsCredits                             CreateTransferResult = 32
	TransferExceedsDebits                              CreateTransferResult = 33
	TransferCannotPostAndVoidPendingTransfer           CreateTransferResult = 34
	TransferPendingTransferCannotPostOrVoidAnother     CreateTransferResult = 35
	TransferTimeoutReservedForPendingTransfer          CreateTransferResult = 36
	TransferPendingIdMustNotBeZero                     CreateTransferResult = 37
	TransferPendingIdMustBeDifferent                   CreateTransferResult = 38
	TransferPendingTransferNotFound                    CreateTransferResult = 39
	TransferPendingTransferNotPending                  CreateTransferResult = 40
	TransferPendingTransferHasDifferentDebitAccountId  CreateTransferResult = 41
	TransferPendingTransferHasDifferentCreditAccountId CreateTransferResult = 42
	TransferPendingTransferHasDifferentLedger          CreateTransferResult = 43
	TransferPendingTransferHasDifferentCode            CreateTransferResult = 44
	TransferExceedsPendingTransferAmount               CreateTransferResult = 45
	TransferPendingTransferHasDifferentAmount          CreateTransferResult = 46
	TransferPendingTransferAlreadyPosted               CreateTransferResult = 47
	TransferPendingTransferAlreadyVoided               CreateTransferResult = 48
	TransferPendingTransferExpired                     CreateTransferResult = 49
)
