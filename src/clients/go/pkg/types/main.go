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

//go:generate go run golang.org/x/tools/cmd/stringer -type=CreateAccountResult -trimprefix=Account

const (
	AccountLinkedEventFailed           CreateAccountResult = 1
	AccountLinkedEventChainOpen        CreateAccountResult = 2
	AccountReservedFlag                CreateAccountResult = 3
	AccountReservedField               CreateAccountResult = 4
	AccountIdMustNotBeZero             CreateAccountResult = 5
	AccountLedgerMustNotBeZero         CreateAccountResult = 6
	AccountCodeMustNotBeZero           CreateAccountResult = 7
	AccountDebitsPendingMustBeZero     CreateAccountResult = 8
	AccountDebitsPostedMustBeZero      CreateAccountResult = 9
	AccountCreditsPendingMustBeZero    CreateAccountResult = 10
	AccountCreditsPostedMustBeZero     CreateAccountResult = 11
	AccountMutuallyExclusiveFlags      CreateAccountResult = 12
	AccountOverflowsDebits             CreateAccountResult = 13
	AccountOverflowsCredits            CreateAccountResult = 14
	AccountExceedsCredits              CreateAccountResult = 15
	AccountExceedsDebits               CreateAccountResult = 16
	AccountExistsWithDifferentFlags    CreateAccountResult = 17
	AccountExistsWithDifferentUserData CreateAccountResult = 18
	AccountExistsWithDifferentLedger   CreateAccountResult = 19
	AccountExistsWithDifferentCode     CreateAccountResult = 20
	AccountExist                       CreateAccountResult = 21
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

//go:generate go run golang.org/x/tools/cmd/stringer -type=CreateTransferResult -trimprefix=Transfer

const (
	TransferLinkedEventFailed                          CreateTransferResult = 1
	TransferLinkedEventChainOpen                       CreateTransferResult = 2
	TransferReservedFlag                               CreateTransferResult = 3
	TransferReservedField                              CreateTransferResult = 4
	TransferIdMustNotBeZero                            CreateTransferResult = 5
	TransferDebitAccountIdMustNotBeZero                CreateTransferResult = 6
	TransferCreditAccountIdMustNotBeZero               CreateTransferResult = 7
	TransferAccountsMustBeDifferent                    CreateTransferResult = 8
	TransferPendingIdMustBeZero                        CreateTransferResult = 9
	TransferPendingTransferMustTimeout                 CreateTransferResult = 10
	TransferLedgerMustNotBeZero                        CreateTransferResult = 11
	TransferCodeMustNotBeZero                          CreateTransferResult = 12
	TransferAmountMustNotBeZero                        CreateTransferResult = 13
	TransferDebitAccountNotFound                       CreateTransferResult = 14
	TransferCreditAccountNotFound                      CreateTransferResult = 15
	TransferAccountsMustHaveTheSameLedger              CreateTransferResult = 16
	TransferTransferMustHaveTheSameLedgerAsAccounts    CreateTransferResult = 17
	TransferExistsWithDifferentFlags                   CreateTransferResult = 18
	TransferExistsWithDifferentDebitAccountId          CreateTransferResult = 19
	TransferExistsWithDifferentCreditAccountId         CreateTransferResult = 20
	TransferExistsWithDifferentUserData                CreateTransferResult = 21
	TransferExistsWithDifferentPendingId               CreateTransferResult = 22
	TransferExistsWithDifferentTimeout                 CreateTransferResult = 23
	TransferExistsWithDifferentCode                    CreateTransferResult = 24
	TransferExistsWithDifferentAmount                  CreateTransferResult = 25
	TransferExists                                     CreateTransferResult = 26
	TransferOverflowsDebitsPending                     CreateTransferResult = 27
	TransferOverflowsCreditsPending                    CreateTransferResult = 28
	TransferOverflowsDebitsPosted                      CreateTransferResult = 29
	TransferOverflowsCreditsPosted                     CreateTransferResult = 30
	TransferOverflowsDebits                            CreateTransferResult = 31
	TransferOverflowsCredits                           CreateTransferResult = 32
	TransferExceedsCredits                             CreateTransferResult = 33
	TransferExceedsDebits                              CreateTransferResult = 34
	TransferCannotPostAndVoidPendingTransfer           CreateTransferResult = 35
	TransferPendingTransferCannotPostOrVoidAnother     CreateTransferResult = 36
	TransferTimeoutReservedForPendingTransfer          CreateTransferResult = 37
	TransferPendingIdMustNotBeZero                     CreateTransferResult = 38
	TransferPendingIdMustBeDifferent                   CreateTransferResult = 39
	TransferPendingTransferNotFound                    CreateTransferResult = 40
	TransferPendingTransferNotPending                  CreateTransferResult = 41
	TransferPendingTransferHasDifferentDebitAccountId  CreateTransferResult = 42
	TransferPendingTransferHasDifferentCreditAccountId CreateTransferResult = 43
	TransferPendingTransferHasDifferentLedger          CreateTransferResult = 44
	TransferPendingTransferHasDifferentCode            CreateTransferResult = 45
	TransferExceedsPendingTransferAmount               CreateTransferResult = 46
	TransferPendingTransferHasDifferentAmount          CreateTransferResult = 47
	TransferPendingTransferAlreadyPosted               CreateTransferResult = 48
	TransferPendingTransferAlreadyVoided               CreateTransferResult = 49
	TransferPendingTransferExpired                     CreateTransferResult = 50
)
