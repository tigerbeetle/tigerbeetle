package types

/*
#include "../native/tb_client.h"
*/
import "C"
import "encoding/hex"
import "fmt"
import "unsafe"

type Operation uint8

const (
	CREATE_ACCOUNT  Operation = 3
	CREATE_TRANSFER Operation = 4
	ACCOUNT_LOOKUP  Operation = 5
	TRANSFER_LOOKUP Operation = 6
)

type Uint128 C.tb_uint128_t

func (value Uint128) Bytes() [16]byte {
	return *(*[16]byte)(unsafe.Pointer(&value))
}

func (value Uint128) String() string {
	bytes := value.Bytes()
	s := hex.EncodeToString(bytes[:16])

	// Prettier to drop preceeding zeros so you get "0" instead of "0000000000000000"
	lastNonZero := 0
	for s[lastNonZero] == '0' && lastNonZero < len(s)-1 {
		lastNonZero++
	}
	return s[lastNonZero:]
}

// BytesToUint128 converts a raw [16]byte value to Uint128.
func BytesToUint128(value [16]byte) Uint128 {
	return *(*Uint128)(unsafe.Pointer(&value[0]))
}

// HexBytesToUint128 converts a hex-encoded integer to a Uint128.
func HexBytesToUint128(value [32]byte) (Uint128, error) {
	decoded := [16]byte{}
	_, err := hex.Decode(decoded[:], value[:])
	if err != nil {
		return Uint128{}, err
	}

	return BytesToUint128(decoded), nil
}

// HexStringToUint128 converts a hex-encoded integer to a Uint128.
func HexStringToUint128(value string) (Uint128, error) {
	if len(value) > 32 {
		return Uint128{}, fmt.Errorf("Uint128 hex string must not be more than 32 bytes.")
	}
	if len(value)%2 == 1 {
		value = "0" + value
	}
	// Pad with zeroes
	bytes := [32]byte{}
	for i := range bytes {
		if i < 32-len(value) {
			bytes[i] = '0'
		} else {
			bytes[i] = value[i-(32-len(value))]
		}
	}
	return HexBytesToUint128(bytes)
}

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
	AccountIdMustNotBeIntMax           CreateAccountResult = 6
	AccountLedgerMustNotBeZero         CreateAccountResult = 7
	AccountCodeMustNotBeZero           CreateAccountResult = 8
	AccountDebitsPendingMustBeZero     CreateAccountResult = 9
	AccountDebitsPostedMustBeZero      CreateAccountResult = 10
	AccountCreditsPendingMustBeZero    CreateAccountResult = 11
	AccountCreditsPostedMustBeZero     CreateAccountResult = 12
	AccountMutuallyExclusiveFlags      CreateAccountResult = 13
	AccountOverflowsDebits             CreateAccountResult = 14
	AccountOverflowsCredits            CreateAccountResult = 15
	AccountExceedsCredits              CreateAccountResult = 16
	AccountExceedsDebits               CreateAccountResult = 17
	AccountExistsWithDifferentFlags    CreateAccountResult = 18
	AccountExistsWithDifferentUserData CreateAccountResult = 19
	AccountExistsWithDifferentLedger   CreateAccountResult = 20
	AccountExistsWithDifferentCode     CreateAccountResult = 21
	AccountExists                      CreateAccountResult = 22
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
	TransferIdMustNotBeIntMax                          CreateTransferResult = 6
	TransferDebitAccountIdMustNotBeZero                CreateTransferResult = 7
	TransferDebitAccountIdMustNotBeIntMax              CreateTransferResult = 8
	TransferCreditAccountIdMustNotBeZero               CreateTransferResult = 9
	TransferCreditAccountIdMustNotBeIntMax             CreateTransferResult = 10
	TransferAccountsMustBeDifferent                    CreateTransferResult = 11
	TransferPendingIdMustBeZero                        CreateTransferResult = 12
	TransferPendingTransferMustTimeout                 CreateTransferResult = 13
	TransferLedgerMustNotBeZero                        CreateTransferResult = 14
	TransferCodeMustNotBeZero                          CreateTransferResult = 15
	TransferAmountMustNotBeZero                        CreateTransferResult = 16
	TransferDebitAccountNotFound                       CreateTransferResult = 17
	TransferCreditAccountNotFound                      CreateTransferResult = 18
	TransferAccountsMustHaveTheSameLedger              CreateTransferResult = 19
	TransferTransferMustHaveTheSameLedgerAsAccounts    CreateTransferResult = 20
	TransferExistsWithDifferentFlags                   CreateTransferResult = 21
	TransferExistsWithDifferentDebitAccountId          CreateTransferResult = 22
	TransferExistsWithDifferentCreditAccountId         CreateTransferResult = 23
	TransferExistsWithDifferentUserData                CreateTransferResult = 24
	TransferExistsWithDifferentPendingId               CreateTransferResult = 25
	TransferExistsWithDifferentTimeout                 CreateTransferResult = 26
	TransferExistsWithDifferentCode                    CreateTransferResult = 27
	TransferExistsWithDifferentAmount                  CreateTransferResult = 28
	TransferExists                                     CreateTransferResult = 29
	TransferOverflowsDebitsPending                     CreateTransferResult = 30
	TransferOverflowsCreditsPending                    CreateTransferResult = 31
	TransferOverflowsDebitsPosted                      CreateTransferResult = 32
	TransferOverflowsCreditsPosted                     CreateTransferResult = 33
	TransferOverflowsDebits                            CreateTransferResult = 34
	TransferOverflowsCredits                           CreateTransferResult = 35
	TransferExceedsCredits                             CreateTransferResult = 36
	TransferExceedsDebits                              CreateTransferResult = 37
	TransferCannotPostAndVoidPendingTransfer           CreateTransferResult = 38
	TransferPendingTransferCannotPostOrVoidAnother     CreateTransferResult = 39
	TransferTimeoutReservedForPendingTransfer          CreateTransferResult = 40
	TransferPendingIdMustNotBeZero                     CreateTransferResult = 41
	TransferPendingIdMustNotBeIntMax                   CreateTransferResult = 42
	TransferPendingIdMustBeDifferent                   CreateTransferResult = 43
	TransferPendingTransferNotFound                    CreateTransferResult = 44
	TransferPendingTransferNotPending                  CreateTransferResult = 45
	TransferPendingTransferHasDifferentDebitAccountId  CreateTransferResult = 46
	TransferPendingTransferHasDifferentCreditAccountId CreateTransferResult = 47
	TransferPendingTransferHasDifferentLedger          CreateTransferResult = 48
	TransferPendingTransferHasDifferentCode            CreateTransferResult = 49
	TransferExceedsPendingTransferAmount               CreateTransferResult = 50
	TransferPendingTransferHasDifferentAmount          CreateTransferResult = 51
	TransferPendingTransferAlreadyPosted               CreateTransferResult = 52
	TransferPendingTransferAlreadyVoided               CreateTransferResult = 53
	TransferPendingTransferExpired                     CreateTransferResult = 54
)
