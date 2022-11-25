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

	// Convert little-endian number to big-endian string
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}

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

// HexStringToUint128 converts a hex-encoded integer to a Uint128.
func HexStringToUint128(value string) (Uint128, error) {
	if len(value) > 32 {
		return Uint128{}, fmt.Errorf("Uint128 hex string must not be more than 32 bytes.")
	}
	if len(value)%2 == 1 {
		value = "0" + value
	}

	bytes := [16]byte{}
	nonZeroLen, err := hex.Decode(bytes[:], []byte(value))
	if err != nil {
		return Uint128{}, err
	}

	// Convert big-endian string to little endian number
	for i := 0; i < nonZeroLen/2; i += 1 {
		j := nonZeroLen - 1 - i
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}

	return BytesToUint128(bytes), nil
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
	AccountExistsWithDifferentFlags    CreateAccountResult = 14
	AccountExistsWithDifferentUserData CreateAccountResult = 15
	AccountExistsWithDifferentLedger   CreateAccountResult = 16
	AccountExistsWithDifferentCode     CreateAccountResult = 17
	AccountExists                      CreateAccountResult = 18
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
	TransferOverflowsTimeout                           CreateTransferResult = 35
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
