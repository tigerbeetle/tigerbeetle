package types

/*
#include "../native/tb_client.h"
*/
import "C"
import (
	"encoding/hex"
	"fmt"
	"math/big"
	"unsafe"
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

	// Prettier to drop preceding zeros so you get "0" instead of "0000000000000000"
	lastNonZero := 0
	for s[lastNonZero] == '0' && lastNonZero < len(s)-1 {
		lastNonZero++
	}
	return s[lastNonZero:]
}

func (value Uint128) BigInt() big.Int {
	ret := big.Int{}
	bytes := value.Bytes()
	ret.SetBytes(bytes[:])
	return ret
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

// BigIntToUint128 converts a [math/big.Int] to a Uint128.
func BigIntToUint128(value big.Int) Uint128 {
	bytes := value.Bytes()
	return BytesToUint128(*(*[16]byte)(bytes))
}

// ToUint128 converts a integer to a Uint128.
func ToUint128(value uint64) Uint128 {
	values := [2]uint64{value, 0}
	return *(*Uint128)(unsafe.Pointer(&values[0]))
}
