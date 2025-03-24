package types

/*
#include "../native/tb_client.h"
*/
import "C"
import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"time"
	"unsafe"
)

type Uint128 C.tb_uint128_t

func (value Uint128) Bytes() [16]byte {
	return *(*[16]byte)(unsafe.Pointer(&value))
}

func swapEndian(bytes []byte) {
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}
}

func (value Uint128) String() string {
	bytes := value.Bytes()

	// Convert little-endian Uint128 number to big-endian string.
	swapEndian(bytes[:])
	s := hex.EncodeToString(bytes[:16])

	// Prettier to drop preceding zeros so you get "0" instead of "0000000000000000".
	lastNonZero := 0
	for s[lastNonZero] == '0' && lastNonZero < len(s)-1 {
		lastNonZero++
	}
	return s[lastNonZero:]
}

func (value Uint128) BigInt() big.Int {
	// big.Int uses bytes in big-endian but Uint128 stores bytes in little endian, so reverse it.
	bytes := value.Bytes()
	swapEndian(bytes[:])

	ret := big.Int{}
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
	if value.Sign() < 0 {
		panic("cannot convert negative big.Int to Uint128")
	}

	// big.Int bytes are big-endian so convert them to little-endian for Uint128 bytes.
	bytes := value.Bytes()
	swapEndian(bytes[:])

	// Only cast slice to bytes when there's enough.
	if len(bytes) >= 16 {
		return BytesToUint128(*(*[16]byte)(bytes))
	}

	var zeroPadded [16]byte
	copy(zeroPadded[:], bytes)
	return BytesToUint128(zeroPadded)
}

// ToUint128 converts a integer to a Uint128.
func ToUint128(value uint64) Uint128 {
	values := [2]uint64{value, 0}
	return *(*Uint128)(unsafe.Pointer(&values[0]))
}

var idLastTimestamp int64
var idLastRandom [10]byte
var idMutex sync.Mutex

// Generates a Universally Unique and Sortable Identifier based on https://github.com/ulid/spec.
// Uint128 returned are guaranteed to be monotonically increasing when interpreted as little-endian.
// `ID()` is safe to call from multiple goroutines with monotonicity being sequentially consistent.
func ID() Uint128 {
	timestamp := time.Now().UnixMilli()

	// Lock the mutex for global id variables.
	// Then ensure lastTimestamp is monotonically increasing & lastRandom changes each millisecond
	idMutex.Lock()
	if timestamp <= idLastTimestamp {
		timestamp = idLastTimestamp
	} else {
		idLastTimestamp = timestamp
		_, err := rand.Read(idLastRandom[:])
		if err != nil {
			idMutex.Unlock()
			panic("crypto.rand failed to provide random bytes")
		}
	}

	// Read out a uint80 from lastRandom as a uint64 and uint16.
	randomLo := binary.LittleEndian.Uint64(idLastRandom[:8])
	randomHi := binary.LittleEndian.Uint16(idLastRandom[8:])

	// Increment the random bits as a uint80 together, checking for overflow.
	// Go defines unsigned arithmetic to wrap around on overflow by default so check for zero.
	randomLo += 1
	if randomLo == 0 {
		randomHi += 1
		if randomHi == 0 {
			idMutex.Unlock()
			panic("random bits overflow on monotonic increment")
		}
	}

	// Write incremented uint80 back to lastRandom and stop mutating global id variables.
	binary.LittleEndian.PutUint64(idLastRandom[:8], randomLo)
	binary.LittleEndian.PutUint16(idLastRandom[8:], randomHi)
	idMutex.Unlock()

	// Create Uint128 from new timestamp and random.
	var id [16]byte
	binary.LittleEndian.PutUint64(id[:8], randomLo)
	binary.LittleEndian.PutUint16(id[8:], randomHi)
	binary.LittleEndian.PutUint16(id[10:], (uint16)(timestamp))     // timestamp lo
	binary.LittleEndian.PutUint32(id[12:], (uint32)(timestamp>>16)) // timestamp hi
	return BytesToUint128(id)
}
