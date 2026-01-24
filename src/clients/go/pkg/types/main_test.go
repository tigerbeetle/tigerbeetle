package types

import (
	"math/big"
	"sync"
	"testing"
	"time"
)

func Test_HexStringToUint128(t *testing.T) {
	tests := []string{
		"0",
		"1",
		"400",
		"203",
		"ffffffffffffffffffffffffffffffff",
		"123456",
	}

	for _, test := range tests {
		res, err := HexStringToUint128(test)
		if err != nil {
			t.Fatalf("Expected %s to be a valid hex string, got: %s", test, err)
		}
		thereAndBack := res.String()
		if thereAndBack != test {
			t.Fatalf("Expected %s to be %s, got %s", test, test, thereAndBack)
		}
	}
}

func Test_HexStringToUint128_LittleEndian(t *testing.T) {

	test := "123456"

	res, err := HexStringToUint128(test)
	if err != nil {
		t.Fatalf("Expected %s to be a valid hex string, got: %s", test, err)
	}

	expected := [16]byte{86, 52, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	if res.Bytes() != expected {
		t.Fatalf("Expected %s to produce bytes %v, got bytes %v", test, expected, res.Bytes())
	}
}

func Test_BigIntToUint128(t *testing.T) {

	tests := []string{
		"0",
		"1",
		"400",
		"203",
		"ffffffffffffffffffffffffffffffff",
		"123456",
	}

	for _, test := range tests {
		uint128, err := HexStringToUint128(test)

		if err != nil {
			t.Fatalf("Expected %s to be a valid hex string, got: %s", test, err)
		}

		bigint := uint128.BigInt()
		uint128_back := BigIntToUint128(*bigint)
		string_back := uint128_back.String()

		if string_back != test {
			t.Fatalf("Expected %s to be %s, got %s", test, test, string_back)
		}
	}
}

func Test_BigIntToUint128_Negative(t *testing.T) {
	negative := new(big.Int).SetInt64(-1)
	testFunc := func() {
		BigIntToUint128(*negative)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic")
		}
	}()

	testFunc()
	t.Errorf("Expected panic, but execution continued")
}

func Test_ID(t *testing.T) {
	verifier := func() {
		idA := ID()
		for i := 0; i < 1_000_000; i++ {
			if i%1_000 == 0 {
				time.Sleep(1 * time.Millisecond)
			}

			idB := ID()

			// Verify idB and idA are monotonic using BigInts.
			a := idA.BigInt()
			b := idB.BigInt()
			if b.Cmp(a) != 1 {
				t.Fatalf("Expected ID %v to be greater than ID %v", b, a)
			}

			idA = idB
		}
	}

	// Verify monotonic IDs locally.
	verifier()

	// Verify monotonic IDs across multiple threads.
	var barrier, finish sync.WaitGroup
	concurrency := 10
	barrier.Add(concurrency) // To sync up all goroutines before verifier() to maximize contention.
	finish.Add(concurrency)  // To wait for all goroutines to finish running verifier().

	for i := 0; i < concurrency; i++ {
		go func() {
			barrier.Done()
			barrier.Wait()
			verifier()
			finish.Done()
		}()
	}

	finish.Wait()
}

func Test_Uint64(t *testing.T) {
	tests := []struct {
		name        string
		hexString   string
		expected    uint64
		shouldError bool
	}{
		{
			name:        "zero",
			hexString:   "0",
			expected:    0,
			shouldError: false,
		},
		{
			name:        "small value",
			hexString:   "123456",
			expected:    0x123456,
			shouldError: false,
		},
		{
			name:        "max uint64",
			hexString:   "ffffffffffffffff",
			expected:    0xffffffffffffffff,
			shouldError: false,
		},
		{
			name:        "value too large - upper bits set",
			hexString:   "ffffffffffffffffffffffffffffffff",
			expected:    0,
			shouldError: true,
		},
		{
			name:        "value with upper bits set",
			hexString:   "10000000000000000",
			expected:    0,
			shouldError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			uint128, err := HexStringToUint128(test.hexString)
			if err != nil {
				t.Fatalf("Expected %s to be a valid hex string, got: %s", test.hexString, err)
			}

			result, err := uint128.Uint64()
			if test.shouldError {
				if err == nil {
					t.Fatalf("Expected error for %s, but got none", test.hexString)
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error for %s, got: %s", test.hexString, err)
				}
				if result != test.expected {
					t.Fatalf("Expected %s to convert to %d, got %d", test.hexString, test.expected, result)
				}
			}
		})
	}

	t.Run("ToUint128 round-trip", func(t *testing.T) {
		values := []uint64{
			0,
			1,
			42,
			1000,
			0x123456,
			0xffffffffffffffff,
		}

		for _, expected := range values {
			uint128 := ToUint128(expected)
			result, err := uint128.Uint64()
			if err != nil {
				t.Fatalf("Expected no error for %d, got: %s", expected, err)
			}
			if result != expected {
				t.Fatalf("Expected %d, got %d", expected, result)
			}
		}
	})

	t.Run("ID generator", func(t *testing.T) {
		// Converting generated ids with ID function should fail to be converted to uint64.
		for i := 0; i < 5; i++ {
			id := ID()
			_, err := id.Uint64()
			if err == nil {
				t.Fatal("Expected ID to have upper bits set and fail conversion")
			}
		}

	})
}

func Test_Compare(t *testing.T) {
	tests := []struct {
		name     string
		a        string
		b        string
		expected int
	}{
		{
			name:     "equal values",
			a:        "123456",
			b:        "123456",
			expected: 0,
		},
		{
			name:     "a less than b",
			a:        "100",
			b:        "200",
			expected: -1,
		},
		{
			name:     "a greater than b",
			a:        "200",
			b:        "100",
			expected: 1,
		},
		{
			name:     "zero comparison",
			a:        "0",
			b:        "0",
			expected: 0,
		},
		{
			name:     "zero vs non-zero",
			a:        "0",
			b:        "1",
			expected: -1,
		},
		{
			name:     "large values",
			a:        "ffffffffffffffffffffffffffffffff",
			b:        "fffffffffffffffffffffffffffffffe",
			expected: 1,
		},
		{
			name:     "max value vs zero",
			a:        "ffffffffffffffffffffffffffffffff",
			b:        "0",
			expected: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a, err := HexStringToUint128(test.a)
			if err != nil {
				t.Fatalf("Expected %s to be a valid hex string, got: %s", test.a, err)
			}

			b, err := HexStringToUint128(test.b)
			if err != nil {
				t.Fatalf("Expected %s to be a valid hex string, got: %s", test.b, err)
			}

			result := a.Compare(b)
			if result != test.expected {
				t.Fatalf("Expected Compare(%s, %s) to be %d, got %d", test.a, test.b, test.expected, result)
			}
		})
	}
}

func Test_IsZero(t *testing.T) {
	tests := []struct {
		name      string
		hexString string
		expected  bool
	}{
		{
			name:      "zero value",
			hexString: "0",
			expected:  true,
		},
		{
			name:      "small non-zero value",
			hexString: "1",
			expected:  false,
		},
		{
			name:      "another small value",
			hexString: "123456",
			expected:  false,
		},
		{
			name:      "max uint64",
			hexString: "ffffffffffffffff",
			expected:  false,
		},
		{
			name:      "max uint128",
			hexString: "ffffffffffffffffffffffffffffffff",
			expected:  false,
		},
		{
			name:      "only upper bits set",
			hexString: "10000000000000000",
			expected:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			uint128, err := HexStringToUint128(test.hexString)
			if err != nil {
				t.Fatalf("Expected %s to be a valid hex string, got: %s", test.hexString, err)
			}

			result := uint128.IsZero()
			if result != test.expected {
				t.Fatalf("Expected IsZero(%s) to be %v, got %v", test.hexString, test.expected, result)
			}
		})
	}

	t.Run("ToUint128 zero", func(t *testing.T) {
		uint128 := ToUint128(0)
		if !uint128.IsZero() {
			t.Fatal("Expected ToUint128(0) to be zero")
		}
	})

	t.Run("ToUint128 non-zero", func(t *testing.T) {
		values := []uint64{1, 42, 1000, 0xffffffffffffffff}
		for _, val := range values {
			uint128 := ToUint128(val)
			if uint128.IsZero() {
				t.Fatalf("Expected ToUint128(%d) to be non-zero", val)
			}
		}
	})

	t.Run("ID generator non-zero", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			id := ID()
			if id.IsZero() {
				t.Fatal("Expected ID to be non-zero")
			}
		}
	})
}
