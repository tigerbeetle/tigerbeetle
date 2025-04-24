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
		uint128_back := BigIntToUint128(bigint)
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
			if b.Cmp(&a) != 1 {
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
