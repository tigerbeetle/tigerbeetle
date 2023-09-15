package types

import (
	"math/big"
	"testing"
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
			t.Errorf("Expected %s to be a valid hex string, got: %s", test, err)
		}
		thereAndBack := res.String()
		if thereAndBack != test {
			t.Errorf("Expected %s to be %s, got %s", test, test, thereAndBack)
		}
	}
}

func Test_HexStringToUint128_LittleEndian(t *testing.T) {

	test := "123456"

	res, err := HexStringToUint128(test)
	if err != nil {
		t.Errorf("Expected %s to be a valid hex string, got: %s", test, err)
	}

	expected := [16]byte{86, 52, 18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	if res.Bytes() != expected {
		t.Errorf("Expected %s to produce bytes %v, got bytes %v", test, expected, res.Bytes())
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
			t.Errorf("Expected %s to be a valid hex string, got: %s", test, err)
		}

		bigint := uint128.BigInt()
		uint128_back = BigIntToUint128(bigint)
		string_back = uint128_back.String()

		if string_back != test {
			t.Errorf("Expected %s to be %s, got %s", test, test, string_back)
		}
	}
}
