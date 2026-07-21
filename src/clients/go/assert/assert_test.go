package assert

import "testing"

func TestEmpty(t *testing.T) {
	Empty(t, nil)
	Empty(t, []int{})
	Empty(t, [0]int{})
	Empty(t, "")
	Empty(t, map[string]int{})

	if isEmpty([]int{1}) {
		t.Fatalf("expected non-empty slice to be non-empty")
	}
	if isEmpty([1]int{1}) {
		t.Fatalf("expected non-empty array to be non-empty")
	}
	if isEmpty("x") {
		t.Fatalf("expected non-empty string to be non-empty")
	}
}

func TestGreater(t *testing.T) {
	if !isGreater(uint64(3), uint64(2)) {
		t.Fatalf("uint64 3 > 2")
	}
	if !isGreater(3, 2) {
		t.Fatalf("int 3 > 2")
	}
	if !isGreater(int64(5), int64(1)) {
		t.Fatalf("int64 5 > 1")
	}
	if isGreater(2, 3) {
		t.Fatalf("2 should not be greater than 3")
	}
	if isGreater(3, 3) {
		t.Fatalf("equal values should not be greater")
	}
	if isGreater("a", "b") {
		t.Fatalf("non-numeric Greater should be false")
	}
}
