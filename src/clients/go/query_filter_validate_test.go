package tigerbeetle_go

import (
	"errors"
	"math"
	"testing"
)

func TestValidateQueryFilter(t *testing.T) {
	t.Parallel()

	ok := QueryFilter{Limit: 10}
	if err := validateQueryFilter(ok); err != nil {
		t.Fatalf("valid filter: %v", err)
	}

	cases := []struct {
		name   string
		filter QueryFilter
	}{
		{"zero limit", QueryFilter{Ledger: 1}},
		{"timestamp min max", QueryFilter{Limit: 10, TimestampMin: math.MaxUint64}},
		{"timestamp max max", QueryFilter{Limit: 10, TimestampMax: math.MaxUint64}},
		{"min greater max", QueryFilter{Limit: 10, TimestampMin: 2, TimestampMax: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateQueryFilter(tc.filter)
			if !errors.Is(err, ErrInvalidQueryFilter) {
				t.Fatalf("got %v want ErrInvalidQueryFilter", err)
			}
		})
	}
}
