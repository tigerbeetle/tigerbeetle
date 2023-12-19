package main

import (
	"fmt"
	"log"
	"os"
	"reflect"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

// Since we only require Go 1.17 we can't do this as a generic function
// even though that would be fine. So do the dynamic approach for now.
func assert(a, b interface{}, field string) {
	if !reflect.DeepEqual(a, b) {
		log.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, b, b, a, a)
	}
}

func main() {
	port := os.Getenv("TB_ADDRESS")
	if port == "" {
		port = "3000"
	}

	client, err := NewClient(ToUint128(0), []string{port}, 256)
	if err != nil {
		log.Fatalf("Error creating client: %s", err)
	}
	defer client.Close()

	// Create two accounts
	res, err := client.CreateAccounts([]Account{
		{
			ID:     ToUint128(1),
			Ledger: 1,
			Code:   1,
		},
		{
			ID:     ToUint128(2),
			Ledger: 1,
			Code:   1,
		},
	})
	if err != nil {
		log.Fatalf("Error creating accounts: %s", err)
	}

	for _, err := range res {
		log.Fatalf("Error creating account %d: %s", err.Index, err.Result)
	}

	transferRes, err := client.CreateTransfers([]Transfer{
		{
			ID:              ToUint128(1),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfer: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Check the sums for both accounts
	accounts, err := client.LookupAccounts([]Uint128{ToUint128(1), ToUint128(2)})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts), 2, "accounts")

	for _, account := range accounts {
		if account.ID == ToUint128(1) {
			assert(account.DebitsPosted, ToUint128(10), "account 1 debits")
			assert(account.CreditsPosted, ToUint128(0), "account 1 credits")
		} else if account.ID == ToUint128(2) {
			assert(account.DebitsPosted, ToUint128(0), "account 2 debits")
			assert(account.CreditsPosted, ToUint128(10), "account 2 credits")
		} else {
			log.Fatalf("Unexpected account")
		}
	}

	fmt.Println("ok")
}
