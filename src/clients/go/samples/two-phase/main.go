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

	// Start a pending transfer
	transferRes, err := client.CreateTransfers([]Transfer{
		{
			ID:              ToUint128(1),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(500),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{Pending: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfer: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate accounts pending and posted debits/credits before finishing the two-phase transfer
	accounts, err := client.LookupAccounts([]Uint128{ToUint128(1), ToUint128(2)})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts), 2, "accounts")

	for _, account := range accounts {
		if account.ID == ToUint128(1) {
			assert(account.DebitsPosted, ToUint128(0), "account 1 debits, before posted")
			assert(account.CreditsPosted, ToUint128(0), "account 1 credits, before posted")
			assert(account.DebitsPending, ToUint128(500), "account 1 debits pending, before posted")
			assert(account.CreditsPending, ToUint128(0), "account 1 credits pending, before posted")
		} else if account.ID == ToUint128(2) {
			assert(account.DebitsPosted, ToUint128(0), "account 2 debits, before posted")
			assert(account.CreditsPosted, ToUint128(0), "account 2 credits, before posted")
			assert(account.DebitsPending, ToUint128(0), "account 2 debits pending, before posted")
			assert(account.CreditsPending, ToUint128(500), "account 2 credits pending, before posted")
		} else {
			log.Fatalf("Unexpected account: %s", account.ID)
		}
	}

	// Create a second transfer simply posting the first transfer
	transferRes, err = client.CreateTransfers([]Transfer{
		{
			ID:              ToUint128(2),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(500),
			PendingID:       ToUint128(1),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{PostPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate the contents of all transfers
	transfers, err := client.LookupTransfers([]Uint128{ToUint128(1), ToUint128(2)})
	if err != nil {
		log.Fatalf("Error looking up transfers: %s", err)
	}
	assert(len(transfers), 2, "transfers")

	for _, transfer := range transfers {
		if transfer.ID == ToUint128(1) {
			assert(transfer.TransferFlags().Pending, true, "transfer 1 pending")
			assert(transfer.TransferFlags().PostPendingTransfer, false, "transfer 1 post_pending_transfer")
		} else if transfer.ID == ToUint128(2) {
			assert(transfer.TransferFlags().Pending, false, "transfer 2 pending")
			assert(transfer.TransferFlags().PostPendingTransfer, true, "transfer 2 post_pending_transfer")
		} else {
			log.Fatalf("Unknown transfer: %s", transfer.ID)
		}
	}

	// Validate accounts pending and posted debits/credits after finishing the two-phase transfer
	accounts, err = client.LookupAccounts([]Uint128{ToUint128(1), ToUint128(2)})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts), 2, "accounts")

	for _, account := range accounts {
		if account.ID == ToUint128(1) {
			assert(account.DebitsPosted, ToUint128(500), "account 1 debits")
			assert(account.CreditsPosted, ToUint128(0), "account 1 credits")
			assert(account.DebitsPending, ToUint128(0), "account 1 debits pending")
			assert(account.CreditsPending, ToUint128(0), "account 1 credits pending")
		} else if account.ID == ToUint128(2) {
			assert(account.DebitsPosted, ToUint128(0), "account 2 debits")
			assert(account.CreditsPosted, ToUint128(500), "account 2 credits")
			assert(account.DebitsPending, ToUint128(0), "account 2 debits pending")
			assert(account.CreditsPending, ToUint128(0), "account 2 credits pending")
		} else {
			log.Fatalf("Unexpected account: %s", account.ID)
		}
	}

	fmt.Println("ok")
}
