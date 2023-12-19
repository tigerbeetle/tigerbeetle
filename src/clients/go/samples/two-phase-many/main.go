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
func assert(expected, got interface{}, field string) {
	if !reflect.DeepEqual(expected, got) {
		log.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, expected, expected, got, got)
	}
}

func assertAccountBalances(client Client, accounts []Account, debugMsg string) {
	ids := []Uint128{}
	for _, account := range accounts {
		ids = append(ids, account.ID)
	}
	found, err := client.LookupAccounts(ids)
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts), len(found), "accounts")

	for _, account_found := range found {
		requested := false
		for _, account := range accounts {
			if account.ID == account_found.ID {
				requested = true
				assert(account.DebitsPosted, account_found.DebitsPosted,
					fmt.Sprintf("account %s debits, %s", account.ID, debugMsg))
				assert(account.CreditsPosted, account_found.CreditsPosted,
					fmt.Sprintf("account %s credits, %s", account.ID, debugMsg))
				assert(account.DebitsPending, account_found.DebitsPending,
					fmt.Sprintf("account %s debits pending, %s", account.ID, debugMsg))
				assert(account.CreditsPending, account_found.CreditsPending,
					fmt.Sprintf("account %s credits pending, %s", account.ID, debugMsg))
			}
		}

		if !requested {
			log.Fatalf("Unexpected account: %s", account_found.ID)
		}
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

	// Create two accounts.
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

	// Start five pending transfers.
	transfers := []Transfer{
		{
			ID:              ToUint128(1),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(100),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{Pending: true}.ToUint16(),
		},
		{
			ID:              ToUint128(2),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(200),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{Pending: true}.ToUint16(),
		},
		{
			ID:              ToUint128(3),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(300),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{Pending: true}.ToUint16(),
		},
		{
			ID:              ToUint128(4),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(400),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{Pending: true}.ToUint16(),
		},
		{
			ID:              ToUint128(5),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(500),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{Pending: true}.ToUint16(),
		},
	}
	transferRes, err := client.CreateTransfers(transfers)
	if err != nil {
		log.Fatalf("Error creating transfer: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate accounts pending and posted debits/credits before finishing the two-phase transfer.
	assertAccountBalances(client, []Account{
		{
			ID:             ToUint128(1),
			DebitsPosted:   ToUint128(0),
			CreditsPosted:  ToUint128(0),
			DebitsPending:  ToUint128(1500),
			CreditsPending: ToUint128(0),
		},
		{
			ID:             ToUint128(2),
			DebitsPosted:   ToUint128(0),
			CreditsPosted:  ToUint128(0),
			DebitsPending:  ToUint128(0),
			CreditsPending: ToUint128(1500),
		},
	}, "after starting 5 pending transfers")

	// Create a 6th transfer posting the 1st transfer.
	transferRes, err = client.CreateTransfers([]Transfer{
		{
			ID:              ToUint128(6),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(100),
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

	// Validate account balances after posting 1st pending transfer.
	assertAccountBalances(client, []Account{
		{
			ID:             ToUint128(1),
			DebitsPosted:   ToUint128(100),
			CreditsPosted:  ToUint128(0),
			DebitsPending:  ToUint128(1400),
			CreditsPending: ToUint128(0),
		},
		{
			ID:             ToUint128(2),
			DebitsPosted:   ToUint128(0),
			CreditsPosted:  ToUint128(100),
			DebitsPending:  ToUint128(0),
			CreditsPending: ToUint128(1400),
		},
	}, "after completing 1 pending transfer")

	// Create a 7th transfer voiding the 2nd transfer.
	transferRes, err = client.CreateTransfers([]Transfer{
		{
			ID:              ToUint128(7),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(200),
			PendingID:       ToUint128(2),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{VoidPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate account balances after voiding 2nd pending transfer.
	assertAccountBalances(client, []Account{
		{
			ID:             ToUint128(1),
			DebitsPosted:   ToUint128(100),
			CreditsPosted:  ToUint128(0),
			DebitsPending:  ToUint128(1200),
			CreditsPending: ToUint128(0),
		},
		{
			ID:             ToUint128(2),
			DebitsPosted:   ToUint128(0),
			CreditsPosted:  ToUint128(100),
			DebitsPending:  ToUint128(0),
			CreditsPending: ToUint128(1200),
		},
	}, "after completing 2 pending transfers")

	// Create a 8th transfer posting the 3rd transfer.
	transferRes, err = client.CreateTransfers([]Transfer{
		{
			ID:              ToUint128(8),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(300),
			PendingID:       ToUint128(3),
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

	// Validate account balances after posting 3rd pending transfer.
	assertAccountBalances(client, []Account{
		{
			ID:             ToUint128(1),
			DebitsPosted:   ToUint128(400),
			CreditsPosted:  ToUint128(0),
			DebitsPending:  ToUint128(900),
			CreditsPending: ToUint128(0),
		},
		{
			ID:             ToUint128(2),
			DebitsPosted:   ToUint128(0),
			CreditsPosted:  ToUint128(400),
			DebitsPending:  ToUint128(0),
			CreditsPending: ToUint128(900),
		},
	}, "after completing 3 pending transfers")

	// Create a 9th transfer voiding the 4th transfer.
	transferRes, err = client.CreateTransfers([]Transfer{
		{
			ID:              ToUint128(9),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(400),
			PendingID:       ToUint128(4),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{VoidPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate account balances after voiding 4th pending transfer.
	assertAccountBalances(client, []Account{
		{
			ID:             ToUint128(1),
			DebitsPosted:   ToUint128(400),
			CreditsPosted:  ToUint128(0),
			DebitsPending:  ToUint128(500),
			CreditsPending: ToUint128(0),
		},
		{
			ID:             ToUint128(2),
			DebitsPosted:   ToUint128(0),
			CreditsPosted:  ToUint128(400),
			DebitsPending:  ToUint128(0),
			CreditsPending: ToUint128(500),
		},
	}, "after completing 4 pending transfers")

	// Create a 10th transfer voiding the 5th transfer.
	transferRes, err = client.CreateTransfers([]Transfer{
		{
			ID:              ToUint128(10),
			DebitAccountID:  ToUint128(1),
			CreditAccountID: ToUint128(2),
			Amount:          ToUint128(500),
			PendingID:       ToUint128(5),
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

	// Validate account balances after voiding 4th pending transfer.
	assertAccountBalances(client, []Account{
		{
			ID:             ToUint128(1),
			DebitsPosted:   ToUint128(900),
			CreditsPosted:  ToUint128(0),
			DebitsPending:  ToUint128(0),
			CreditsPending: ToUint128(0),
		},
		{
			ID:             ToUint128(2),
			DebitsPosted:   ToUint128(0),
			CreditsPosted:  ToUint128(900),
			DebitsPending:  ToUint128(0),
			CreditsPending: ToUint128(0),
		},
	}, "after completing 5 pending transfers")

	fmt.Println("ok")
}
