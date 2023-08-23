package main

import (
	"fmt"
	"log"
	"os"
	"reflect"

	tb "github.com/tigerbeetle/tigerbeetle-go"
	tb_types "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func uint128(value string) tb_types.Uint128 {
	x, err := tb_types.HexStringToUint128(value)
	if err != nil {
		panic(err)
	}
	return x
}

// Since we only require Go 1.17 we can't do this as a generic function
// even though that would be fine. So do the dynamic approach for now.
func assert(expected, got interface{}, field string) {
	if !reflect.DeepEqual(expected, got) {
		log.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, expected, expected, got, got)
	}
}

func assertAccountBalances(client tb.Client, accounts []tb_types.Account, debugMsg string) {
	ids := []tb_types.Uint128{}
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

	client, err := tb.NewClient(0, []string{port}, 32)
	if err != nil {
		log.Fatalf("Error creating client: %s", err)
	}
	defer client.Close()

	// Create two accounts.
	res, err := client.CreateAccounts([]tb_types.Account{
		{
			ID:     uint128("1"),
			Ledger: 1,
			Code:   1,
		},
		{
			ID:     uint128("2"),
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
	transfers := []tb_types.Transfer{
		{
			ID:              uint128("1"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          100,
			Flags:           tb_types.TransferFlags{Pending: true}.ToUint16(),
		},
		{
			ID:              uint128("2"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          200,
			Flags:           tb_types.TransferFlags{Pending: true}.ToUint16(),
		},
		{
			ID:              uint128("3"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          300,
			Flags:           tb_types.TransferFlags{Pending: true}.ToUint16(),
		},
		{
			ID:              uint128("4"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          400,
			Flags:           tb_types.TransferFlags{Pending: true}.ToUint16(),
		},
		{
			ID:              uint128("5"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          500,
			Flags:           tb_types.TransferFlags{Pending: true}.ToUint16(),
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
	assertAccountBalances(client, []tb_types.Account{
		{
			ID: uint128("1"),
			DebitsPosted: uint64(0),
			CreditsPosted: uint64(0),
			DebitsPending: uint64(1500),
			CreditsPending: uint64(0),
		},
		{
			ID: uint128("2"),
			DebitsPosted: uint64(0),
			CreditsPosted: uint64(0),
			DebitsPending: uint64(0),
			CreditsPending: uint64(1500),
		},
	}, "after starting 5 pending transfers")

	// Create a 6th transfer posting the 1st transfer.
	transferRes, err = client.CreateTransfers([]tb_types.Transfer{
		{
			ID:              uint128("6"),
			PendingID:       uint128("1"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          100,
			Flags:           tb_types.TransferFlags{PostPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate account balances after posting 1st pending transfer.
	assertAccountBalances(client, []tb_types.Account{
		{
			ID: uint128("1"),
			DebitsPosted: uint64(100),
			CreditsPosted: uint64(0),
			DebitsPending: uint64(1400),
			CreditsPending: uint64(0),
		},
		{
			ID: uint128("2"),
			DebitsPosted: uint64(0),
			CreditsPosted: uint64(100),
			DebitsPending: uint64(0),
			CreditsPending: uint64(1400),
		},
	}, "after completing 1 pending transfer")

	// Create a 7th transfer voiding the 2nd transfer.
	transferRes, err = client.CreateTransfers([]tb_types.Transfer{
		{
			ID:              uint128("7"),
			PendingID:       uint128("2"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          200,
			Flags:           tb_types.TransferFlags{VoidPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate account balances after voiding 2nd pending transfer.
	assertAccountBalances(client, []tb_types.Account{
		{
			ID: uint128("1"),
			DebitsPosted: uint64(100),
			CreditsPosted: uint64(0),
			DebitsPending: uint64(1200),
			CreditsPending: uint64(0),
		},
		{
			ID: uint128("2"),
			DebitsPosted: uint64(0),
			CreditsPosted: uint64(100),
			DebitsPending: uint64(0),
			CreditsPending: uint64(1200),
		},
	}, "after completing 2 pending transfers")

	// Create a 8th transfer posting the 3rd transfer.
	transferRes, err = client.CreateTransfers([]tb_types.Transfer{
		{
			ID:              uint128("8"),
			PendingID:       uint128("3"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          300,
			Flags:           tb_types.TransferFlags{PostPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate account balances after voiding 3rd pending transfer.
	assertAccountBalances(client, []tb_types.Account{
		{
			ID: uint128("1"),
			DebitsPosted: uint64(400),
			CreditsPosted: uint64(0),
			DebitsPending: uint64(900),
			CreditsPending: uint64(0),
		},
		{
			ID: uint128("2"),
			DebitsPosted: uint64(0),
			CreditsPosted: uint64(400),
			DebitsPending: uint64(0),
			CreditsPending: uint64(900),
		},
	}, "after completing 3 pending transfers")

	// Create a 9th transfer voiding the 4th transfer.
	transferRes, err = client.CreateTransfers([]tb_types.Transfer{
		{
			ID:              uint128("9"),
			PendingID:       uint128("4"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          400,
			Flags:           tb_types.TransferFlags{VoidPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate account balances after voiding 4th pending transfer.
	assertAccountBalances(client, []tb_types.Account{
		{
			ID: uint128("1"),
			DebitsPosted: uint64(400),
			CreditsPosted: uint64(0),
			DebitsPending: uint64(500),
			CreditsPending: uint64(0),
		},
		{
			ID: uint128("2"),
			DebitsPosted: uint64(0),
			CreditsPosted: uint64(400),
			DebitsPending: uint64(0),
			CreditsPending: uint64(500),
		},
	}, "after completing 4 pending transfers")

	// Create a 10th transfer voiding the 5th transfer.
	transferRes, err = client.CreateTransfers([]tb_types.Transfer{
		{
			ID:              uint128("10"),
			PendingID:       uint128("5"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          500,
			Flags:           tb_types.TransferFlags{PostPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate account balances after voiding 4th pending transfer.
	assertAccountBalances(client, []tb_types.Account{
		{
			ID: uint128("1"),
			DebitsPosted: uint64(900),
			CreditsPosted: uint64(0),
			DebitsPending: uint64(0),
			CreditsPending: uint64(0),
		},
		{
			ID: uint128("2"),
			DebitsPosted: uint64(0),
			CreditsPosted: uint64(900),
			DebitsPending: uint64(0),
			CreditsPending: uint64(0),
		},
	}, "after completing 5 pending transfers")

	fmt.Println("ok")
}
