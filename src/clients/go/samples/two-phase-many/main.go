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
func assert(a, b interface{}, field string) {
	if !reflect.DeepEqual(a, b) {
		log.Fatalf("Expected %s to be [%+v (%T)], got: [%+v (%T)]", field, b, b, a, a)
	}
}

func assertAccountBalances(client *tb.Client, accounts []tb_types.Account, debugMsg string) {
	ids := []tb_types.Uint128{}
	for _, account := range accounts {
		ids = append(ids, account.ID)
	}
	found, err := client.LookupAccounts(ids)
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(found), len(accounts), "accounts")

	for _, account_found := range found {
		requested := false
		for _, account := range accounts {
			if account.ID  == account_found.ID {
				requested = true
				assert(account.DebitsPosted, account_found.DebitsPosted,
					fmt.Sprintf("account %s debits, %s", account.ID, debugMsg))
				assert(account.CreditsPosted,account_found.DebitsPosted,
					fmt.Sprintf("account %s credits, %s", account.ID, debugMsg))
				assert(account.DebitsPending, account_found.DebitsPosted,
					fmt.Sprintf("account %s debits pending, %s", account.ID, debugMsg))
				assert(account.CreditsPending, account_found.DebitsPosted,
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

	// Create two accounts
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

	// Start five pending transfer
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

	// Validate accounts pending and posted debits/credits before finishing the two-phase transfer
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
		}
	}, "after starting 5 pending transfers")

	// Create a second transfer simply posting the first transfer
	transferRes, err = client.CreateTransfers([]tb_types.Transfer{
		{
			ID:              uint128("2"),
			PendingID:       uint128("1"),
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

	// Validate accounts pending and posted debits/credits after finishing the two-phase transfer
	accounts, err = client.LookupAccounts([]tb_types.Uint128{uint128("1"), uint128("2")})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts), 2, "accounts")

	for _, account := range accounts {
		if account.ID == uint128("1") {
			assert(account.DebitsPosted, uint64(500), "account 1 debits")
			assert(account.CreditsPosted, uint64(0), "account 1 credits")
			assert(account.DebitsPending, uint64(0), "account 1 debits pending")
			assert(account.CreditsPending, uint64(0), "account 1 credits pending")
		} else if account.ID == uint128("2") {
			assert(account.DebitsPosted, uint64(0), "account 2 debits")
			assert(account.CreditsPosted, uint64(500), "account 2 credits")
			assert(account.DebitsPending, uint64(0), "account 2 debits pending")
			assert(account.CreditsPending, uint64(0), "account 2 credits pending")
		} else {
			log.Fatalf("Unexpected account: %s", account.ID)
		}
	}

	fmt.Println("ok")
}
