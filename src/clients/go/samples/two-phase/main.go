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
			if account.ID == account_found.ID {
				requested = true
				assert(account.DebitsPosted, account_found.DebitsPosted,
					fmt.Sprintf("account %s debits, %s", account.ID, debugMsg))
				assert(account.CreditsPosted, account_found.DebitsPosted,
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

	// Start a pending transfer
	transferRes, err := client.CreateTransfers([]tb_types.Transfer{
		{
			ID:              uint128("1"),
			DebitAccountID:  uint128("1"),
			CreditAccountID: uint128("2"),
			Ledger:          1,
			Code:            1,
			Amount:          500,
			Flags:           tb_types.TransferFlags{Pending: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfer: %s", err)
	}

	for _, err := range transferRes {
		log.Fatalf("Error creating transfer: %s", err.Result)
	}

	// Validate accounts pending and posted debits/credits before finishing the two-phase transfer
	assertAccountBalances(client, []tb_types.Account{
		{
			ID:             uint128("1"),
			DebitsPosted:   uint64(0),
			CreditsPosted:  uint64(0),
			DebitsPending:  uint64(500),
			CreditsPending: uint64(0),
		},
		{
			ID:             uint128("2"),
			DebitsPosted:   uint64(0),
			CreditsPosted:  uint64(0),
			DebitsPending:  uint64(0),
			CreditsPending: uint64(500),
		},
	}, "before posted")

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

	// Validate the contents of all transfers
	transfers, err := client.LookupTransfers([]tb_types.Uint128{uint128("1"), uint128("2")})
	if err != nil {
		log.Fatalf("Error looking up transfers: %s", err)
	}
	assert(len(transfers), 2, "transfers")

	for _, transfer := range transfers {
		if transfer.ID == uint128("1") {
			assert(transfer.TransferFlags().Pending, true, "transfer 1 pending")
			assert(transfer.TransferFlags().PostPendingTransfer, false, "transfer 1 post_pending_transfer")
		} else if transfer.ID == uint128("2") {
			assert(transfer.TransferFlags().Pending, false, "transfer 2 pending")
			assert(transfer.TransferFlags().PostPendingTransfer, true, "transfer 2 post_pending_transfer")
		} else {
			log.Fatalf("Unknown transfer: %s", transfer.ID)
		}
	}

	// Validate accounts pending and posted debits/credits after finishing the two-phase transfer
	assertAccountBalances(client, []tb_types.Account{
		{
			ID:             uint128("1"),
			DebitsPosted:   uint64(500),
			CreditsPosted:  uint64(0),
			DebitsPending:  uint64(0),
			CreditsPending: uint64(0),
		},
		{
			ID:             uint128("2"),
			DebitsPosted:   uint64(0),
			CreditsPosted:  uint64(500),
			DebitsPending:  uint64(0),
			CreditsPending: uint64(0),
		},
	}, "before posted")

	fmt.Println("ok")
}
