package main

import (
	"fmt"
	"log"

	tb "github.com/tigerbeetledb/tigerbeetle-go"
	tb_types "github.com/tigerbeetledb/tigerbeetle-go/pkg/types"
)

func uint128(value string) tb_types.Uint128 {
	x, err := tb_types.HexStringToUint128(value)
	if err != nil {
		panic(err)
	}
	return x
}

func main() {
	client, err := tb.NewClient(0, []string{"3000"}, 1)
	if err != nil {
		log.Printf("Error creating client: %s", err)
		return
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
		log.Printf("Error creating accounts: %s", err)
		return
	}

	for _, err := range res {
		log.Printf("Error creating account %d: %s", err.Index, err.Code)
		return
	}

	// Send money from one account to another
	SAMPLES := 1_000_000
	BATCH_SIZE := 8191
	batch := make([]tb_types.Transfer, BATCH_SIZE)
	for i := 0; i < SAMPLES; i += BATCH_SIZE {
		for j := 0; (j < BATCH_SIZE) && (i+j < SAMPLES); j++ {
			batch[j] = tb_types.Transfer{
				ID:              uint128(fmt.Sprintf("%d", i+j+1)),
				DebitAccountID:  uint128("1"),
				CreditAccountID: uint128("2"),
				Ledger:          1,
				Code:            1,
				Amount:          10,
			}
		}

		res, err := client.CreateTransfers(batch)
		if err != nil {
			log.Printf("Error creating transfer batch %d: %s", i, err)
			return
		}

		for _, err := range res {
			id := int(err.Index) + i
			if id < SAMPLES {
				log.Printf("Error creating transfer %d: %s", id, err.Code)
			}
			return
		}
	}

	// Check the sums for both accounts
	accounts, err := client.LookupAccounts([]tb_types.Uint128{uint128("1"), uint128("2")})
	if err != nil {
		log.Printf("Could not fetch accounts: %s", err)
		return
	}

	total := uint64(10 * SAMPLES)
	for _, account := range accounts {
		if account.ID == uint128("1") {
			if account.DebitsPosted != total {
				panic(fmt.Sprintf("Expected debits to be %d, got %d", total, account.DebitsPosted))
			}
		} else {
			if account.CreditsPosted != total {
				panic(fmt.Sprintf("Expected credits to be %d, got %d", total, account.CreditsPosted))
			}
		}
	}
}
