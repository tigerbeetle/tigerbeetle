package main

import (
	"log"
	"os"
	"reflect"

	. "github.com/tigerbeetle/tigerbeetle-go"
)

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

	client, err := NewClient(ToUint128(0), port)
	if err != nil {
		log.Fatalf("Error creating client: %s", err)
	}
	defer client.Close()

	// Create accounts.
	accountDRID := ID()
	accountCRID := ID()

	accountResults, err := client.CreateAccounts([]Account{
		{
			ID:     accountDRID,
			Ledger: 1,
			Code:   1,
		},
		{
			ID:     accountCRID,
			Ledger: 1,
			Code:   1,
		},
	})
	if err != nil {
		log.Fatalf("Error creating accounts: %s", err)
	}

	assert(len(accountResults), 2, "accountResults")
	for i, result := range accountResults {
		switch result.Status {
		case AccountCreated:
		default:
			log.Fatalf("Error creating account %d: %s", i, result.Status)
		}
	}

	// Create a transfer.
	transferResults, err := client.CreateTransfers([]Transfer{
		{
			ID:              ID(),
			DebitAccountID:  accountDRID,
			CreditAccountID: accountCRID,
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfer: %s", err)
	}

	assert(len(transferResults), 1, "transferResults")
	for i, result := range transferResults {
		switch result.Status {
		case TransferCreated:
		default:
			log.Fatalf("Error creating transfer %d: %s", i, result.Status)
		}
	}

	// Fetch and validate account balances.
	accounts, err := client.LookupAccounts([]Uint128{accountDRID, accountDRID})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts), 2, "accounts")

	for _, account := range accounts {
		switch account.ID {
		case accountDRID:
			assert(account.DebitsPosted, ToUint128(10), "account 1 debits")
			assert(account.CreditsPosted, ToUint128(0), "account 1 credits")
		case accountCRID:
			assert(account.DebitsPosted, ToUint128(0), "account 2 debits")
			assert(account.CreditsPosted, ToUint128(10), "account 2 credits")
		default:
			log.Fatalf("Unexpected account")
		}
	}
}
