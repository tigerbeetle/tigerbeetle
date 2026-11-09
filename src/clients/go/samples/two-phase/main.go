package main

import (
	"fmt"
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

	// Create pending transfer.
	transferPendingID := ID()
	transferResults, err := client.CreateTransfers([]Transfer{
		{
			ID:              transferPendingID,
			DebitAccountID:  accountDRID,
			CreditAccountID: accountCRID,
			Ledger:          1,
			Code:            1,
			Amount:          ToUint128(500),
			Flags:           TransferFlags{Pending: true}.ToUint16(),
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

	// Fetch and validate pending account balances.
	accounts, err := client.LookupAccounts([]Uint128{accountDRID, accountCRID})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts), 2, "accounts")

	for _, account := range accounts {
		switch account.ID {
		case accountDRID:
			assert(account.DebitsPosted, ToUint128(0), "account 1 debits, before posted")
			assert(account.CreditsPosted, ToUint128(0), "account 1 credits, before posted")
			assert(account.DebitsPending, ToUint128(500), "account 1 debits pending, before posted")
			assert(account.CreditsPending, ToUint128(0), "account 1 credits pending, before posted")
		case accountCRID:
			assert(account.DebitsPosted, ToUint128(0), "account 2 debits, before posted")
			assert(account.CreditsPosted, ToUint128(0), "account 2 credits, before posted")
			assert(account.DebitsPending, ToUint128(0), "account 2 debits pending, before posted")
			assert(account.CreditsPending, ToUint128(500), "account 2 credits pending, before posted")
		default:
			log.Fatalf("Unexpected account: %s", account.ID)
		}
	}

	// Post pending transfer;
	transferResults, err = client.CreateTransfers([]Transfer{
		{
			ID:              ID(),
			DebitAccountID:  accountDRID,
			CreditAccountID: accountCRID,
			PendingID:       transferPendingID,
			Ledger:          1,
			Code:            1,
			// You can post any amount between zero and the pending transfer amount,
			// or void the two-phase transfer using the `VoidPendingTransfer` flag.
			Amount: ToUint128(500),
			Flags:  TransferFlags{PostPendingTransfer: true}.ToUint16(),
		},
	})
	if err != nil {
		log.Fatalf("Error creating transfers: %s", err)
	}

	assert(len(transferResults), 1, "transferResults")
	for i, result := range transferResults {
		switch result.Status {
		case TransferCreated:
		default:
			log.Fatalf("Error creating transfer %d: %s", i, result.Status)
		}
	}

	// Fetch and validate final account balances.
	accounts, err = client.LookupAccounts([]Uint128{accountDRID, accountCRID})
	if err != nil {
		log.Fatalf("Could not fetch accounts: %s", err)
	}
	assert(len(accounts), 2, "accounts")

	for _, account := range accounts {
		switch account.ID {
		case accountDRID:
			assert(account.DebitsPosted, ToUint128(500), "account 1 debits")
			assert(account.CreditsPosted, ToUint128(0), "account 1 credits")
			assert(account.DebitsPending, ToUint128(0), "account 1 debits pending")
			assert(account.CreditsPending, ToUint128(0), "account 1 credits pending")
		case accountCRID:
			assert(account.DebitsPosted, ToUint128(0), "account 2 debits")
			assert(account.CreditsPosted, ToUint128(500), "account 2 credits")
			assert(account.DebitsPending, ToUint128(0), "account 2 debits pending")
			assert(account.CreditsPending, ToUint128(0), "account 2 credits pending")
		default:
			log.Fatalf("Unexpected account: %s", account.ID)
		}
	}

	fmt.Println("ok")
}
