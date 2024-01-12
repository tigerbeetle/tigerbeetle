// section:imports
package main

import (
	"fmt"
	"log"
	"os"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func main() {
	fmt.Println("Import ok!")
	// endsection:imports

	// section:client
	tbAddress := os.Getenv("TB_ADDRESS")
	if len(tbAddress) == 0 {
		tbAddress = "3000"
	}
	client, err := NewClient(ToUint128(0), []string{tbAddress}, 256)
	if err != nil {
		log.Printf("Error creating client: %s", err)
		return
	}
	defer client.Close()
	// endsection:client

	// section:create-accounts
	accountsRes, err := client.CreateAccounts([]Account{
		{
			ID:             ToUint128(137),
			DebitsPending:  ToUint128(0),
			DebitsPosted:   ToUint128(0),
			CreditsPending: ToUint128(0),
			CreditsPosted:  ToUint128(0),
			UserData128:    ToUint128(0),
			UserData64:     0,
			UserData32:     0,
			Reserved:       0,
			Ledger:         1,
			Code:           718,
			Flags:          0,
			Timestamp:      0,
		},
	})
	if err != nil {
		log.Printf("Error creating accounts: %s", err)
		return
	}

	for _, err := range accountsRes {
		log.Printf("Error creating account %d: %s", err.Index, err.Result)
		return
	}
	// endsection:create-accounts

	// section:account-flags
	account0 := Account{ /* ... account values ... */ }
	account1 := Account{ /* ... account values ... */ }
	account0.Flags = AccountFlags{Linked: true}.ToUint16()

	accountErrors, err := client.CreateAccounts([]Account{account0, account1})
	if err != nil {
		log.Printf("Error creating accounts: %s", err)
		return
	}
	// endsection:account-flags

	// section:create-accounts-errors
	account2 := Account{ /* ... account values ... */ }
	account3 := Account{ /* ... account values ... */ }
	account4 := Account{ /* ... account values ... */ }

	accountErrors, err = client.CreateAccounts([]Account{account2, account3, account4})
	if err != nil {
		log.Printf("Error creating accounts: %s", err)
		return
	}
	for _, err := range accountErrors {
		log.Printf("Error creating account %d: %s", err.Index, err.Result)
		return
	}
	// endsection:create-accounts-errors

	// section:lookup-accounts
	accounts, err := client.LookupAccounts([]Uint128{ToUint128(137), ToUint128(138)})
	if err != nil {
		log.Printf("Could not fetch accounts: %s", err)
		return
	}
	log.Println(accounts)
	// endsection:lookup-accounts

	// section:create-transfers
	transfers := []Transfer{{
		ID:              ToUint128(1),
		DebitAccountID:  ToUint128(1),
		CreditAccountID: ToUint128(2),
		Amount:          ToUint128(10),
		PendingID:       ToUint128(0),
		UserData128:     ToUint128(2),
		UserData64:      0,
		UserData32:      0,
		Timeout:         0,
		Ledger:          1,
		Code:            1,
		Flags:           0,
		Timestamp:       0,
	}}

	transfersRes, err := client.CreateTransfers(transfers)
	if err != nil {
		log.Printf("Error creating transfer batch: %s", err)
		return
	}
	// endsection:create-transfers

	// section:create-transfers-errors
	for _, err := range transfersRes {
		log.Printf("Batch transfer at %d failed to create: %s", err.Index, err.Result)
		return
	}
	// endsection:create-transfers-errors

	// section:no-batch
	for i := 0; i < len(transfers); i++ {
		transfersRes, err = client.CreateTransfers([]Transfer{transfers[i]})
		// error handling omitted
	}
	// endsection:no-batch

	// section:batch
	BATCH_SIZE := 8190
	for i := 0; i < len(transfers); i += BATCH_SIZE {
		batch := BATCH_SIZE
		if i+BATCH_SIZE > len(transfers) {
			batch = len(transfers) - i
		}
		transfersRes, err = client.CreateTransfers(transfers[i : i+batch])
		// error handling omitted
	}
	// endsection:batch

	// section:transfer-flags-link
	transfer0 := Transfer{ /* ... account values ... */ }
	transfer1 := Transfer{ /* ... account values ... */ }
	transfer0.Flags = TransferFlags{Linked: true}.ToUint16()
	transfersRes, err = client.CreateTransfers([]Transfer{transfer0, transfer1})
	// error handling omitted
	// endsection:transfer-flags-link

	// section:transfer-flags-post
	transfer := Transfer{
		ID:        ToUint128(2),
		PendingID: ToUint128(1),
		Flags:     TransferFlags{PostPendingTransfer: true}.ToUint16(),
		Timestamp: 0,
	}
	transfersRes, err = client.CreateTransfers([]Transfer{transfer})
	// error handling omitted
	// endsection:transfer-flags-post

	// section:transfer-flags-void
	transfer = Transfer{
		ID:        ToUint128(2),
		PendingID: ToUint128(1),
		Flags:     TransferFlags{VoidPendingTransfer: true}.ToUint16(),
		Timestamp: 0,
	}
	transfersRes, err = client.CreateTransfers([]Transfer{transfer})
	// error handling omitted
	// endsection:transfer-flags-void

	// section:lookup-transfers
	transfers, err = client.LookupTransfers([]Uint128{ToUint128(1), ToUint128(2)})
	if err != nil {
		log.Printf("Could not fetch transfers: %s", err)
		return
	}
	log.Println(transfers)
	// endsection:lookup-transfers

	// section:get-account-transfers
	filter := GetAccountTransfers{
		AccountID: ToUint128(2),
		Timestamp: 0,  // No filter by Timestamp.
		Limit:     10, // Limit to ten transfers at most.
		Flags: GetAccountTransfersFlags{
			Debits:   true, // Include transfer from the debit side.
			Credits:  true, // Include transfer from the credit side.
			Reversed: true, // Sort by timestamp in reverse-chronological order.
		}.ToUint32(),
	}
	transfers, err = client.GetAccountTransfers(filter)
	if err != nil {
		log.Printf("Could not fetch transfers: %s", err)
		return
	}
	log.Println(transfers)
	// endsection:get-account-transfers

	// section:linked-events
	batch := []Transfer{}
	linkedFlag := TransferFlags{Linked: true}.ToUint16()

	// An individual transfer (successful):
	batch = append(batch, Transfer{ID: ToUint128(1) /* ... rest of transfer ... */})

	// A chain of 4 transfers (the last transfer in the chain closes the chain with linked=false):
	batch = append(batch, Transfer{ID: ToUint128(2) /* ... , */, Flags: linkedFlag}) // Commit/rollback.
	batch = append(batch, Transfer{ID: ToUint128(3) /* ... , */, Flags: linkedFlag}) // Commit/rollback.
	batch = append(batch, Transfer{ID: ToUint128(2) /* ... , */, Flags: linkedFlag}) // Fail with exists
	batch = append(batch, Transfer{ID: ToUint128(4) /* ... , */})                    // Fail without committing

	// An individual transfer (successful):
	// This should not see any effect from the failed chain above.
	batch = append(batch, Transfer{ID: ToUint128(2) /* ... rest of transfer ... */})

	// A chain of 2 transfers (the first transfer fails the chain):
	batch = append(batch, Transfer{ID: ToUint128(2) /* ... rest of transfer ... */, Flags: linkedFlag})
	batch = append(batch, Transfer{ID: ToUint128(3) /* ... rest of transfer ... */})

	// A chain of 2 transfers (successful):
	batch = append(batch, Transfer{ID: ToUint128(3) /* ... rest of transfer ... */, Flags: linkedFlag})
	batch = append(batch, Transfer{ID: ToUint128(4) /* ... rest of transfer ... */})

	transfersRes, err = client.CreateTransfers(batch)
	// endsection:linked-events

	// section:imports
}
// endsection:imports
