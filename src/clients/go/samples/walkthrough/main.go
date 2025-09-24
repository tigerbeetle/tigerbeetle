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
	client, err := NewClient(ToUint128(0), []string{tbAddress})
	if err != nil {
		log.Printf("Error creating client: %s", err)
		return
	}
	defer client.Close()
	// endsection:client

	{
		// section:create-accounts
		accountErrors, err := client.CreateAccounts([]Account{
			{
				ID:          ID(), // TigerBeetle time-based ID.
				UserData128: ToUint128(0),
				UserData64:  0,
				UserData32:  0,
				Ledger:      1,
				Code:        718,
				Flags:       0,
				Timestamp:   0,
			},
		})
		// Error handling omitted.
		// endsection:create-accounts
		_, _ = accountErrors, err
	}

	{
		// section:account-flags
		account0 := Account{
			ID:     ToUint128(100),
			Ledger: 1,
			Code:   718,
			Flags: AccountFlags{
				DebitsMustNotExceedCredits: true,
				Linked:                     true,
			}.ToUint16(),
		}
		account1 := Account{
			ID:     ToUint128(101),
			Ledger: 1,
			Code:   718,
			Flags: AccountFlags{
				History: true,
			}.ToUint16(),
		}

		accountErrors, err := client.CreateAccounts([]Account{account0, account1})
		// Error handling omitted.
		// endsection:account-flags
		_, _ = accountErrors, err
	}

	{
		// section:create-accounts-errors
		account0 := Account{
			ID:     ToUint128(102),
			Ledger: 1,
			Code:   718,
			Flags:  0,
		}
		account1 := Account{
			ID:     ToUint128(103),
			Ledger: 1,
			Code:   718,
			Flags:  0,
		}
		account2 := Account{
			ID:     ToUint128(104),
			Ledger: 1,
			Code:   718,
			Flags:  0,
		}

		accountErrors, err := client.CreateAccounts([]Account{account0, account1, account2})
		if err != nil {
			log.Printf("Error creating accounts: %s", err)
			return
		}

		for _, err := range accountErrors {
			switch err.Index {
			case uint32(AccountExists):
				log.Printf("Batch account at %d already exists.", err.Index)
			default:
				log.Printf("Batch account at %d failed to create: %s", err.Index, err.Result)
			}
		}
		// endsection:create-accounts-errors
	}

	{
		// section:lookup-accounts
		accounts, err := client.LookupAccounts([]Uint128{ToUint128(100), ToUint128(101)})
		// endsection:lookup-accounts
		_, _ = accounts, err
	}

	{
		// section:create-transfers
		transfers := []Transfer{{
			ID:              ID(), // TigerBeetle time-based ID.
			DebitAccountID:  ToUint128(101),
			CreditAccountID: ToUint128(102),
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
			Flags:           0,
			Timestamp:       0,
		}}

		transferErrors, err := client.CreateTransfers(transfers)
		// Error handling omitted.
		// endsection:create-transfers
		_, _ = transferErrors, err
	}

	{
		// section:create-transfers-errors
		transfers := []Transfer{{
			ID:              ToUint128(1),
			DebitAccountID:  ToUint128(101),
			CreditAccountID: ToUint128(102),
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
			Flags:           0,
		}, {
			ID:              ToUint128(2),
			DebitAccountID:  ToUint128(101),
			CreditAccountID: ToUint128(102),
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
			Flags:           0,
		}, {
			ID:              ToUint128(3),
			DebitAccountID:  ToUint128(101),
			CreditAccountID: ToUint128(102),
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
			Flags:           0,
		}}

		transferErrors, err := client.CreateTransfers(transfers)
		if err != nil {
			log.Printf("Error creating transfers: %s", err)
			return
		}

		for _, err := range transferErrors {
			switch err.Index {
			case uint32(TransferExists):
				log.Printf("Batch transfer at %d already exists.", err.Index)
			default:
				log.Printf("Batch transfer at %d failed to create: %s", err.Index, err.Result)
			}
		}
		// endsection:create-transfers-errors
	}

	{
		// section:no-batch
		batch := []Transfer{}
		for i := 0; i < len(batch); i++ {
			transferErrors, err := client.CreateTransfers([]Transfer{batch[i]})
			_, _ = transferErrors, err // Error handling omitted.
		}
		// endsection:no-batch
	}

	{
		// section:batch
		batch := []Transfer{}
		BATCH_SIZE := 8189
		for i := 0; i < len(batch); i += BATCH_SIZE {
			size := BATCH_SIZE
			if i+BATCH_SIZE > len(batch) {
				size = len(batch) - i
			}
			transferErrors, err := client.CreateTransfers(batch[i : i+size])
			_, _ = transferErrors, err // Error handling omitted.
		}
		// endsection:batch
	}

	{
		// section:transfer-flags-link
		transfer0 := Transfer{
			ID:              ToUint128(4),
			DebitAccountID:  ToUint128(101),
			CreditAccountID: ToUint128(102),
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
			Flags:           TransferFlags{Linked: true}.ToUint16(),
		}
		transfer1 := Transfer{
			ID:              ToUint128(5),
			DebitAccountID:  ToUint128(101),
			CreditAccountID: ToUint128(102),
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
			Flags:           0,
		}

		transferErrors, err := client.CreateTransfers([]Transfer{transfer0, transfer1})
		// Error handling omitted.
		// endsection:transfer-flags-link
		_, _ = transferErrors, err
	}

	{
		// section:transfer-flags-post
		transfer0 := Transfer{
			ID:              ToUint128(6),
			DebitAccountID:  ToUint128(101),
			CreditAccountID: ToUint128(102),
			Amount:          ToUint128(10),
			Ledger:          1,
			Code:            1,
			Flags:           0,
		}

		transferErrors, err := client.CreateTransfers([]Transfer{transfer0})
		// Error handling omitted.

		transfer1 := Transfer{
			ID: ToUint128(7),
			// Post the entire pending amount.
			Amount:    AmountMax,
			PendingID: ToUint128(6),
			Flags:     TransferFlags{PostPendingTransfer: true}.ToUint16(),
		}

		transferErrors, err = client.CreateTransfers([]Transfer{transfer1})
		// Error handling omitted.
		// endsection:transfer-flags-post
		_, _ = transferErrors, err
	}

	{
		// section:transfer-flags-void
		transfer0 := Transfer{
			ID:              ToUint128(8),
			DebitAccountID:  ToUint128(101),
			CreditAccountID: ToUint128(102),
			Amount:          ToUint128(10),
			Timeout:         0,
			Ledger:          1,
			Code:            1,
			Flags:           0,
		}

		transferErrors, err := client.CreateTransfers([]Transfer{transfer0})
		// Error handling omitted.

		transfer1 := Transfer{
			ID:        ToUint128(9),
			Amount:    ToUint128(0),
			PendingID: ToUint128(8),
			Flags:     TransferFlags{VoidPendingTransfer: true}.ToUint16(),
		}

		transferErrors, err = client.CreateTransfers([]Transfer{transfer1})
		// Error handling omitted.
		// endsection:transfer-flags-void
		_, _ = transferErrors, err
	}

	{
		// section:lookup-transfers
		transfers, err := client.LookupTransfers([]Uint128{ToUint128(1), ToUint128(2)})
		// endsection:lookup-transfers
		_, _ = transfers, err
	}

	{
		// section:get-account-transfers
		filter := AccountFilter{
			AccountID:    ToUint128(2),
			UserData128:  ToUint128(0), // No filter by UserData.
			UserData64:   0,
			UserData32:   0,
			Code:         0,  // No filter by Code.
			TimestampMin: 0,  // No filter by Timestamp.
			TimestampMax: 0,  // No filter by Timestamp.
			Limit:        10, // Limit to ten transfers at most.
			Flags: AccountFilterFlags{
				Debits:   true, // Include transfer from the debit side.
				Credits:  true, // Include transfer from the credit side.
				Reversed: true, // Sort by timestamp in reverse-chronological order.
			}.ToUint32(),
		}

		transfers, err := client.GetAccountTransfers(filter)
		// endsection:get-account-transfers
		_, _ = transfers, err
	}

	{
		// section:get-account-balances
		filter := AccountFilter{
			AccountID:    ToUint128(2),
			UserData128:  ToUint128(0), // No filter by UserData.
			UserData64:   0,
			UserData32:   0,
			Code:         0,  // No filter by Code.
			TimestampMin: 0,  // No filter by Timestamp.
			TimestampMax: 0,  // No filter by Timestamp.
			Limit:        10, // Limit to ten balances at most.
			Flags: AccountFilterFlags{
				Debits:   true, // Include transfer from the debit side.
				Credits:  true, // Include transfer from the credit side.
				Reversed: true, // Sort by timestamp in reverse-chronological order.
			}.ToUint32(),
		}

		account_balances, err := client.GetAccountBalances(filter)
		// endsection:get-account-balances
		_, _ = account_balances, err
	}

	{
		// section:query-accounts
		filter := QueryFilter{
			UserData128:  ToUint128(1000), // Filter by UserData
			UserData64:   100,
			UserData32:   10,
			Code:         1,  // Filter by Code
			Ledger:       0,  // No filter by Ledger
			TimestampMin: 0,  // No filter by Timestamp.
			TimestampMax: 0,  // No filter by Timestamp.
			Limit:        10, // Limit to ten accounts at most.
			Flags: QueryFilterFlags{
				Reversed: true, // Sort by timestamp in reverse-chronological order.
			}.ToUint32(),
		}

		accounts, err := client.QueryAccounts(filter)
		// endsection:query-accounts
		_, _ = accounts, err
	}

	{
		// section:query-transfers
		filter := QueryFilter{
			UserData128:  ToUint128(1000), // Filter by UserData.
			UserData64:   100,
			UserData32:   10,
			Code:         1,  // Filter by Code.
			Ledger:       0,  // No filter by Ledger.
			TimestampMin: 0,  // No filter by Timestamp.
			TimestampMax: 0,  // No filter by Timestamp.
			Limit:        10, // Limit to ten transfers at most.
			Flags: QueryFilterFlags{
				Reversed: true, // Sort by timestamp in reverse-chronological order.
			}.ToUint32(),
		}

		transfers, err := client.QueryTransfers(filter)
		// endsection:query-transfers
		_, _ = transfers, err
	}

	{
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

		transferErrors, err := client.CreateTransfers(batch)
		// Error handling omitted.
		// endsection:linked-events
		_, _ = transferErrors, err
	}

	{
		// section:imported-events
		// External source of time.
		var historicalTimestamp uint64 = 0
		historicalAccounts := []Account{ /* Loaded from an external source. */ }
		historicalTransfers := []Transfer{ /* Loaded from an external source. */ }

		// First, load and import all accounts with their timestamps from the historical source.
		accountsBatch := []Account{}
		for index, account := range historicalAccounts {
			// Set a unique and strictly increasing timestamp.
			historicalTimestamp += 1
			account.Timestamp = historicalTimestamp

			account.Flags = AccountFlags{
				// Set the account as `imported`.
				Imported: true,
				// To ensure atomicity, the entire batch (except the last event in the chain)
				// must be `linked`.
				Linked: index < len(historicalAccounts)-1,
			}.ToUint16()

			accountsBatch = append(accountsBatch, account)
		}

		accountErrors, err := client.CreateAccounts(accountsBatch)
		// Error handling omitted.

		// Then, load and import all transfers with their timestamps from the historical source.
		transfersBatch := []Transfer{}
		for index, transfer := range historicalTransfers {
			// Set a unique and strictly increasing timestamp.
			historicalTimestamp += 1
			transfer.Timestamp = historicalTimestamp

			transfer.Flags = TransferFlags{
				// Set the transfer as `imported`.
				Imported: true,
				// To ensure atomicity, the entire batch (except the last event in the chain)
				// must be `linked`.
				Linked: index < len(historicalAccounts)-1,
			}.ToUint16()

			transfersBatch = append(transfersBatch, transfer)
		}

		transferErrors, err := client.CreateTransfers(transfersBatch)
		// Error handling omitted..
		// Since it is a linked chain, in case of any error the entire batch is rolled back and can be retried
		// with the same historical timestamps without regressing the cluster timestamp.
		// endsection:imported-events
		_, _, _ = accountErrors, transferErrors, err
	}

	// section:imports
}

// endsection:imports
