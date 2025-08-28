package tigerbeetle_go

import (
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/tigerbeetle/tigerbeetle-go/assert"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

const (
	TIGERBEETLE_PORT                 = "3000"
	TIGERBEETLE_CLUSTER_ID    uint64 = 0
	TIGERBEETLE_REPLICA_ID    uint32 = 0
	TIGERBEETLE_REPLICA_COUNT uint32 = 1
)

func HexStringToUint128(value string) types.Uint128 {
	number, err := types.HexStringToUint128(value)
	if err != nil {
		panic(err)
	}
	return number
}

func WithClient(t testing.TB, withClient func(Client)) {
	var tigerbeetlePath string
	if runtime.GOOS == "windows" {
		tigerbeetlePath = "../../../tigerbeetle.exe"
	} else {
		tigerbeetlePath = "../../../tigerbeetle"
	}

	addressArg := "--addresses=" + TIGERBEETLE_PORT
	cacheSizeArg := "--cache-grid=256MiB"
	replicaArg := fmt.Sprintf("--replica=%d", TIGERBEETLE_REPLICA_ID)
	replicaCountArg := fmt.Sprintf("--replica-count=%d", TIGERBEETLE_REPLICA_COUNT)
	clusterArg := fmt.Sprintf("--cluster=%d", TIGERBEETLE_CLUSTER_ID)

	fileName := fmt.Sprintf("./%d_%d_%d.tigerbeetle", TIGERBEETLE_CLUSTER_ID, TIGERBEETLE_REPLICA_ID, rand.Int())
	t.Cleanup(func() {
		_ = os.Remove(fileName)
	})

	tbInit := exec.Command(tigerbeetlePath, "format", clusterArg, replicaArg, replicaCountArg, fileName)
	var tbErr bytes.Buffer
	tbInit.Stdout = &tbErr
	tbInit.Stderr = &tbErr
	if err := tbInit.Run(); err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + tbErr.String())
		t.Fatal(err)
	}

	tbStart := exec.Command(tigerbeetlePath, "start", addressArg, cacheSizeArg, fileName)
	if testing.Verbose() {
		tbStart.Stderr = os.Stderr
	}
	if err := tbStart.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := tbStart.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	})

	addresses := []string{"127.0.0.1:" + TIGERBEETLE_PORT}
	client, err := NewClient(types.ToUint128(TIGERBEETLE_CLUSTER_ID), addresses)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	withClient(client)
}

func TestClient(t *testing.T) {
	WithClient(t, func(client Client) {
		doTestClient(t, client)
	})
}

func TestImportedFlag(t *testing.T) {
	// This test cannot run in parallel with the others because it needs an
	// stable "timestamp max" reference.
	WithClient(t, func(client Client) {
		doTestImportedFlag(t, client)
	})
}

func doTestClient(t *testing.T, client Client) {
	createTwoAccounts := func(t *testing.T) (types.Account, types.Account) {
		accountA := types.Account{
			ID:     types.ID(),
			Ledger: 1,
			Code:   1,
		}
		accountB := types.Account{
			ID:     types.ID(),
			Ledger: 1,
			Code:   2,
		}

		results, err := client.CreateAccounts([]types.Account{
			accountA,
			accountB,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Empty(t, results)

		return accountA, accountB
	}

	/// Consistency of U128 across Zig and the language clients.
	/// It must be kept in sync with all platforms.
	t.Run("u128 consistency", func(t *testing.T) {
		t.Parallel()

		// Binary little endian representation:
		// Using signed bytes for convenience to match Java's representation:
		binary := [16]byte{
			0xe6, 0xe5, 0xe4, 0xe3, 0xe2, 0xe1,
			0xd2, 0xd1,
			0xc2, 0xc1,
			0xb2, 0xb1,
			0xa4, 0xa3, 0xa2, 0xa1,
		}
		decimal, ok := big.NewInt(0).SetString("214850178493633095719753766415838275046", 10)
		if !ok {
			t.Fatal()
		}

		u128 := types.BytesToUint128(binary)

		assert.Equal(t, u128.Bytes(), binary)
		assert.Equal(t, u128.BigInt(), *decimal)
		assert.Equal(t, types.BigIntToUint128(*decimal).Bytes(), u128.Bytes())
	})

	t.Run("can create accounts", func(t *testing.T) {
		t.Parallel()
		createTwoAccounts(t)
	})

	t.Run("can lookup accounts", func(t *testing.T) {
		t.Parallel()
		accountA, accountB := createTwoAccounts(t)

		results, err := client.LookupAccounts([]types.Uint128{
			accountA.ID,
			accountB.ID,
		})
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, results, 2)
		accA := results[0]
		assert.Equal(t, uint32(1), accA.Ledger)
		assert.Equal(t, uint16(1), accA.Code)
		assert.Equal(t, uint16(0), accA.Flags)
		assert.Equal(t, types.ToUint128(0), accA.DebitsPending)
		assert.Equal(t, types.ToUint128(0), accA.DebitsPosted)
		assert.Equal(t, types.ToUint128(0), accA.CreditsPending)
		assert.Equal(t, types.ToUint128(0), accA.CreditsPosted)
		assert.NotEqual(t, uint64(0), accA.Timestamp)
		assert.Equal(t, unsafe.Sizeof(accA), 128)

		accB := results[1]
		assert.Equal(t, uint32(1), accB.Ledger)
		assert.Equal(t, uint16(2), accB.Code)
		assert.Equal(t, uint16(0), accB.Flags)
		assert.Equal(t, types.ToUint128(0), accB.DebitsPending)
		assert.Equal(t, types.ToUint128(0), accB.DebitsPosted)
		assert.Equal(t, types.ToUint128(0), accB.CreditsPending)
		assert.Equal(t, types.ToUint128(0), accB.CreditsPosted)
		assert.NotEqual(t, uint64(0), accB.Timestamp)
	})

	t.Run("can create a transfer", func(t *testing.T) {
		t.Parallel()
		accountA, accountB := createTwoAccounts(t)

		results, err := client.CreateTransfers([]types.Transfer{
			{
				ID:              types.ID(),
				CreditAccountID: accountA.ID,
				DebitAccountID:  accountB.ID,
				Amount:          types.ToUint128(100),
				Ledger:          1,
				Code:            1,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, results)

		accounts, err := client.LookupAccounts([]types.Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)

		accountA = accounts[0]
		assert.Equal(t, types.ToUint128(0), accountA.DebitsPending)
		assert.Equal(t, types.ToUint128(0), accountA.DebitsPosted)
		assert.Equal(t, types.ToUint128(0), accountA.CreditsPending)
		assert.Equal(t, types.ToUint128(100), accountA.CreditsPosted)

		accountB = accounts[1]
		assert.Equal(t, types.ToUint128(0), accountB.DebitsPending)
		assert.Equal(t, types.ToUint128(100), accountB.DebitsPosted)
		assert.Equal(t, types.ToUint128(0), accountB.CreditsPending)
		assert.Equal(t, types.ToUint128(0), accountB.CreditsPosted)
	})

	t.Run("can create linked transfers", func(t *testing.T) {
		t.Parallel()
		accountA, accountB := createTwoAccounts(t)

		id := types.ID()
		transfer1 := types.Transfer{
			ID:              id,
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          types.ToUint128(50),
			Flags:           types.TransferFlags{Linked: true}.ToUint16(), // points to transfer 2
			Code:            1,
			Ledger:          1,
		}
		transfer2 := types.Transfer{
			ID:              id,
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          types.ToUint128(50),
			// Does not have linked flag as it is the end of the chain.
			// This will also cause it to fail as this is now a duplicate with different flags
			Flags:  0,
			Code:   1,
			Ledger: 1,
		}
		results, err := client.CreateTransfers([]types.Transfer{transfer1, transfer2})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, results, 2)
		assert.Equal(t, unsafe.Sizeof(transfer1), 128)
		assert.Equal(t, types.TransferEventResult{Index: 0, Result: types.TransferLinkedEventFailed}, results[0])
		assert.Equal(t, types.TransferEventResult{Index: 1, Result: types.TransferExistsWithDifferentFlags}, results[1])

		accounts, err := client.LookupAccounts([]types.Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)

		accountA = accounts[0]
		assert.Equal(t, types.ToUint128(0), accountA.CreditsPosted)
		assert.Equal(t, types.ToUint128(0), accountA.CreditsPending)
		assert.Equal(t, types.ToUint128(0), accountA.DebitsPosted)
		assert.Equal(t, types.ToUint128(0), accountA.DebitsPending)

		accountB = accounts[1]
		assert.Equal(t, types.ToUint128(0), accountB.CreditsPosted)
		assert.Equal(t, types.ToUint128(0), accountB.CreditsPending)
		assert.Equal(t, types.ToUint128(0), accountB.DebitsPosted)
		assert.Equal(t, types.ToUint128(0), accountB.DebitsPending)
	})

	t.Run("can close accounts", func(t *testing.T) {
		t.Parallel()
		accountA, accountB := createTwoAccounts(t)

		closingTransfer := types.Transfer{
			ID:              types.ID(),
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          types.ToUint128(0),
			Flags: types.TransferFlags{
				ClosingDebit:  true,
				ClosingCredit: true,
				Pending:       true,
			}.ToUint16(),
			Code:   1,
			Ledger: 1,
		}
		results, err := client.CreateTransfers([]types.Transfer{closingTransfer})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, results, 0)

		accounts, err := client.LookupAccounts([]types.Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)

		assert.NotEqual(t, accountA.Flags, accounts[0].Flags)
		assert.True(t, accounts[0].AccountFlags().Closed)

		assert.NotEqual(t, accountB.Flags, accounts[1].Flags)
		assert.True(t, accounts[1].AccountFlags().Closed)

		voidingTransfer := types.Transfer{
			ID:              types.ID(),
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          types.ToUint128(0),
			Flags: types.TransferFlags{
				VoidPendingTransfer: true,
			}.ToUint16(),
			PendingID: closingTransfer.ID,
			Code:      1,
			Ledger:    1,
		}
		results, err = client.CreateTransfers([]types.Transfer{voidingTransfer})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, results, 0)

		accounts, err = client.LookupAccounts([]types.Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)

		assert.Equal(t, accountA.Flags, accounts[0].Flags)
		assert.True(t, !accounts[0].AccountFlags().Closed)

		assert.Equal(t, accountB.Flags, accounts[1].Flags)
		assert.True(t, !accounts[1].AccountFlags().Closed)
	})

	t.Run("accept zero-length create_accounts", func(t *testing.T) {
		t.Parallel()
		results, err := client.CreateAccounts([]types.Account{})
		if err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, results)
	})

	t.Run("accept zero-length create_transfers", func(t *testing.T) {
		t.Parallel()
		results, err := client.CreateTransfers([]types.Transfer{})
		if err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, results)
	})

	t.Run("accept zero-length lookup_accounts", func(t *testing.T) {
		t.Parallel()
		results, err := client.LookupAccounts([]types.Uint128{})
		if err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, results)
	})

	t.Run("accept zero-length lookup_transfers", func(t *testing.T) {
		t.Parallel()
		results, err := client.LookupTransfers([]types.Uint128{})
		if err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, results)
	})

	t.Run("can submit concurrent requests", func(t *testing.T) {
		accountA, accountB := createTwoAccounts(t)
		accounts, err := client.LookupAccounts([]types.Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)
		accountACredits := accounts[0].CreditsPosted.BigInt()
		accountBDebits := accounts[1].DebitsPosted.BigInt()

		const TASKS_MAX = 1_000_000
		var waitGroup sync.WaitGroup
		for i := 0; i < TASKS_MAX; i++ {
			waitGroup.Add(1)

			go func(i int) {
				defer waitGroup.Done()
				if i%2 == 0 {
					results, err := client.CreateTransfers([]types.Transfer{
						{
							ID:              types.ID(),
							CreditAccountID: accountA.ID,
							DebitAccountID:  accountB.ID,
							Amount:          types.ToUint128(1),
							Ledger:          1,
							Code:            1,
						},
					})
					if err != nil {
						t.Error(err)
						return
					}
					assert.Empty(t, results)
				} else {
					results, err := client.LookupAccounts([]types.Uint128{accountA.ID})
					if err != nil {
						t.Error(err)
						return
					}
					assert.Len(t, results, 1)
					assert.Equal(t, results[0].ID, accountA.ID)
				}
			}(i)
		}
		waitGroup.Wait()

		accounts, err = client.LookupAccounts([]types.Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)
		accountACreditsAfter := accounts[0].CreditsPosted.BigInt()
		accountBDebitsAfter := accounts[1].DebitsPosted.BigInt()

		// Each transfer moves ONE unit,
		// so the credit/debit must differ from TRANSFERS_MAX units:
		assert.Equal(t, TASKS_MAX/2, big.NewInt(0).Sub(&accountACreditsAfter, &accountACredits).Int64())
		assert.Equal(t, TASKS_MAX/2, big.NewInt(0).Sub(&accountBDebitsAfter, &accountBDebits).Int64())
	})

	t.Run("can create concurrent linked chains", func(t *testing.T) {
		accountA, accountB := createTwoAccounts(t)

		// NB: this test is _not_ parallel, so can use up all the concurrency.
		const TRANSFERS_MAX = 10_000

		accounts, err := client.LookupAccounts([]types.Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)

		var waitGroup sync.WaitGroup
		for i := 0; i < TRANSFERS_MAX; i++ {
			waitGroup.Add(1)
			go func(i int) {
				defer waitGroup.Done()

				// The Linked flag will cause the
				// batch to fail due to LinkedEventChainOpen.
				flags := types.TransferFlags{Linked: i%10 == 0}.ToUint16()
				results, err := client.CreateTransfers([]types.Transfer{
					{
						ID:              types.ID(),
						CreditAccountID: accountA.ID,
						DebitAccountID:  accountB.ID,
						Amount:          types.ToUint128(1),
						Ledger:          1,
						Code:            1,
						Flags:           flags,
					},
				})

				if err != nil {
					t.Error(err)
					return
				}

				if i%10 == 0 {
					assert.Len(t, results, 1)
					assert.Equal(t, results[0].Result, types.TransferLinkedEventChainOpen)
				} else {
					assert.Empty(t, results)
				}
			}(i)
		}
		waitGroup.Wait()
	})

	t.Run("can query transfers for an account", func(t *testing.T) {
		t.Parallel()
		accountA, accountB := createTwoAccounts(t)

		BATCH_MAX := uint32(8189)

		// Create a new account:
		accountC := types.Account{
			ID:     types.ID(),
			Ledger: 1,
			Code:   1,
			Flags: types.AccountFlags{
				History: true,
			}.ToUint16(),
		}
		account_results, err := client.CreateAccounts([]types.Account{accountC})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, account_results, 0)

		// Create transfers where the new account is either the debit or credit account:
		transfers_created := make([]types.Transfer, 10)
		for i := 0; i < 10; i++ {
			transfer_id := types.ID()

			// Swap debit and credit accounts:
			if i%2 == 0 {
				transfers_created[i] = types.Transfer{
					ID:              transfer_id,
					CreditAccountID: accountA.ID,
					DebitAccountID:  accountC.ID,
					Amount:          types.ToUint128(50),
					Flags:           0,
					Code:            1,
					Ledger:          1,
				}
			} else {
				transfers_created[i] = types.Transfer{
					ID:              transfer_id,
					CreditAccountID: accountC.ID,
					DebitAccountID:  accountB.ID,
					Amount:          types.ToUint128(50),
					Flags:           0,
					Code:            1,
					Ledger:          1,
				}
			}
		}
		transfer_results, err := client.CreateTransfers(transfers_created)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, transfer_results, 0)

		// Query all transfers for accountC:
		filter := types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err := client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err := client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, len(transfers_created))
		assert.Len(t, account_balances, len(transfers_retrieved))

		timestamp := uint64(0)
		for i, transfer := range transfers_retrieved {
			assert.True(t, timestamp < transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Timestamp == account_balances[i].Timestamp)
		}

		// Query only the debit transfers for accountC, descending:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  false,
				Reversed: true,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, len(transfers_created)/2)
		assert.Len(t, account_balances, len(transfers_retrieved))

		timestamp = ^uint64(0)
		for i, transfer := range transfers_retrieved {
			assert.True(t, transfer.Timestamp < timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Timestamp == account_balances[i].Timestamp)
		}

		// Query only the credit transfers for accountC, descending:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.AccountFilterFlags{
				Debits:   false,
				Credits:  true,
				Reversed: true,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, len(transfers_created)/2)
		assert.Len(t, account_balances, len(transfers_retrieved))

		timestamp = ^uint64(0)
		for i, transfer := range transfers_retrieved {
			assert.True(t, transfer.Timestamp < timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Timestamp == account_balances[i].Timestamp)
		}

		// Query the first 5 transfers for accountC:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        uint32(len(transfers_created) / 2),
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, len(transfers_created)/2)
		assert.Len(t, account_balances, len(transfers_retrieved))

		timestamp = 0
		for i, transfer := range transfers_retrieved {
			assert.True(t, timestamp < transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Timestamp == account_balances[i].Timestamp)
		}

		// Query the next 5 transfers for accountC, with pagination:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: timestamp + 1,
			TimestampMax: 0,
			Limit:        uint32(len(transfers_created) / 2),
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, len(transfers_created)/2)
		assert.Len(t, account_balances, len(transfers_retrieved))

		for i, transfer := range transfers_retrieved {
			assert.True(t, timestamp < transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Timestamp == account_balances[i].Timestamp)
		}

		// Query again, no more transfers should be found:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: timestamp + 1,
			TimestampMax: 0,
			Limit:        uint32(len(transfers_created) / 2),
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))

		// Query the first 5 transfers for accountC order by DESC:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        uint32(len(transfers_created) / 2),
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: true,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, len(transfers_created)/2)
		assert.Len(t, account_balances, len(transfers_retrieved))

		timestamp = ^uint64(0)
		for i, transfer := range transfers_retrieved {
			assert.True(t, timestamp > transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Timestamp == account_balances[i].Timestamp)
		}

		// Query the next 5 transfers for accountC, with pagination:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: timestamp - 1,
			Limit:        uint32(len(transfers_created) / 2),
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: true,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, len(transfers_created)/2)
		assert.Len(t, account_balances, len(transfers_retrieved))

		for i, transfer := range transfers_retrieved {
			assert.True(t, timestamp > transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Timestamp == account_balances[i].Timestamp)
		}

		// Query again, no more transfers should be found:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: timestamp - 1,
			Limit:        uint32(len(transfers_created) / 2),
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: true,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))

		// Invalid account:
		filter = types.AccountFilter{
			AccountID:    types.ToUint128(0),
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))

		// Invalid timestamp min:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: ^uint64(0), // ulong max value
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))

		// Invalid timestamp max:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: ^uint64(0), // ulong max value
			Limit:        BATCH_MAX,
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))

		// Invalid timestamps:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: (^uint64(0)) - 1, // ulong max - 1
			TimestampMax: 1,
			Limit:        BATCH_MAX,
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))

		// Zero limit:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        0,
			Flags: types.AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))

		// Empty flags:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags:        0,
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))

		// Invalid flags:
		filter = types.AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags:        0xFFFF,
		}
		transfers_retrieved, err = client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err = client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))
	})

	t.Run("can query accounts", func(t *testing.T) {
		t.Parallel()

		BATCH_MAX := uint32(8189)

		// Creating accounts:
		accounts_created := make([]types.Account, 10)
		for i := 0; i < 10; i++ {
			account_id := types.ID()

			if i%2 == 0 {
				accounts_created[i] = types.Account{
					ID:          account_id,
					UserData128: types.ToUint128(1000),
					UserData64:  100,
					UserData32:  10,
					Code:        999,
					Ledger:      1,
					Flags:       0,
				}
			} else {
				accounts_created[i] = types.Account{
					ID:          account_id,
					UserData128: types.ToUint128(2000),
					UserData64:  200,
					UserData32:  20,
					Code:        999,
					Ledger:      1,
					Flags:       0,
				}
			}
		}
		account_results, err := client.CreateAccounts(accounts_created)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, account_results, 0)

		// Querying accounts where:
		// `user_data_128=1000 AND user_data_64=100 AND user_data_32=10
		// AND code=999 AND ledger=1 ORDER BY timestamp ASC`.
		filter := types.QueryFilter{
			UserData128:  types.ToUint128(1000),
			UserData64:   100,
			UserData32:   10,
			Code:         999,
			Ledger:       1,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		query, err := client.QueryAccounts(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 5)

		timestamp := uint64(0)
		for _, account := range query {
			assert.True(t, timestamp < account.Timestamp)
			timestamp = account.Timestamp

			assert.True(t, account.UserData128 == filter.UserData128)
			assert.True(t, account.UserData64 == filter.UserData64)
			assert.True(t, account.UserData32 == filter.UserData32)
			assert.True(t, account.Code == filter.Code)
			assert.True(t, account.Ledger == filter.Ledger)
		}

		// Querying accounts where:
		// `user_data_128=2000 AND user_data_64=200 AND user_data_32=20
		// AND code=999 AND ledger=1 ORDER BY timestamp DESC`.
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(2000),
			UserData64:   200,
			UserData32:   20,
			Code:         999,
			Ledger:       1,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: true,
			}.ToUint32(),
		}
		query, err = client.QueryAccounts(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 5)

		timestamp = ^uint64(0) // ulong max value
		for _, account := range query {
			assert.True(t, timestamp > account.Timestamp)
			timestamp = account.Timestamp

			assert.True(t, account.UserData128 == filter.UserData128)
			assert.True(t, account.UserData64 == filter.UserData64)
			assert.True(t, account.UserData32 == filter.UserData32)
			assert.True(t, account.Code == filter.Code)
			assert.True(t, account.Ledger == filter.Ledger)
		}

		// Querying accounts where:
		// `code=999 ORDER BY timestamp ASC`.
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Code:         999,
			Ledger:       0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		query, err = client.QueryAccounts(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 10)

		timestamp = uint64(0)
		for _, account := range query {
			assert.True(t, timestamp < account.Timestamp)
			timestamp = account.Timestamp

			assert.True(t, account.Code == filter.Code)
		}

		// Querying accounts where:
		// `code=999 ORDER BY timestamp DESC LIMIT 5`.
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Code:         999,
			Ledger:       0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        5,
			Flags: types.QueryFilterFlags{
				Reversed: true,
			}.ToUint32(),
		}

		// First 5 items:
		query, err = client.QueryAccounts(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 5)

		timestamp = ^uint64(0) // ulong max value
		for _, account := range query {
			assert.True(t, timestamp > account.Timestamp)
			timestamp = account.Timestamp

			assert.True(t, account.Code == filter.Code)
		}

		// Next 5 items from this timestamp:
		filter.TimestampMax = timestamp - 1
		query, err = client.QueryAccounts(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 5)

		for _, account := range query {
			assert.True(t, timestamp > account.Timestamp)
			timestamp = account.Timestamp

			assert.True(t, account.Code == filter.Code)
		}

		// No more results:
		filter.TimestampMax = timestamp - 1
		query, err = client.QueryAccounts(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)

		// Not found:
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   200,
			UserData32:   10,
			Code:         0,
			Ledger:       0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}

		query, err = client.QueryAccounts(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)
	})

	t.Run("can query transfers", func(t *testing.T) {
		t.Parallel()
		accountA, accountB := createTwoAccounts(t)

		BATCH_MAX := uint32(8189)

		// Create a new account:
		account := types.Account{
			ID:     types.ID(),
			Ledger: 1,
			Code:   1,
			Flags:  0,
		}
		account_results, err := client.CreateAccounts([]types.Account{account})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, account_results, 0)

		// Creating transfers:
		transfers_created := make([]types.Transfer, 10)
		for i := 0; i < 10; i++ {
			transfer_id := types.ID()

			if i%2 == 0 {
				transfers_created[i] = types.Transfer{
					ID:              transfer_id,
					CreditAccountID: accountA.ID,
					DebitAccountID:  account.ID,
					Amount:          types.ToUint128(100),
					Flags:           0,
					UserData128:     types.ToUint128(1000),
					UserData64:      100,
					UserData32:      10,
					Code:            999,
					Ledger:          1,
				}
			} else {
				transfers_created[i] = types.Transfer{
					ID:              transfer_id,
					CreditAccountID: account.ID,
					DebitAccountID:  accountB.ID,
					Amount:          types.ToUint128(100),
					Flags:           0,
					UserData128:     types.ToUint128(2000),
					UserData64:      200,
					UserData32:      20,
					Code:            999,
					Ledger:          1,
				}
			}
		}
		transfer_results, err := client.CreateTransfers(transfers_created)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, transfer_results, 0)

		// Querying transfers where:
		// `user_data_128=1000 AND user_data_64=100 AND user_data_32=10
		// AND code=999 AND ledger=1 ORDER BY timestamp ASC`.
		filter := types.QueryFilter{
			UserData128:  types.ToUint128(1000),
			UserData64:   100,
			UserData32:   10,
			Code:         999,
			Ledger:       1,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		query, err := client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 5)

		timestamp := uint64(0)
		for _, transfer := range query {
			assert.True(t, timestamp < transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.UserData128 == filter.UserData128)
			assert.True(t, transfer.UserData64 == filter.UserData64)
			assert.True(t, transfer.UserData32 == filter.UserData32)
			assert.True(t, transfer.Code == filter.Code)
			assert.True(t, transfer.Ledger == filter.Ledger)
		}

		// Querying transfers where:
		// `user_data_128=2000 AND user_data_64=200 AND user_data_32=20
		// AND code=999 AND ledger=1 ORDER BY timestamp DESC`.
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(2000),
			UserData64:   200,
			UserData32:   20,
			Code:         999,
			Ledger:       1,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: true,
			}.ToUint32(),
		}
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 5)

		timestamp = ^uint64(0) // ulong max value
		for _, transfer := range query {
			assert.True(t, timestamp > transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.UserData128 == filter.UserData128)
			assert.True(t, transfer.UserData64 == filter.UserData64)
			assert.True(t, transfer.UserData32 == filter.UserData32)
			assert.True(t, transfer.Code == filter.Code)
			assert.True(t, transfer.Ledger == filter.Ledger)
		}

		// Querying transfers where:
		// `code=999 ORDER BY timestamp ASC`.
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Code:         999,
			Ledger:       0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 10)

		timestamp = uint64(0)
		for _, transfer := range query {
			assert.True(t, timestamp < transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Code == filter.Code)
		}

		// Querying transfers where:
		// `code=999 ORDER BY timestamp DESC LIMIT 5`.
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Code:         999,
			Ledger:       0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        5,
			Flags: types.QueryFilterFlags{
				Reversed: true,
			}.ToUint32(),
		}

		// First 5 items:
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 5)

		timestamp = ^uint64(0) // ulong max value
		for _, transfer := range query {
			assert.True(t, timestamp > transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Code == filter.Code)
		}

		// Next 5 items from this timestamp:
		filter.TimestampMax = timestamp - 1
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 5)

		for _, transfer := range query {
			assert.True(t, timestamp > transfer.Timestamp)
			timestamp = transfer.Timestamp

			assert.True(t, transfer.Code == filter.Code)
		}

		// No more results:
		filter.TimestampMax = timestamp - 1
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)

		// Not found:
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   200,
			UserData32:   10,
			Code:         0,
			Ledger:       0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}

		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)
	})

	t.Run("invalid query filters", func(t *testing.T) {
		t.Parallel()

		BATCH_MAX := uint32(8189)

		// Invalid timestamp min:
		filter := types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Ledger:       0,
			Code:         0,
			TimestampMin: ^uint64(0), // ulong max value
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		query, err := client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)

		// Invalid timestamp max:
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Ledger:       0,
			Code:         0,
			TimestampMin: 0,
			TimestampMax: ^uint64(0), // ulong max value,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)

		// Invalid timestamps:
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Ledger:       0,
			Code:         0,
			TimestampMin: (^uint64(0)) - 1, // ulong max - 1,
			TimestampMax: 1,
			Limit:        BATCH_MAX,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)

		// Zero limit:
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Ledger:       0,
			Code:         0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        0,
			Flags: types.QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)

		// Invalid flags:
		filter = types.QueryFilter{
			UserData128:  types.ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Ledger:       0,
			Code:         0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags:        0xFFFF,
		}
		query, err = client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)
	})

	t.Run("get change events", func(t *testing.T) {
		t.Parallel()
		filter := types.ChangeEventsFilter{
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        10,
		}
		events, err := client.GetChangeEvents(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, events, int(filter.Limit))
	})
}

func doTestImportedFlag(t *testing.T, client Client) {
	t.Run("can import accounts and transfers", func(t *testing.T) {
		tmpAccount := types.ID()
		tmpResults, err := client.CreateAccounts([]types.Account{
			{
				ID:     tmpAccount,
				Ledger: 1,
				Code:   2,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Empty(t, tmpResults)

		tmpAccounts, err := client.LookupAccounts([]types.Uint128{tmpAccount})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, tmpAccounts, 1)

		// Wait 10 ms so we can use the account's timestamp as the reference for past time
		// after the last object inserted.
		time.Sleep(10 * time.Millisecond)
		timestampMax := tmpAccounts[0].Timestamp

		accountA := types.ID()
		accountB := types.ID()
		transferA := types.ID()

		accountResults, err := client.CreateAccounts([]types.Account{
			{
				ID:     accountA,
				Ledger: 1,
				Code:   1,
				Flags: types.AccountFlags{
					Imported: true,
				}.ToUint16(),
				Timestamp: timestampMax + 1,
			},
			{
				ID:     accountB,
				Ledger: 1,
				Code:   2,
				Flags: types.AccountFlags{
					Imported: true,
				}.ToUint16(),
				Timestamp: timestampMax + 2,
			}})
		if err != nil {
			t.Fatal(err)
		}
		assert.Empty(t, accountResults)

		transfersResults, err := client.CreateTransfers([]types.Transfer{
			{
				ID:              transferA,
				CreditAccountID: accountA,
				DebitAccountID:  accountB,
				Amount:          types.ToUint128(100),
				Ledger:          1,
				Code:            1,
				Flags: types.TransferFlags{
					Imported: true,
				}.ToUint16(),
				Timestamp: timestampMax + 3,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Empty(t, transfersResults)

		accounts, err := client.LookupAccounts([]types.Uint128{accountA, accountB})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)
		assert.Equal(t, timestampMax+1, accounts[0].Timestamp)
		assert.Equal(t, timestampMax+2, accounts[1].Timestamp)

		transfers, err := client.LookupTransfers([]types.Uint128{transferA})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, transfers, 1)
		assert.Equal(t, timestampMax+3, transfers[0].Timestamp)
	})
}

func BenchmarkNop(b *testing.B) {
	WithClient(b, func(client Client) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := client.Nop(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
