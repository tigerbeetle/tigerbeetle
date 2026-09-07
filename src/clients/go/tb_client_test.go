package tigerbeetle_go

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/tigerbeetle/tigerbeetle-go/assert"
)

const (
	TIGERBEETLE_CLUSTER_ID    uint64 = 0
	TIGERBEETLE_REPLICA_ID    uint32 = 0
	TIGERBEETLE_REPLICA_COUNT uint32 = 1
)

func WithClient(t testing.TB, withClient func(Client)) {
	tigerbeetlePath := os.Getenv("TIGERBEETLE_BINARY")
	if tigerbeetlePath == "" {
		t.Fatalf("TIGERBEETLE_BINARY environmental variable is required")
	}

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

	tbStart := exec.Command(tigerbeetlePath,
		"start",
		"--development",
		"--addresses=0",
		fileName)

	if testing.Verbose() {
		tbStart.Stderr = os.Stderr
	}

	// Stdin is not used,
	// but when running with `--addresses=0`, the replica exits if stdin is closed.
	stdin, err := tbStart.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = stdin

	// Stdout is used to read the assigned TCP port.
	stdout, err := tbStart.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}

	if err := tbStart.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tbStart.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	})

	reader := bufio.NewReader(stdout)
	port, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}

	addresses := strings.TrimSpace(port)
	client, err := NewClient(ToUint128(TIGERBEETLE_CLUSTER_ID), addresses)
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
	createTwoAccounts := func(t *testing.T) (Account, Account) {
		accountA := Account{
			ID:     ID(),
			Ledger: 1,
			Code:   1,
		}
		accountB := Account{
			ID:     ID(),
			Ledger: 1,
			Code:   2,
		}

		results, err := client.CreateAccounts([]Account{
			accountA,
			accountB,
		})
		if err != nil {
			t.Fatal(err)
		}
		assertCreateAccountsOK(t, results, 2)

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
		decimal, ok := new(big.Int).SetString("214850178493633095719753766415838275046", 10)
		if !ok {
			t.Fatal()
		}

		u128 := BytesToUint128(binary)

		assert.Equal(t, u128.Bytes(), binary)
		assert.Equal(t, u128.BigInt(), decimal)
		assert.Equal(t, BigIntToUint128(decimal).Bytes(), u128.Bytes())

		lo, hi := u128.Uint64()
		assert.True(t, lo == 15119395263638463974)
		assert.True(t, hi == 11647051514084770242)
	})

	t.Run("can lookup accounts", func(t *testing.T) {
		t.Parallel()
		accountA, accountB := createTwoAccounts(t)

		results, err := client.LookupAccounts([]Uint128{
			accountA.ID,
			accountB.ID,
		})
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, results, 2)
		accA := results[0]
		assert.Equal(t, unsafe.Sizeof(accA), 128)
	})

	t.Run("can submit concurrent requests", func(t *testing.T) {
		accountA, accountB := createTwoAccounts(t)
		accounts, err := client.LookupAccounts([]Uint128{accountA.ID, accountB.ID})
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
					results, err := client.CreateTransfers([]Transfer{
						{
							ID:              ID(),
							CreditAccountID: accountA.ID,
							DebitAccountID:  accountB.ID,
							Amount:          ToUint128(1),
							Ledger:          1,
							Code:            1,
						},
					})
					if err != nil {
						t.Error(err)
						return
					}
					assertCreateTransfersOK(t, results, 1)
				} else {
					results, err := client.LookupAccounts([]Uint128{accountA.ID})
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

		accounts, err = client.LookupAccounts([]Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)
		accountACreditsAfter := accounts[0].CreditsPosted.BigInt()
		accountBDebitsAfter := accounts[1].DebitsPosted.BigInt()

		// Each transfer moves ONE unit,
		// so the credit/debit must differ from TRANSFERS_MAX units:
		assert.Equal(t, TASKS_MAX/2, big.NewInt(0).Sub(accountACreditsAfter, accountACredits).Int64())
		assert.Equal(t, TASKS_MAX/2, big.NewInt(0).Sub(accountBDebitsAfter, accountBDebits).Int64())
	})

	t.Run("can create concurrent linked chains", func(t *testing.T) {
		accountA, accountB := createTwoAccounts(t)

		// NB: this test is _not_ parallel, so can use up all the concurrency.
		const TRANSFERS_MAX = 10_000

		accounts, err := client.LookupAccounts([]Uint128{accountA.ID, accountB.ID})
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
				flags := TransferFlags{Linked: i%10 == 0}.ToUint16()
				results, err := client.CreateTransfers([]Transfer{
					{
						ID:              ID(),
						CreditAccountID: accountA.ID,
						DebitAccountID:  accountB.ID,
						Amount:          ToUint128(1),
						Ledger:          1,
						Code:            1,
						Flags:           flags,
					},
				})
				if err != nil {
					t.Error(err)
					return
				}

				assert.Len(t, results, 1)
				assert.True(t, results[0].Timestamp > 0)
				if i%10 == 0 {
					assert.Equal(t, results[0].Status, TransferLinkedEventChainOpen)
				} else {
					assert.Equal(t, results[0].Status, TransferCreated)
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
		accountC := Account{
			ID:     ID(),
			Ledger: 1,
			Code:   1,
			Flags: AccountFlags{
				History: true,
			}.ToUint16(),
		}
		account_results, err := client.CreateAccounts([]Account{accountC})
		if err != nil {
			t.Fatal(err)
		}
		assertCreateAccountsOK(t, account_results, 1)

		// Create transfers where the new account is either the debit or credit account:
		transfers_created := make([]Transfer, 10)
		for i := 0; i < 10; i++ {
			transfer_id := ID()

			// Swap debit and credit accounts:
			if i%2 == 0 {
				transfers_created[i] = Transfer{
					ID:              transfer_id,
					CreditAccountID: accountA.ID,
					DebitAccountID:  accountC.ID,
					Amount:          ToUint128(50),
					Flags:           0,
					Code:            1,
					Ledger:          1,
				}
			} else {
				transfers_created[i] = Transfer{
					ID:              transfer_id,
					CreditAccountID: accountC.ID,
					DebitAccountID:  accountB.ID,
					Amount:          ToUint128(50),
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
		assertCreateTransfersOK(t, transfer_results, len(transfers_created))

		// TooMuchData:
		filter := AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        10_000,
			Flags: AccountFilterFlags{
				Debits:   true,
				Credits:  true,
				Reversed: false,
			}.ToUint32(),
		}

		_, err = client.GetAccountTransfers(filter)
		assert.True(t, errors.Is(err, ErrTooMuchData))

		_, err = client.GetAccountBalances(filter)
		assert.True(t, errors.Is(err, ErrTooMuchData))

		// Invalid flags:
		filter = AccountFilter{
			AccountID:    accountC.ID,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags:        0xFFFF,
		}
		transfers_retrieved, err := client.GetAccountTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		account_balances, err := client.GetAccountBalances(filter)
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers_retrieved, 0)
		assert.Len(t, account_balances, len(transfers_retrieved))
	})

	t.Run("invalid query filters", func(t *testing.T) {
		t.Parallel()

		BATCH_MAX := uint32(8189)

		// TooMuchData:
		filter := QueryFilter{
			UserData128:  ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Ledger:       0,
			Code:         0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        10_000,
			Flags: QueryFilterFlags{
				Reversed: false,
			}.ToUint32(),
		}
		_, err := client.QueryTransfers(filter)
		assert.True(t, errors.Is(err, ErrTooMuchData))

		_, err = client.QueryAccounts(filter)
		assert.True(t, errors.Is(err, ErrTooMuchData))

		// Invalid flags:
		filter = QueryFilter{
			UserData128:  ToUint128(0),
			UserData64:   0,
			UserData32:   0,
			Ledger:       0,
			Code:         0,
			TimestampMin: 0,
			TimestampMax: 0,
			Limit:        BATCH_MAX,
			Flags:        0xFFFF,
		}
		query, err := client.QueryTransfers(filter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, query, 0)
	})

	t.Run("get change events", func(t *testing.T) {
		t.Parallel()
		filter := ChangeEventsFilter{
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
		tmpAccount := ID()
		tmpResults, err := client.CreateAccounts([]Account{
			{
				ID:     tmpAccount,
				Ledger: 1,
				Code:   2,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		assertCreateAccountsOK(t, tmpResults, 1)

		tmpAccounts, err := client.LookupAccounts([]Uint128{tmpAccount})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, tmpAccounts, 1)

		// Wait 10 ms so we can use the account's timestamp as the reference for past time
		// after the last object inserted.
		time.Sleep(10 * time.Millisecond)
		timestampMax := tmpAccounts[0].Timestamp

		accountA := ID()
		accountB := ID()
		transferA := ID()

		accountResults, err := client.CreateAccounts([]Account{
			{
				ID:     accountA,
				Ledger: 1,
				Code:   1,
				Flags: AccountFlags{
					Imported: true,
				}.ToUint16(),
				Timestamp: timestampMax + 1,
			},
			{
				ID:     accountB,
				Ledger: 1,
				Code:   2,
				Flags: AccountFlags{
					Imported: true,
				}.ToUint16(),
				Timestamp: timestampMax + 2,
			}})
		if err != nil {
			t.Fatal(err)
		}
		assertCreateAccountsOK(t, accountResults, 2)

		transferResults, err := client.CreateTransfers([]Transfer{
			{
				ID:              transferA,
				CreditAccountID: accountA,
				DebitAccountID:  accountB,
				Amount:          ToUint128(100),
				Ledger:          1,
				Code:            1,
				Flags: TransferFlags{
					Imported: true,
				}.ToUint16(),
				Timestamp: timestampMax + 3,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		assertCreateTransfersOK(t, transferResults, 1)

		accounts, err := client.LookupAccounts([]Uint128{accountA, accountB})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)
		assert.Equal(t, timestampMax+1, accounts[0].Timestamp)
		assert.Equal(t, timestampMax+2, accounts[1].Timestamp)

		transfers, err := client.LookupTransfers([]Uint128{transferA})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, transfers, 1)
		assert.Equal(t, timestampMax+3, transfers[0].Timestamp)
	})
}

func assertCreateAccountsOK(t *testing.T, results []CreateAccountResult, expected int) {
	assert.Len(t, results, expected)
	for _, result := range results {
		assert.True(t, result.Timestamp > 0)
		assert.Equal(t, result.Status, AccountCreated)
	}
}

func assertCreateTransfersOK(t *testing.T, results []CreateTransferResult, expected int) {
	assert.Len(t, results, expected)
	for _, result := range results {
		assert.True(t, result.Timestamp > 0)
		assert.Equal(t, result.Status, TransferCreated)
	}
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
