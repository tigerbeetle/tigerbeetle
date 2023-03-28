package tigerbeetle_go

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"unsafe"

	"github.com/tigerbeetledb/tigerbeetle-go/assert"
	"github.com/tigerbeetledb/tigerbeetle-go/pkg/types"
)

const (
	TIGERBEETLE_PORT                 = "3000"
	TIGERBEETLE_CLUSTER_ID    uint32 = 0
	TIGERBEETLE_REPLICA_ID    uint32 = 0
	TIGERBEETLE_REPLICA_COUNT uint32 = 1
)

func toU128(value string) *types.Uint128 {
	src := []byte(value)
	dst := make([]byte, unsafe.Sizeof(types.Uint128{}))
	hex.Encode(dst[:], src)
	return (*types.Uint128)(unsafe.Pointer(&dst[0]))
}

func WithClient(s testing.TB, withClient func(Client)) {
	var tigerbeetlePath string
	if runtime.GOOS == "windows" {
		tigerbeetlePath = "../../../zig-out/bin/tigerbeetle.exe"
	} else {
		tigerbeetlePath = "../../../zig-out/bin/tigerbeetle"
	}

	addressArg := "--addresses=" + TIGERBEETLE_PORT
	replicaArg := fmt.Sprintf("--replica=%d", TIGERBEETLE_REPLICA_ID)
	replicaCountArg := fmt.Sprintf("--replica-count=%d", TIGERBEETLE_REPLICA_COUNT)
	clusterArg := fmt.Sprintf("--cluster=%d", TIGERBEETLE_CLUSTER_ID)

	fileName := fmt.Sprintf("./%d_%d.tigerbeetle", TIGERBEETLE_CLUSTER_ID, TIGERBEETLE_REPLICA_ID)
	_ = os.Remove(fileName)

	tbInit := exec.Command(tigerbeetlePath, "format", clusterArg, replicaArg, replicaCountArg, fileName)
	var tbErr bytes.Buffer
	tbInit.Stdout = &tbErr
	tbInit.Stderr = &tbErr
	if err := tbInit.Run(); err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + tbErr.String())
		s.Fatal(err)
	}

	s.Cleanup(func() {
		_ = os.Remove(fileName)
	})

	tbStart := exec.Command(tigerbeetlePath, "start", addressArg, fileName)
	if err := tbStart.Start(); err != nil {
		s.Fatal(err)
	}

	s.Cleanup(func() {
		if err := tbStart.Process.Kill(); err != nil {
			s.Fatal(err)
		}
	})

	addresses := []string{"127.0.0.1:" + TIGERBEETLE_PORT}
	maxConcurrency := uint(32)
	client, err := NewClient(TIGERBEETLE_CLUSTER_ID, addresses, maxConcurrency)
	if err != nil {
		s.Fatal(err)
	}

	s.Cleanup(func() {
		client.Close()
	})

	withClient(client)
}

func TestClient(s *testing.T) {
	WithClient(s, func(client Client) {
		doTestClient(s, client)
	})
}

func doTestClient(s *testing.T, client Client) {
	accountA := types.Account{
		ID:     *toU128("a"),
		Ledger: 1,
		Code:   1,
	}
	accountB := types.Account{
		ID:     *toU128("b"),
		Ledger: 1,
		Code:   2,
	}

	s.Run("can create accounts", func(t *testing.T) {
		results, err := client.CreateAccounts([]types.Account{
			accountA,
			accountB,
		})
		if err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, results)
	})

	s.Run("can lookup accounts", func(t *testing.T) {
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
		assert.Equal(t, uint64(0), accA.DebitsPending)
		assert.Equal(t, uint64(0), accA.DebitsPosted)
		assert.Equal(t, uint64(0), accA.CreditsPending)
		assert.Equal(t, uint64(0), accA.CreditsPosted)
		assert.NotEqual(t, uint64(0), accA.Timestamp)
		assert.Equal(t, unsafe.Sizeof(accA), 128)

		accB := results[1]
		assert.Equal(t, uint32(1), accB.Ledger)
		assert.Equal(t, uint16(2), accB.Code)
		assert.Equal(t, uint16(0), accB.Flags)
		assert.Equal(t, uint64(0), accB.DebitsPending)
		assert.Equal(t, uint64(0), accB.DebitsPosted)
		assert.Equal(t, uint64(0), accB.CreditsPending)
		assert.Equal(t, uint64(0), accB.CreditsPosted)
		assert.NotEqual(t, uint64(0), accB.Timestamp)
	})

	s.Run("can create a transfer", func(t *testing.T) {
		results, err := client.CreateTransfers([]types.Transfer{
			{
				ID:              *toU128("a"),
				CreditAccountID: accountA.ID,
				DebitAccountID:  accountB.ID,
				Amount:          100,
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
		assert.Equal(t, uint64(0), accountA.DebitsPending)
		assert.Equal(t, uint64(0), accountA.DebitsPosted)
		assert.Equal(t, uint64(0), accountA.CreditsPending)
		assert.Equal(t, uint64(100), accountA.CreditsPosted)

		accountB = accounts[1]
		assert.Equal(t, uint64(0), accountB.DebitsPending)
		assert.Equal(t, uint64(100), accountB.DebitsPosted)
		assert.Equal(t, uint64(0), accountB.CreditsPending)
		assert.Equal(t, uint64(0), accountB.CreditsPosted)
	})

	s.Run("can create linked transfers", func(t *testing.T) {
		transfer1 := types.Transfer{
			ID:              *toU128("d"),
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          50,
			Flags:           types.TransferFlags{Linked: true}.ToUint16(), // points to transfer 2
			Code:            1,
			Ledger:          1,
		}
		transfer2 := types.Transfer{
			ID:              *toU128("d"),
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          50,
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
		assert.Equal(t, uint64(100), accountA.CreditsPosted)
		assert.Equal(t, uint64(0), accountA.CreditsPending)
		assert.Equal(t, uint64(0), accountA.DebitsPosted)
		assert.Equal(t, uint64(0), accountA.DebitsPending)

		accountB = accounts[1]
		assert.Equal(t, uint64(0), accountB.CreditsPosted)
		assert.Equal(t, uint64(0), accountB.CreditsPending)
		assert.Equal(t, uint64(100), accountB.DebitsPosted)
		assert.Equal(t, uint64(0), accountB.DebitsPending)
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
