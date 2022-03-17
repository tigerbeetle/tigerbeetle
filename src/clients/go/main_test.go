package tigerbeetle_go

import (
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/coilhq/tigerbeetle_go/pkg/types"
	"github.com/stretchr/testify/assert"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	TIGERBEETLE_PORT              = "3000"
	TIGERBEETLE_CLUSTER_ID uint32 = 0
	TIGERBEETLE_DIR               = "/var/lib/tigerbeetle"
)

type TigerBeetleContainer struct {
	testcontainers.Container
	URI     string
	DataDir string
}

func setupTigerBeetle(ctx context.Context) (*TigerBeetleContainer, error) {
	tigerbeetleImage := "tigerbeetle"

	dataDir, err := ioutil.TempDir("", "tb")
	if err != nil {
		return nil, err
	}

	fmt.Println("Initializing TigerBeetle temporary data dir:" + dataDir)
	initReq := testcontainers.ContainerRequest{
		Image:        tigerbeetleImage, // TODO: host image
		ExposedPorts: []string{TIGERBEETLE_PORT},
		WaitingFor:   wait.ForLog("info: initialized data file").WithPollInterval(1 * time.Second),
		BindMounts: map[string]string{
			TIGERBEETLE_DIR: dataDir,
		},
		Cmd: []string{
			"init",
			fmt.Sprintf("--cluster=%d", TIGERBEETLE_CLUSTER_ID),
			"--replica=0",
			"--directory=" + TIGERBEETLE_DIR,
		},
	}
	_, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: initReq,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	fmt.Println("Starting TigerBeetle test container.")
	req := testcontainers.ContainerRequest{
		Image:        tigerbeetleImage, // TODO: host image
		ExposedPorts: []string{TIGERBEETLE_PORT},
		BindMounts: map[string]string{
			TIGERBEETLE_DIR: dataDir,
		},
		WaitingFor: wait.ForLog("info: cluster=0 replica=0: listening on 0.0.0.0:3000").WithPollInterval(1 * time.Second),
		Cmd: []string{
			"start",
			fmt.Sprintf("--cluster=%d", TIGERBEETLE_CLUSTER_ID),
			"--replica=0",
			"--addresses=0.0.0.0:" + TIGERBEETLE_PORT,
			"--directory=" + TIGERBEETLE_DIR,
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	mappedPort, err := container.MappedPort(ctx, TIGERBEETLE_PORT)
	if err != nil {
		return nil, err
	}

	return &TigerBeetleContainer{Container: container, URI: fmt.Sprintf("0.0.0.0:%s", mappedPort.Port()), DataDir: dataDir}, nil
}

func toU128(value string) *types.Uint128 {
	src := []byte(value)
	dst := new(types.Uint128)
	hex.Encode(dst[:], src)

	return dst
}

func TestClient(s *testing.T) {
	tbContainer, err := setupTigerBeetle(context.Background())
	if err != nil {
		s.Fatal(err)
	}

	addresses := []string{tbContainer.URI}
	maxConcurrency := uint(32)
	client, err := NewClient(TIGERBEETLE_CLUSTER_ID, addresses, maxConcurrency)
	if err != nil {
		s.Fatal(err)
	}

	s.Cleanup(func() {
		client.Close()
		os.RemoveAll(tbContainer.DataDir)
	})

	accountA := types.Account{
		ID:   *toU128("a"),
		Unit: 1,
		Code: 1,
	}
	accountB := types.Account{
		ID:   *toU128("b"),
		Unit: 1,
		Code: 2,
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
		assert.Equal(t, uint16(1), accA.Unit)
		assert.Equal(t, uint16(1), accA.Code)
		assert.Equal(t, uint32(0), accA.Flags)
		assert.Equal(t, uint64(0), accA.DebitsReserved)
		assert.Equal(t, uint64(0), accA.DebitsAccepted)
		assert.Equal(t, uint64(0), accA.CreditsReserved)
		assert.Equal(t, uint64(0), accA.CreditsAccepted)
		assert.NotEqual(t, uint64(0), accA.TimeStamp)

		accB := results[1]
		assert.Equal(t, uint16(1), accB.Unit)
		assert.Equal(t, uint16(2), accB.Code)
		assert.Equal(t, uint32(0), accB.Flags)
		assert.Equal(t, uint64(0), accB.DebitsReserved)
		assert.Equal(t, uint64(0), accB.DebitsAccepted)
		assert.Equal(t, uint64(0), accB.CreditsReserved)
		assert.Equal(t, uint64(0), accB.CreditsAccepted)
		assert.NotEqual(t, uint64(0), accB.TimeStamp)
	})

	s.Run("can create a transfer", func(t *testing.T) {
		results, err := client.CreateTransfers([]types.Transfer{
			{
				ID:              *toU128("a"),
				CreditAccountID: accountA.ID,
				DebitAccountID:  accountB.ID,
				Amount:          100,
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
		assert.Equal(t, uint64(100), accountA.CreditsAccepted)
		assert.Equal(t, uint64(0), accountA.CreditsReserved)
		assert.Equal(t, uint64(0), accountA.DebitsAccepted)
		assert.Equal(t, uint64(0), accountA.DebitsReserved)

		accountB = accounts[1]
		assert.Equal(t, uint64(0), accountB.CreditsAccepted)
		assert.Equal(t, uint64(0), accountB.CreditsReserved)
		assert.Equal(t, uint64(100), accountB.DebitsAccepted)
		assert.Equal(t, uint64(0), accountB.DebitsReserved)
	})

	s.Run("can create a two-phase transfer", func(t *testing.T) {
		transfer := types.Transfer{
			ID:              *toU128("b"),
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          50,
			Flags:           types.TransferFlags{TwoPhaseCommit: true}.ToUint32(),
			Code:            1,
			Timeout:         2e9,
		}
		results, err := client.CreateTransfers([]types.Transfer{transfer})
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
		assert.Equal(t, uint64(100), accountA.CreditsAccepted)
		assert.Equal(t, uint64(50), accountA.CreditsReserved)
		assert.Equal(t, uint64(0), accountA.DebitsAccepted)
		assert.Equal(t, uint64(0), accountA.DebitsReserved)

		accountB = accounts[1]
		assert.Equal(t, uint64(0), accountB.CreditsAccepted)
		assert.Equal(t, uint64(0), accountB.CreditsReserved)
		assert.Equal(t, uint64(100), accountB.DebitsAccepted)
		assert.Equal(t, uint64(50), accountB.DebitsReserved)

		transfers, err := client.LookupTransfers([]types.Uint128{transfer.ID})
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, transfers, 1)
		assert.Equal(t, transfers[0].ID, transfer.ID)
		assert.Equal(t, transfers[0].DebitAccountID, accountB.ID)
		assert.Equal(t, transfers[0].CreditAccountID, accountA.ID)
		assert.Equal(t, transfers[0].UserData, *new(types.Uint128))
		assert.Equal(t, transfers[0].Reserved, *new([32]uint8))
		assert.Greater(t, transfers[0].Timeout, uint64(0))
		assert.Equal(t, transfers[0].Code, uint32(1))
		assert.Equal(t, transfers[0].Flags, uint32(2))
		assert.Equal(t, transfers[0].Amount, uint64(50))
		assert.Greater(t, transfers[0].Timestamp, uint64(0))
	})

	s.Run("can commit a two-phase transfer", func(t *testing.T) {
		transferID := *toU128("b")
		commit := types.Commit{
			ID:    transferID,
			Flags: 0,
			Code:  1,
		}
		results, err := client.CommitTransfers([]types.Commit{commit})
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
		assert.Equal(t, uint64(150), accountA.CreditsAccepted)
		assert.Equal(t, uint64(0), accountA.CreditsReserved)
		assert.Equal(t, uint64(0), accountA.DebitsAccepted)
		assert.Equal(t, uint64(0), accountA.DebitsReserved)

		accountB = accounts[1]
		assert.Equal(t, uint64(0), accountB.CreditsAccepted)
		assert.Equal(t, uint64(0), accountB.CreditsReserved)
		assert.Equal(t, uint64(150), accountB.DebitsAccepted)
		assert.Equal(t, uint64(0), accountB.DebitsReserved)
	})

	s.Run("can reject a two-phase transfer", func(t *testing.T) {
		transfer := types.Transfer{
			ID:              *toU128("c"),
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          50,
			Flags:           types.TransferFlags{TwoPhaseCommit: true}.ToUint32(),
			Code:            1,
			Timeout:         2e9,
		}
		results, err := client.CreateTransfers([]types.Transfer{transfer})
		if err != nil {
			t.Fatal(err)
		}

		assert.Empty(t, results)

		reject := types.Commit{
			ID:    transfer.ID,
			Code:  1,
			Flags: types.CommitFlags{Reject: true}.ToUint32(),
		}

		results, err = client.CommitTransfers([]types.Commit{reject})
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
		assert.Equal(t, uint64(150), accountA.CreditsAccepted)
		assert.Equal(t, uint64(0), accountA.CreditsReserved)
		assert.Equal(t, uint64(0), accountA.DebitsAccepted)
		assert.Equal(t, uint64(0), accountA.DebitsReserved)

		accountB = accounts[1]
		assert.Equal(t, uint64(0), accountB.CreditsAccepted)
		assert.Equal(t, uint64(0), accountB.CreditsReserved)
		assert.Equal(t, uint64(150), accountB.DebitsAccepted)
		assert.Equal(t, uint64(0), accountB.DebitsReserved)
	})

	s.Run("can create linked transfers", func(t *testing.T) {
		transfer1 := types.Transfer{
			ID:              *toU128("d"),
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          50,
			Flags:           types.TransferFlags{Linked: true}.ToUint32(), // points to transfer 2
			Code:            1,
		}
		transfer2 := types.Transfer{
			ID:              *toU128("d"),
			CreditAccountID: accountA.ID,
			DebitAccountID:  accountB.ID,
			Amount:          50,
			// Does not have linked flag as it is the end of the chain.
			// This will also cause it to fail as this is now a duplicate with different flags
			Flags: 0,
			Code:  1,
		}
		results, err := client.CreateTransfers([]types.Transfer{transfer1, transfer2})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, results, 2)
		assert.Equal(t, types.EventResult{Index: 0, Code: types.TransferLinkedEventFailed}, results[0])
		assert.Equal(t, types.EventResult{Index: 1, Code: types.TransferExistsWithDifferentFlags}, results[1])

		accounts, err := client.LookupAccounts([]types.Uint128{accountA.ID, accountB.ID})
		if err != nil {
			t.Fatal(err)
		}
		assert.Len(t, accounts, 2)

		accountA = accounts[0]
		assert.Equal(t, uint64(150), accountA.CreditsAccepted)
		assert.Equal(t, uint64(0), accountA.CreditsReserved)
		assert.Equal(t, uint64(0), accountA.DebitsAccepted)
		assert.Equal(t, uint64(0), accountA.DebitsReserved)

		accountB = accounts[1]
		assert.Equal(t, uint64(0), accountB.CreditsAccepted)
		assert.Equal(t, uint64(0), accountB.CreditsReserved)
		assert.Equal(t, uint64(150), accountB.DebitsAccepted)
		assert.Equal(t, uint64(0), accountB.DebitsReserved)
	})
}
