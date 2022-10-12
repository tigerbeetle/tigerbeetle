package main

import (
	"fmt"
	"os"
	"time"
	"unsafe"
)

type Uint128 struct {
	data [16]byte
}

type Transfer struct {
	id                Uint128
	debit_account_id  Uint128
	credit_account_id Uint128
	user_data         Uint128
	reserved          Uint128
	pending_iD        Uint128
	timeout           uint64
	ledger            uint32
	code              uint16
	flags             uint16
	amount            uint64
	timestamp         uint64
}

const TRANSFER int = 128

func main() {

	buffer := load("transfers")

	var loops int = 10
	for loops > 0 {
		var offset int = 0
		var sum uint64 = 0
		now := time.Now()

		for offset < len(buffer) {

			// Deserialize without any overhead:
			transfer := (*Transfer)(unsafe.Pointer(&buffer[offset]))
			sum += transfer.amount
			offset += TRANSFER
		}

		elapsed := time.Since(now)
		fmt.Printf("  go: sum of transfer amounts=%d ns=%d\n", sum, elapsed.Nanoseconds())
		loops -= 1
	}
}

func load(name string) []byte {

	file, openErr := os.Open(name)
	if openErr != nil {
		panic(openErr)
	}
	defer file.Close()

	stats, statsErr := file.Stat()
	if statsErr != nil {
		panic(statsErr)
	}

	var size int64 = stats.Size()
	bytes := make([]byte, size)

	_, readErr := file.Read(bytes)
	if readErr != nil {
		panic(readErr)
	}

	return bytes
}
