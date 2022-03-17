package main

/*
#cgo LDFLAGS: -L../../zig-out -ltb_client

#include <stdlib.h>
#include <string.h>
#include "./tb_client.h"

typedef const uint8_t* tb_result_bytes_t;
void onGoPacketCompletion(
	uintptr_t ctx,
	tb_client_t client,
	tb_packet_t* packet,
	tb_result_bytes_t result_ptr,
	uint32_t result_len
);
*/
import "C"
import (
	"log"
	"unsafe"
	"strings"
	"errors"
)

type Uint128 = C.tb_uint128_t
type Account = C.tb_account_t
type Transfer = C.tb_transfer_t
type Commit = C.tb_commit_t

type CreateAccountResult = C.TB_CREATE_ACCOUNT_RESULT
type CreateTransferResult = C.TB_CREATE_TRANSFER_RESULT
type CommitTransferResult = C.TB_COMMIT_TRANSFER_RESULT

type CreateAccountsResult = C.tb_create_accounts_result_t
type CreateTransfersResult = C.tb_create_transfers_result_t
type CommitTransfersResult = C.tb_commit_transfers_result_t

///////////////////////////////////////////////////////////////

type Client interface {
	CreateAccounts([]Account) ([]CreateAccountsResult, error)
	Close()
}

type request struct {
	packet *C.tb_packet_t
	result unsafe.Pointer
	ready chan struct{}
}

type c_client struct {
	tb_client C.tb_client_t
	max_requests uint32
	requests chan *request
}

type clientError struct {
	status C.TB_STATUS
}

func (e clientError) Error() string {
	switch e.status {
	case C.TB_STATUS_UNEXPECTED:
		return "An unexpected error occured when trying to create the TB client"
	case C.TB_STATUS_OUT_OF_MEMORY:
		return "Creating the TB client failed due to running out of memory"
	case C.TB_STATUS_INVALID_ADDRESS:
		return "The address provided for the TB client to connect to was invalid"
	case C.TB_STATUS_SYSTEM_RESOURCES:
		return "The system ran out of resources when creating the TB client"
	case C.TB_STATUS_NETWORK_SUBSYSTEM:
		return "The system's networking failed when creating the TB client"
	default:
		panic("invalid error code")
	}
}

func NewClient(
	clusterID uint32, 
	addresses []string, 
	maxConcurrency uint32,
) (Client, error) {
	// Cap the maximum amount of packets
	if maxConcurrency > 4096 {
		maxConcurrency = 4096
	}
	
	// Allocate a cstring of the addresses joined with ","
	addresses_raw := strings.Join(addresses[:], ",")
	c_addresses := C.CString(addresses_raw)
	defer C.free(unsafe.Pointer(c_addresses))

	var tb_client C.tb_client_t
	var packets C.tb_packet_list_t

	// Create the tb_client
	status := C.tb_client_init(
		&tb_client,
		&packets,
		C.uint32_t(clusterID),
		c_addresses,
		C.uint32_t(len(addresses_raw)),
		C.uint32_t(maxConcurrency),
		C.uintptr_t(0), // on_completion_ctx
		(*[0]byte)(C.onGoPacketCompletion),
	)

	if status != C.TB_STATUS_SUCCESS {
		return nil, clientError{ status: status }
	}

	c := &c_client{
		tb_client: tb_client,
		max_requests: maxConcurrency,
		requests: make(chan *request, int(maxConcurrency)),
	}

	// Fill the client requests with available packets we received on creation
	for packet := packets.head; packet != nil; packet = packet.next {
		c.requests <- &request{
			packet: packet,
			ready: make(chan struct{}),
		}
	}
	
	return c, nil
}

func (c *c_client) Close() {
	// Consume all requests available (waits for pending ones to complete)
	for i := 0; i < int(c.max_requests); i++ {
		req := <- c.requests
		_ = req
	}

	// Now that there can be no active requests,
	// destroy the tb_client and ensure no future requests can procceed
	C.tb_client_deinit(c.tb_client)
	close(c.requests)
}

type packetError struct {
	status C.TB_PACKET_STATUS
}

func (e packetError) Error() string {
	switch e.status {
	case C.TB_PACKET_TOO_MUCH_DATA:
		return "Request was given too much data"
	case C.TB_PACKET_INVALID_OPERATION:
		panic("unreachable") // we control what operation is given
	case C.TB_PACKET_INVALID_DATA_SIZE:
		panic("unreachable") // we control what data is given
	default:
		panic("invalid error code")
	}
}

func getEventSize(op C.TB_OPERATION) uintptr {
	switch (op) {
	case C.TB_OP_CREATE_ACCOUNTS:
		return unsafe.Sizeof(Account{})
	case C.TB_OP_CREATE_TRANSFERS:
		return unsafe.Sizeof(Transfer{})
	case C.TB_OP_COMMIT_TRANSFERS:
		return unsafe.Sizeof(Commit{})
	case C.TB_OP_LOOKUP_ACCOUNTS:
	case C.TB_OP_LOOKUP_TRANSFERS:
		return unsafe.Sizeof(Uint128{})
	}
	panic("invalid tigerbeetle operation")
}

func getResultSize(op C.TB_OPERATION) uintptr {
	switch (op) {
	case C.TB_OP_CREATE_ACCOUNTS:
		return unsafe.Sizeof(CreateAccountsResult{})
	case C.TB_OP_CREATE_TRANSFERS:
		return unsafe.Sizeof(CreateTransfersResult{})
	case C.TB_OP_COMMIT_TRANSFERS:
		return unsafe.Sizeof(CommitTransfersResult{})
	case C.TB_OP_LOOKUP_ACCOUNTS:
		return unsafe.Sizeof(Account{})
	case C.TB_OP_LOOKUP_TRANSFERS:
		return unsafe.Sizeof(Transfer{})
	}
	panic("invalid tigerbeetle operation")
}

func (c *c_client) doRequest(
	op C.TB_OPERATION,
	count int,
	data unsafe.Pointer,
	result unsafe.Pointer,
) (int, error) {	
	// Get (and possibly wait) for a request to use.
	// Returns false if the client was Close()'d.
	req, isOpen := <- c.requests
	if !isOpen {
		return 0, errors.New("Tigerbeetle client was closed")
	}

	// Setup the packet.
	req.packet.next = nil
	req.packet.user_data = C.uintptr_t((uintptr)((unsafe.Pointer)(req)))
	req.packet.operation = C.uint8_t(op)
	req.packet.status = C.TB_PACKET_OK
	req.packet.data_size = C.uint32_t(count * int(getEventSize(op)))
	req.packet.data = data

	// Set where to write the result bytes.
	req.result = result

	// Submit the request.
	var list C.tb_packet_list_t
	list.head = req.packet
	list.tail = req.packet
	C.tb_client_submit(c.tb_client, &list)

	// Wait for the request to complete.
	<- req.ready
	status := C.TB_PACKET_STATUS(req.packet.status)
	wrote := int(req.packet.data_size)

	// Free the request for other goroutines to use.
	c.requests <- req
	
	// Handle packet error
	if status != C.TB_PACKET_OK {
		return 0, packetError{ status: status }
	}

	// Return the amount of bytes written into result
	return wrote, nil
}

//export onGoPacketCompletion
func onGoPacketCompletion(
	_context C.uintptr_t,
	client C.tb_client_t,
	packet *C.tb_packet_t,
	result_ptr C.tb_result_bytes_t,
	result_len C.uint32_t,
) {
	// Get the request from the packet user data
	req := (*request)((unsafe.Pointer)((uintptr)(packet.user_data)))
	op := C.TB_OPERATION(packet.operation)

	var wrote C.uint32_t
	if result_ptr != nil {
		// Make sure the completion handler is giving us valid data
		resultSize := C.uint32_t(getResultSize(op))
		if result_len == 0 || (result_len % resultSize != 0) {
			panic("invalid result_len: zero or misaligned for the event")
		}

		// Make sure the amount of results at least matches the amount of requests
		count := packet.data_size / C.uint32_t(getEventSize(op))
		if count * resultSize < result_len {
			panic("invalid result_len: implied multiple results per event")
		}
		
		// Write the result data into the request's result
		if req.result != nil {
			wrote = result_len
			C.memcpy(req.result, unsafe.Pointer(result_ptr), C.size_t(result_len))
		}
	}

	// Signal to the goroutine which owns this request that it's ready
	req.packet.data_size = wrote
	req.ready <- struct{}{}
}

func (c *c_client) CreateAccounts(accounts []Account) ([]CreateAccountsResult, error) {
	count := len(accounts)
	results := make([]CreateAccountsResult, count)
	wrote, err := c.doRequest(
		C.TB_OP_CREATE_ACCOUNTS,
		count,
		unsafe.Pointer(&accounts[0]),
		unsafe.Pointer(&results[0]),
	)

	if err != nil {
		return nil, err
	}

	resultCount := wrote / int(unsafe.Sizeof(CreateAccountsResult{}))
	return results[0:resultCount], nil
}


func main() {
	cluster := uint32(0)
	addresses := []string{"127.0.0.1:3001"}

	client, err := NewClient(cluster, addresses, 32)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	_, err = client.CreateAccounts([]Account{
		Account{},
	})

	if err != nil {
		log.Fatal(err)
	}
}