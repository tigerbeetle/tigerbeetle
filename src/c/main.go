package main

/*
#cgo LDFLAGS: -L../../zig-out -ltb_client

#include <stdlib.h>
#include <string.h>
#include "./tb_client.h"

void onGoPacketCompletion(uintptr_t ctx, tb_client_t client, tb_packet_list_t* packets);

static void tb_client_request(
	tb_client_t client,
	tb_packet_t* packet,
	TB_OPERATION operation,
	void* request_data,
	uintptr_t user_data
) {
	size_t req_bytes;
	switch (operation)
	{
	case TB_OP_CREATE_ACCOUNTS:
		req_bytes = sizeof(tb_account_t);
		break;
	case TB_OP_CREATE_TRANSFERS:
		req_bytes = sizeof(tb_transfer_t);
		break;
	case TB_OP_COMMIT_TRANSFERS:
		req_bytes = sizeof(tb_commit_t);
		break;
	case TB_OP_LOOKUP_ACCOUNTS:
	case TB_OP_LOOKUP_TRANSFERS:
		req_bytes = sizeof(tb_uint128_t);
		break;
	default:
		__builtin_unreachable();
	}

	packet->user_data = user_data;
	packet->operation = operation;
	memcpy(&packet->data.request, request_data, req_bytes);

	tb_packet_list_t list;
	list.head = packet;
	list.tail = packet;
	packet->next = NULL;

	tb_client_submit(client, &list);
}

static void tb_client_response(
	tb_packet_t* packet,
	void* response_data
) {
	size_t resp_bytes;
	switch (packet->operation)
	{
	case TB_OP_CREATE_ACCOUNTS:
		resp_bytes = sizeof(tb_create_accounts_result_t);
		break;
	case TB_OP_CREATE_TRANSFERS:
		resp_bytes = sizeof(tb_create_transfers_result_t);
		break;
	case TB_OP_COMMIT_TRANSFERS:
		resp_bytes = sizeof(tb_commit_transfers_result_t);
		break;
	case TB_OP_LOOKUP_ACCOUNTS:
		resp_bytes = sizeof(tb_account_t);
		break;
	case TB_OP_LOOKUP_TRANSFERS:
		resp_bytes = sizeof(tb_transfer_t);
		break;
	default:
		__builtin_unreachable();
	}

	memcpy(response_data, &packet->data.response, resp_bytes);
}
*/
import "C"

import (
	"unsafe"
	"strings"
)

type Uint128 = C.tb_uint128_t

type Account = C.tb_account_t
type Transfer = C.tb_transfer_t
type Commit = C.tb_commit_t

type CreateAccountsResult = C.tb_create_accounts_result_t
type CreateTransfersResult = C.tb_create_transfers_result_t
type CommitTransfersResult = C.tb_commit_transfers_result_t

type ErrClient struct {
	status C.TB_STATUS
}

func (e ErrClient) Error() string {
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

///////////////////////////////////////////////////////////////

type Client interface {
	CreateAccounts(Account) (CreateAccountsResult, error)
	CreateTransfers(Transfer) (CreateTransfersResult, error)
	CommitTransfers(Commit) (CommitTransfersResult, error)
	LookupAccounts(Uint128) (Account, error)
	LookupTransfers(Uint128) (Transfer, error)
	Close()
}

type request struct {
	packet *C.tb_packet_t
	ready chan struct{}
}

type c_client struct {
	tb_client C.tb_client_t
	requests chan *request
}

//export onGoPacketCompletion
func onGoPacketCompletion(_ctx C.uintptr_t, client C.tb_client_t, packets *C.tb_packet_list_t) {
	packet := packets.head
	for packet != nil {
		req := (*request)((unsafe.Pointer)((uintptr)(packet.user_data)))
		packet = packet.next
		req.ready <- struct{}{}
	}
}

func NewClient(clusterID uint32, addresses []string) (Client, error) {
	addresses_raw := strings.Join(addresses[:], ",")
	c_addresses := C.CString(addresses_raw)
	defer C.free(unsafe.Pointer(c_addresses))

	num_packets := 32
	var tb_client C.tb_client_t
	var packets C.tb_packet_list_t

	status := C.tb_client_init(
		&tb_client,
		&packets,
		C.uint32_t(clusterID),
		c_addresses,
		C.uint32_t(len(addresses_raw)),
		C.uint32_t(num_packets),
		C.uintptr_t(0), // on_completion_ctx
		(*[0]byte)(C.onGoPacketCompletion),
	)

	if status != C.TB_STATUS_SUCCESS {
		return nil, ErrClient{ status: status }
	}

	c := &c_client{
		tb_client: tb_client,
		requests: make(chan *request, num_packets),
	}

	for packet := packets.head; packet != nil; packet = packet.next {
		c.requests <- &request{
			packet: packet,
			ready: make(chan struct{}),
		}
	}
	
	return c, nil
}

func (c *c_client) doRequest(op C.TB_OPERATION, event_data, result_data unsafe.Pointer) {
	req := <- c.requests

	user_data := C.uintptr_t((uintptr)((unsafe.Pointer)(req)))
	C.tb_client_request(c.tb_client, req.packet, op, event_data, user_data)
	
	<- req.ready
	C.tb_client_response(req.packet, result_data)

	c.requests <- req
}

func (c *c_client) CreateAccounts(account Account) (CreateAccountsResult, error) {
	var result CreateAccountsResult
	c.doRequest(C.TB_OP_CREATE_ACCOUNTS, (unsafe.Pointer)(&account), (unsafe.Pointer)(&result))

	if result.index != 0 {
		panic("invalid CreateAccountsResult index")
	}
	
	var err error
	switch result.result {
	case C.TB_CREATE_ACCOUNT_OK:
		err = nil
		break
	default:
		panic("todo")
	}

	return result, err
}

func (c *c_client) CreateTransfers(transfer Transfer) (CreateTransfersResult, error) {
	var r CreateTransfersResult
	return r, nil
}

func (c *c_client) CommitTransfers(commit Commit) (CommitTransfersResult, error) {
	var r CommitTransfersResult
	return r, nil
}

func (c *c_client) LookupAccounts(account_id Uint128) (Account, error) {
	var r Account
	return r, nil
}

func (c *c_client) LookupTransfers(transfer_id Uint128) (Transfer, error) {
	var r Transfer
	return r, nil
}

func (c *c_client) Close() {
	C.tb_client_deinit(c.tb_client)
}


func main() {

}