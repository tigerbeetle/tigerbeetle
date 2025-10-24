package tigerbeetle_go

/*
#cgo CFLAGS: -g -Wall
#cgo darwin,arm64 LDFLAGS: ${SRCDIR}/native/libtb_client_aarch64-macos.a -ldl -lm
#cgo darwin,amd64 LDFLAGS: ${SRCDIR}/native/libtb_client_x86_64-macos.a -ldl -lm
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/native/libtb_client_aarch64-linux.a -ldl -lm
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/native/libtb_client_x86_64-linux.a -ldl -lm
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/native -ltb_client_x86_64-windows -lws2_32 -lntdll

#include <stdlib.h>
#include <string.h>
#include "./native/tb_client.h"

#ifndef __declspec
	#define __declspec(x)
#endif

typedef const uint8_t* tb_result_bytes_t;

extern __declspec(dllexport) void onGoPacketCompletion(
	uintptr_t ctx,
	tb_packet_t* packet,
	uint64_t timestamp,
	tb_result_bytes_t result_ptr,
	uint32_t result_len
);
*/
import "C"
import (
	e "errors"
	"runtime"
	"strings"
	"unsafe"

	_ "github.com/tigerbeetle/tigerbeetle-go/native"
)

///////////////////////////////////////////////////////////////

var AmountMax = BytesToUint128([16]byte{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
})

type Client interface {
	CreateAccounts(accounts []Account) ([]CreateAccountResult, error)
	CreateTransfers(transfers []Transfer) ([]CreateTransferResult, error)
	LookupAccounts(accountIDs []Uint128) ([]Account, error)
	LookupTransfers(transferIDs []Uint128) ([]Transfer, error)
	GetAccountTransfers(filter AccountFilter) ([]Transfer, error)
	GetAccountBalances(filter AccountFilter) ([]AccountBalance, error)
	QueryAccounts(filter QueryFilter) ([]Account, error)
	QueryTransfers(filter QueryFilter) ([]Transfer, error)

	// Experimental: GetChangeEvents API is undocumented.
	GetChangeEvents(filter ChangeEventsFilter) ([]ChangeEvent, error)

	Nop() error
	Close()
}

type request struct {
	ready chan []uint8
}

type c_client struct {
	tb_client *C.tb_client_t
}

func NewClient(
	clusterID Uint128,
	addresses []string,
) (Client, error) {
	// Allocate a cstring of the addresses joined with ",".
	addresses_raw := strings.Join(addresses[:], ",")
	c_addresses := C.CString(addresses_raw)
	defer C.free(unsafe.Pointer(c_addresses))

	tb_client := new(C.tb_client_t)
	var cluster_id = C.tb_uint128_t(clusterID)

	// Create the tb_client.
	init_status := C.tb_client_init(
		tb_client,
		(*C.uint8_t)(unsafe.Pointer(&cluster_id)),
		c_addresses,
		C.uint32_t(len(addresses_raw)),
		C.uintptr_t(0), // on_completion_ctx
		(*[0]byte)(C.onGoPacketCompletion),
	)

	if init_status != C.TB_INIT_SUCCESS {
		switch init_status {
		case C.TB_INIT_UNEXPECTED:
			return nil, ErrUnexpected{}
		case C.TB_INIT_OUT_OF_MEMORY:
			return nil, ErrOutOfMemory{}
		case C.TB_INIT_ADDRESS_INVALID:
			return nil, ErrInvalidAddress{}
		case C.TB_INIT_ADDRESS_LIMIT_EXCEEDED:
			return nil, ErrAddressLimitExceeded{}
		case C.TB_INIT_SYSTEM_RESOURCES:
			return nil, ErrSystemResources{}
		case C.TB_INIT_NETWORK_SUBSYSTEM:
			return nil, ErrNetworkSubsystem{}
		default:
			panic("tb_client_init(): invalid error code")
		}
	}

	c := &c_client{
		tb_client: tb_client,
	}

	return c, nil
}

func (c *c_client) Close() {
	_ = C.tb_client_deinit(c.tb_client)
}

func getEventSize(op C.TB_OPERATION) uintptr {
	switch op {
	case C.TB_OPERATION_CREATE_ACCOUNTS:
		return unsafe.Sizeof(Account{})
	case C.TB_OPERATION_CREATE_TRANSFERS:
		return unsafe.Sizeof(Transfer{})
	case C.TB_OPERATION_LOOKUP_ACCOUNTS:
		fallthrough
	case C.TB_OPERATION_LOOKUP_TRANSFERS:
		return unsafe.Sizeof(Uint128{})
	case C.TB_OPERATION_GET_ACCOUNT_TRANSFERS:
		return unsafe.Sizeof(AccountFilter{})
	case C.TB_OPERATION_GET_ACCOUNT_BALANCES:
		return unsafe.Sizeof(AccountFilter{})
	case C.TB_OPERATION_QUERY_ACCOUNTS:
		return unsafe.Sizeof(QueryFilter{})
	case C.TB_OPERATION_QUERY_TRANSFERS:
		return unsafe.Sizeof(QueryFilter{})
	case C.TB_OPERATION_GET_CHANGE_EVENTS:
		return unsafe.Sizeof(ChangeEventsFilter{})
	default:
		return 0
	}
}

func getResultSize(op C.TB_OPERATION) uintptr {
	switch op {
	case C.TB_OPERATION_CREATE_ACCOUNTS:
		return unsafe.Sizeof(CreateAccountResult{})
	case C.TB_OPERATION_CREATE_TRANSFERS:
		return unsafe.Sizeof(CreateTransferResult{})
	case C.TB_OPERATION_LOOKUP_ACCOUNTS:
		return unsafe.Sizeof(Account{})
	case C.TB_OPERATION_LOOKUP_TRANSFERS:
		return unsafe.Sizeof(Transfer{})
	case C.TB_OPERATION_GET_ACCOUNT_TRANSFERS:
		return unsafe.Sizeof(Transfer{})
	case C.TB_OPERATION_GET_ACCOUNT_BALANCES:
		return unsafe.Sizeof(AccountBalance{})
	case C.TB_OPERATION_QUERY_ACCOUNTS:
		return unsafe.Sizeof(Account{})
	case C.TB_OPERATION_QUERY_TRANSFERS:
		return unsafe.Sizeof(Transfer{})
	case C.TB_OPERATION_GET_CHANGE_EVENTS:
		return unsafe.Sizeof(ChangeEvent{})
	default:
		return 0
	}
}

func (c *c_client) doRequest(
	op C.TB_OPERATION,
	count int,
	data unsafe.Pointer,
) ([]uint8, error) {
	var req request
	req.ready = make(chan []uint8, 1) // buffered chan prevents completion handler blocking for Go.

	// NOTE: packet must be its own allocation and cannot live in request as then CGO is unable to
	// correctly track it (panic: runtime error: cgo argument has Go pointer to unpinned Go pointer)
	packet := new(C.tb_packet_t)
	packet.user_data = unsafe.Pointer(&req)
	packet.user_tag = 0
	packet.operation = C.uint8_t(op)
	packet.data_size = C.uint32_t(count * int(getEventSize(op)))
	packet.data = data

	// NOTE: Pin all go-allocated refs that will be accessed by onGoPacketCompletion after submit().
	var pinner runtime.Pinner
	defer pinner.Unpin()
	pinner.Pin(&req)
	pinner.Pin(packet)
	if data != nil {
		pinner.Pin(data)
	}

	client_status := C.tb_client_submit(c.tb_client, packet)
	if client_status == C.TB_CLIENT_INVALID {
		return nil, ErrClientClosed{}
	}

	// Wait for the request to complete.
	reply := <-req.ready
	packet_status := C.TB_PACKET_STATUS(packet.status)

	// Handle packet error
	if packet_status != C.TB_PACKET_OK {
		switch packet_status {
		case C.TB_PACKET_TOO_MUCH_DATA:
			return nil, ErrMaximumBatchSizeExceeded{}
		case C.TB_PACKET_CLIENT_EVICTED:
			return nil, ErrClientEvicted{}
		case C.TB_PACKET_CLIENT_RELEASE_TOO_LOW:
			return nil, ErrClientReleaseTooLow{}
		case C.TB_PACKET_CLIENT_RELEASE_TOO_HIGH:
			return nil, ErrClientReleaseTooHigh{}
		case C.TB_PACKET_CLIENT_SHUTDOWN:
			return nil, ErrClientClosed{}
		case C.TB_PACKET_INVALID_OPERATION:
			// we control what C.TB_OPERATION is given
			// but allow an invalid opcode to be passed to emulate a client nop
			return nil, ErrInvalidOperation{}
		case C.TB_PACKET_INVALID_DATA_SIZE:
			panic("unreachable") // we control what type of data is given
		default:
			panic("tb_client_submit(): returned packet with invalid status")
		}
	}

	return reply, nil
}

//export onGoPacketCompletion
func onGoPacketCompletion(
	_context C.uintptr_t,
	packet *C.tb_packet_t,
	timestamp C.uint64_t,
	result_ptr C.tb_result_bytes_t,
	result_len C.uint32_t,
) {
	_ = _context
	_ = timestamp

	// Get the request from the packet user data.
	req := (*request)(unsafe.Pointer(packet.user_data))
	var reply []uint8 = nil
	if result_len > 0 && result_ptr != nil {
		op := C.TB_OPERATION(packet.operation)

		// Make sure the completion handler is giving us valid data.
		resultSize := C.uint32_t(getResultSize(op))
		if result_len%resultSize != 0 {
			panic("invalid result_len:  misaligned for the event")
		}

		//TODO(batiati): Refine the way we handle events with asymmetric results.
		if op != C.TB_OPERATION_GET_ACCOUNT_TRANSFERS &&
			op != C.TB_OPERATION_GET_ACCOUNT_BALANCES &&
			op != C.TB_OPERATION_QUERY_ACCOUNTS &&
			op != C.TB_OPERATION_QUERY_TRANSFERS &&
			op != C.TB_OPERATION_GET_CHANGE_EVENTS {
			// Make sure the amount of results at least matches the amount of requests.
			count := packet.data_size / C.uint32_t(getEventSize(op))
			if count*resultSize < result_len {
				panic("invalid result_len: implied multiple results per event")
			}
		}

		// Copy the result data into a new buffer.
		reply = make([]uint8, result_len)
		C.memcpy(unsafe.Pointer(&reply[0]), unsafe.Pointer(result_ptr), C.size_t(result_len))
	}

	// Signal to the goroutine which owns this request that it's ready.
	req.ready <- reply
}

func (c *c_client) CreateAccounts(accounts []Account) ([]CreateAccountResult, error) {
	count := len(accounts)
	var dataPtr unsafe.Pointer
	if count > 0 {
		dataPtr = unsafe.Pointer(&accounts[0])
	} else {
		dataPtr = nil
	}

	reply, err := c.doRequest(
		C.TB_OPERATION_CREATE_ACCOUNTS,
		count,
		dataPtr,
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]CreateAccountResult, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(CreateAccountResult{}))
	results := unsafe.Slice((*CreateAccountResult)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) CreateTransfers(transfers []Transfer) ([]CreateTransferResult, error) {
	count := len(transfers)
	var dataPtr unsafe.Pointer
	if count > 0 {
		dataPtr = unsafe.Pointer(&transfers[0])
	} else {
		dataPtr = nil
	}

	reply, err := c.doRequest(
		C.TB_OPERATION_CREATE_TRANSFERS,
		count,
		dataPtr,
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]CreateTransferResult, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(CreateTransferResult{}))
	results := unsafe.Slice((*CreateTransferResult)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) LookupAccounts(accountIDs []Uint128) ([]Account, error) {
	count := len(accountIDs)
	var dataPtr unsafe.Pointer
	if count > 0 {
		dataPtr = unsafe.Pointer(&accountIDs[0])
	} else {
		dataPtr = nil
	}

	reply, err := c.doRequest(
		C.TB_OPERATION_LOOKUP_ACCOUNTS,
		count,
		dataPtr,
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]Account, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(Account{}))
	results := unsafe.Slice((*Account)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) LookupTransfers(transferIDs []Uint128) ([]Transfer, error) {
	count := len(transferIDs)
	var dataPtr unsafe.Pointer
	if count > 0 {
		dataPtr = unsafe.Pointer(&transferIDs[0])
	} else {
		dataPtr = nil
	}

	reply, err := c.doRequest(
		C.TB_OPERATION_LOOKUP_TRANSFERS,
		count,
		dataPtr,
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]Transfer, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(Transfer{}))
	results := unsafe.Slice((*Transfer)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) GetAccountTransfers(filter AccountFilter) ([]Transfer, error) {
	reply, err := c.doRequest(
		C.TB_OPERATION_GET_ACCOUNT_TRANSFERS,
		1,
		unsafe.Pointer(&filter),
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]Transfer, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(Transfer{}))
	results := unsafe.Slice((*Transfer)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) GetAccountBalances(filter AccountFilter) ([]AccountBalance, error) {
	reply, err := c.doRequest(
		C.TB_OPERATION_GET_ACCOUNT_BALANCES,
		1,
		unsafe.Pointer(&filter),
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]AccountBalance, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(AccountBalance{}))
	results := unsafe.Slice((*AccountBalance)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) QueryAccounts(filter QueryFilter) ([]Account, error) {
	reply, err := c.doRequest(
		C.TB_OPERATION_QUERY_ACCOUNTS,
		1,
		unsafe.Pointer(&filter),
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]Account, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(Account{}))
	results := unsafe.Slice((*Account)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) QueryTransfers(filter QueryFilter) ([]Transfer, error) {
	reply, err := c.doRequest(
		C.TB_OPERATION_QUERY_TRANSFERS,
		1,
		unsafe.Pointer(&filter),
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]Transfer, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(Transfer{}))
	results := unsafe.Slice((*Transfer)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) GetChangeEvents(filter ChangeEventsFilter) ([]ChangeEvent, error) {
	reply, err := c.doRequest(
		C.TB_OPERATION_GET_CHANGE_EVENTS,
		1,
		unsafe.Pointer(&filter),
	)

	if err != nil {
		return nil, err
	}

	if reply == nil {
		return make([]ChangeEvent, 0), nil
	}

	resultsCount := len(reply) / int(unsafe.Sizeof(ChangeEvent{}))
	results := unsafe.Slice((*ChangeEvent)(unsafe.Pointer(&reply[0])), resultsCount)
	return results, nil
}

func (c *c_client) Nop() error {
	const dataSize = 256
	var dummyData [dataSize]C.uint8_t
	ptr := unsafe.Pointer(&dummyData)

	reservedOp := C.TB_OPERATION(0)
	reply, err := c.doRequest(reservedOp, 1, ptr)

	if !e.Is(err, ErrInvalidOperation{}) {
		return err
	}

	_ = reply
	return nil
}
