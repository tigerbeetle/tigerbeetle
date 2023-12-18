package tigerbeetle_go

/*
#cgo CFLAGS: -g -Wall
#cgo darwin,arm64 LDFLAGS: ${SRCDIR}/pkg/native/aarch64-macos/libtb_client.a -ldl -lm
#cgo darwin,amd64 LDFLAGS: ${SRCDIR}/pkg/native/x86_64-macos/libtb_client.a -ldl -lm
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/pkg/native/aarch64-linux/libtb_client.a -ldl -lm
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/pkg/native/x86_64-linux/libtb_client.a -ldl -lm
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/pkg/native/x86_64-windows -ltb_client -lws2_32 -lntdll

#include <stdlib.h>
#include <string.h>
#include "./pkg/native/tb_client.h"

#ifndef __declspec
	#define __declspec(x)
#endif

typedef const uint8_t* tb_result_bytes_t;

extern __declspec(dllexport) void onGoPacketCompletion(
	uintptr_t ctx,
	tb_client_t client,
	tb_packet_t* packet,
	tb_result_bytes_t result_ptr,
	uint32_t result_len
);
*/
import "C"
import (
	e "errors"
	"strings"
	"unsafe"

	"github.com/tigerbeetle/tigerbeetle-go/pkg/errors"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

///////////////////////////////////////////////////////////////

type Client interface {
	CreateAccounts(accounts []types.Account) ([]types.AccountEventResult, error)
	CreateTransfers(transfers []types.Transfer) ([]types.TransferEventResult, error)
	LookupAccounts(accountIDs []types.Uint128) ([]types.Account, error)
	LookupTransfers(transferIDs []types.Uint128) ([]types.Transfer, error)
	GetAccountTransfers(filter types.GetAccountTransfers) ([]types.Transfer, error)

	Nop() error
	Close()
}

type request struct {
	packet *C.tb_packet_t
	result unsafe.Pointer
	ready  chan struct{}
}

type c_client struct {
	tb_client C.tb_client_t
}

func NewClient(
	clusterID types.Uint128,
	addresses []string,
	concurrencyMax uint,
) (Client, error) {
	// Allocate a cstring of the addresses joined with ",".
	addresses_raw := strings.Join(addresses[:], ",")
	c_addresses := C.CString(addresses_raw)
	defer C.free(unsafe.Pointer(c_addresses))

	var tb_client C.tb_client_t

	// Create the tb_client.
	status := C.tb_client_init(
		&tb_client,
		C.tb_uint128_t(clusterID),
		c_addresses,
		C.uint32_t(len(addresses_raw)),
		C.uint32_t(concurrencyMax),
		C.uintptr_t(0), // on_completion_ctx
		(*[0]byte)(C.onGoPacketCompletion),
	)

	if status != C.TB_STATUS_SUCCESS {
		switch status {
		case C.TB_STATUS_UNEXPECTED:
			return nil, errors.ErrUnexpected{}
		case C.TB_STATUS_OUT_OF_MEMORY:
			return nil, errors.ErrOutOfMemory{}
		case C.TB_STATUS_ADDRESS_INVALID:
			return nil, errors.ErrInvalidAddress{}
		case C.TB_STATUS_ADDRESS_LIMIT_EXCEEDED:
			return nil, errors.ErrAddressLimitExceeded{}
		case C.TB_STATUS_CONCURRENCY_MAX_INVALID:
			return nil, errors.ErrInvalidConcurrencyMax{}
		case C.TB_STATUS_SYSTEM_RESOURCES:
			return nil, errors.ErrSystemResources{}
		case C.TB_STATUS_NETWORK_SUBSYSTEM:
			return nil, errors.ErrNetworkSubsystem{}
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
	if c.tb_client != nil {
		C.tb_client_deinit(c.tb_client)
		c.tb_client = nil
	}
}

func getEventSize(op C.TB_OPERATION) uintptr {
	switch op {
	case C.TB_OPERATION_CREATE_ACCOUNTS:
		return unsafe.Sizeof(types.Account{})
	case C.TB_OPERATION_CREATE_TRANSFERS:
		return unsafe.Sizeof(types.Transfer{})
	case C.TB_OPERATION_LOOKUP_ACCOUNTS:
		fallthrough
	case C.TB_OPERATION_LOOKUP_TRANSFERS:
		return unsafe.Sizeof(types.Uint128{})
	case C.TB_OPERATION_GET_ACCOUNT_TRANSFERS:
		return unsafe.Sizeof(types.GetAccountTransfers{})
	default:
		return 0
	}
}

func getResultSize(op C.TB_OPERATION) uintptr {
	switch op {
	case C.TB_OPERATION_CREATE_ACCOUNTS:
		return unsafe.Sizeof(types.AccountEventResult{})
	case C.TB_OPERATION_CREATE_TRANSFERS:
		return unsafe.Sizeof(types.TransferEventResult{})
	case C.TB_OPERATION_LOOKUP_ACCOUNTS:
		return unsafe.Sizeof(types.Account{})
	case C.TB_OPERATION_LOOKUP_TRANSFERS:
		return unsafe.Sizeof(types.Transfer{})
	case C.TB_OPERATION_GET_ACCOUNT_TRANSFERS:
		return unsafe.Sizeof(types.Transfer{})
	default:
		return 0
	}
}

func (c *c_client) doRequest(
	op C.TB_OPERATION,
	count int,
	data unsafe.Pointer,
	result unsafe.Pointer,
) (int, error) {
	if count == 0 {
		return 0, errors.ErrEmptyBatch{}
	}

	if c.tb_client == nil {
		return 0, errors.ErrClientClosed{}
	}

	req := request{
		packet: nil,
		ready:  make(chan struct{}),
	}

	switch acquire_status := C.tb_client_acquire_packet(c.tb_client, &req.packet); acquire_status {
	case C.TB_PACKET_ACQUIRE_CONCURRENCY_MAX_EXCEEDED:
		return 0, errors.ErrConcurrencyExceeded{}
	case C.TB_PACKET_ACQUIRE_SHUTDOWN:
		return 0, errors.ErrClientClosed{}
	default:
		if req.packet == nil {
			panic("tb_client_acquire_packet(): returned null packet")
		}
	}

	// Release the packet for other goroutines to use.
	defer C.tb_client_release_packet(c.tb_client, req.packet)

	req.packet.user_data = unsafe.Pointer(&req)
	req.packet.operation = C.uint8_t(op)
	req.packet.status = C.TB_PACKET_OK
	req.packet.data_size = C.uint32_t(count * int(getEventSize(op)))
	req.packet.data = data

	// Set where to write the result bytes.
	req.result = result

	// Submit the request.
	C.tb_client_submit(c.tb_client, req.packet)

	// Wait for the request to complete.
	<-req.ready
	status := C.TB_PACKET_STATUS(req.packet.status)
	wrote := int(req.packet.data_size)

	// Handle packet error
	if status != C.TB_PACKET_OK {
		switch status {
		case C.TB_PACKET_TOO_MUCH_DATA:
			return 0, errors.ErrMaximumBatchSizeExceeded{}
		case C.TB_PACKET_INVALID_OPERATION:
			// we control what C.TB_OPERATION is given
			// but allow an invalid opcode to be passed to emulate a client nop
			return 0, errors.ErrInvalidOperation{}
		case C.TB_PACKET_INVALID_DATA_SIZE:
			panic("unreachable") // we control what type of data is given
		default:
			panic("tb_client_submit(): returned packet with invalid status")
		}
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
	// Get the request from the packet user data.
	req := (*request)(unsafe.Pointer(packet.user_data))
	if req.packet != packet {
		panic("invalid packet: request packet mismatch")
	}

	var wrote C.uint32_t
	if result_len > 0 && result_ptr != nil {
		op := C.TB_OPERATION(packet.operation)

		// Make sure the completion handler is giving us valid data.
		resultSize := C.uint32_t(getResultSize(op))
		if result_len%resultSize != 0 {
			panic("invalid result_len:  misaligned for the event")
		}

		//TODO(batiati): Refine the way we handle events with asymmetric results.
		if op != C.TB_OPERATION_GET_ACCOUNT_TRANSFERS {
			// Make sure the amount of results at least matches the amount of requests.
			count := packet.data_size / C.uint32_t(getEventSize(op))
			if count*resultSize < result_len {
				panic("invalid result_len: implied multiple results per event")
			}
		}

		// Write the result data into the request's result.
		if req.result != nil {
			wrote = result_len
			C.memcpy(req.result, unsafe.Pointer(result_ptr), C.size_t(result_len))
		}
	}

	// Signal to the goroutine which owns this request that it's ready.
	req.packet.data_size = wrote
	req.ready <- struct{}{}
}

func (c *c_client) CreateAccounts(accounts []types.Account) ([]types.AccountEventResult, error) {
	count := len(accounts)
	results := make([]types.AccountEventResult, count)
	wrote, err := c.doRequest(
		C.TB_OPERATION_CREATE_ACCOUNTS,
		count,
		unsafe.Pointer(&accounts[0]),
		unsafe.Pointer(&results[0]),
	)

	if err != nil {
		return nil, err
	}

	resultCount := wrote / int(unsafe.Sizeof(types.TransferEventResult{}))
	return results[0:resultCount], nil
}

func (c *c_client) CreateTransfers(transfers []types.Transfer) ([]types.TransferEventResult, error) {
	count := len(transfers)
	results := make([]types.TransferEventResult, count)
	wrote, err := c.doRequest(
		C.TB_OPERATION_CREATE_TRANSFERS,
		count,
		unsafe.Pointer(&transfers[0]),
		unsafe.Pointer(&results[0]),
	)

	if err != nil {
		return nil, err
	}

	resultCount := wrote / int(unsafe.Sizeof(types.TransferEventResult{}))
	return results[0:resultCount], nil
}

func (c *c_client) LookupAccounts(accountIDs []types.Uint128) ([]types.Account, error) {
	count := len(accountIDs)
	results := make([]types.Account, count)
	wrote, err := c.doRequest(
		C.TB_OPERATION_LOOKUP_ACCOUNTS,
		count,
		unsafe.Pointer(&accountIDs[0]),
		unsafe.Pointer(&results[0]),
	)

	if err != nil {
		return nil, err
	}

	resultCount := wrote / int(unsafe.Sizeof(types.Account{}))
	return results[0:resultCount], nil
}

func (c *c_client) LookupTransfers(transferIDs []types.Uint128) ([]types.Transfer, error) {
	count := len(transferIDs)
	results := make([]types.Transfer, count)
	wrote, err := c.doRequest(
		C.TB_OPERATION_LOOKUP_TRANSFERS,
		count,
		unsafe.Pointer(&transferIDs[0]),
		unsafe.Pointer(&results[0]),
	)

	if err != nil {
		return nil, err
	}

	resultCount := wrote / int(unsafe.Sizeof(types.Transfer{}))
	return results[0:resultCount], nil
}

func (c *c_client) GetAccountTransfers(filter types.GetAccountTransfers) ([]types.Transfer, error) {
	//TODO(batiati): we need to expose the max message size to the client.
	//since queries have asymmetric events and results, we can't allocate
	//the results array based on the number of events.
	results := make([]types.Transfer, 8190)

	wrote, err := c.doRequest(
		C.TB_OPERATION_GET_ACCOUNT_TRANSFERS,
		1,
		unsafe.Pointer(&filter),
		unsafe.Pointer(&results[0]),
	)

	if err != nil {
		return nil, err
	}

	resultCount := wrote / int(unsafe.Sizeof(types.Transfer{}))
	return results[0:resultCount], nil
}

func (c *c_client) Nop() error {
	const dataSize = 256
	var dummyData [dataSize]C.uint8_t
	ptr := unsafe.Pointer(&dummyData)

	reservedOp := C.TB_OPERATION(0)
	wrote, err := c.doRequest(reservedOp, 1, ptr, ptr)

	if !e.Is(err, errors.ErrInvalidOperation{}) {
		return err
	}

	_ = wrote
	return nil
}
