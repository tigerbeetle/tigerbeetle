package tigerbeetle_go

/*
#cgo LDFLAGS: -L./ -lclient_c

#include <stdint.h>
#include <stdlib.h>
#include "./internal/client_c/client.h"

void onResult(void* const user_data, uint8_t error, uint8_t operation, uint8_t* const results, const uint32_t results_length);
*/
import "C"
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	tberrors "github.com/coilhq/tigerbeetle_go/pkg/errors"
	"github.com/coilhq/tigerbeetle_go/pkg/types"
)

// Go may only pass a pointer to C provided that it does not point to memory that
// contains Go pointers. https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers.
// We therefore keep a registry of user data here that can be accessed when we
// receive a response from the C client.
var userDataMap map[uint32]*UserData = make(map[uint32]*UserData)
var userDataIndex uint32 = 0

// We acquire a lock when accessing the userDataMap.
var mutex sync.Mutex

//export onResult
func onResult(user_data_raw *C.void, error uint8, operation uint8, results *uint8, results_length uint32) {
	index := *(*uint32)((unsafe.Pointer)(user_data_raw))
	userData := getUserData(index)
	cb := userData.Callback
	ctx := userData.Ctx

	// copy results as memory lifetime is not guaranteed.
	result_slice := unsafe.Slice(results, results_length)
	ret := make([]byte, results_length)
	r := bytes.NewReader(result_slice)
	err := binary.Read(r, binary.LittleEndian, &ret)
	if err != nil {
		fmt.Println("onResult: binary.Read failed:", err)
	}

	cb(ctx, err, types.Operation(operation), ret)
}

type UserData struct {
	Ctx      interface{}
	Callback ResultsCallback
}

type ResultsCallback func(ctx interface{}, err error, operation types.Operation, results []byte)
type Client interface {
	CreateAccounts(batch []types.Account) ([]types.EventResult, error)
	LookupAccounts(batch []types.Uint128) ([]types.Account, error)
	CreateTransfers(batch []types.Transfer) ([]types.EventResult, error)
	CommitTransfers(batch []types.Commit) ([]types.EventResult, error)
	LookupTransfers(batch []types.Uint128) ([]types.Transfer, error)
	Request(userCtx interface{}, callback ResultsCallback, operation types.Operation, batch []byte) error
	Tick()
	Deinit()
}

type client struct {
	ctx *C.TB_Context
}

func NewClient(clusterID uint32, addresses []string) (Client, error) {
	tbCtx := &C.TB_Context{}

	addresses_raw := strings.Join(addresses[:], ",")
	cstring := C.CString(addresses_raw)
	defer C.free(unsafe.Pointer(cstring))

	err := C.init(tbCtx, C.uint32_t(clusterID), cstring, C.uint32_t(len(addresses_raw)))
	if err != 0 {
		return nil, errors.New("Failed to create client.")
	}

	return &client{ctx: tbCtx}, nil
}

// Raw request method based on callbacks for flexibility.
func (s *client) Request(userCtx interface{}, callback ResultsCallback, operation types.Operation, batch []byte) error {
	userData := &UserData{
		Ctx:      userCtx,
		Callback: callback,
	}
	index := storeUserData(userData)

	cErr := C.request(
		s.ctx,
		unsafe.Pointer(&index),
		(C.results_callback)(C.onResult),
		(C.uchar)(operation),
		(*C.uchar)(&batch[0]),
		(C.uint32_t)(len(batch)),
	)
	if cErr != 0 {
		return tberrors.ErrorCast(int(cErr), "Failed to create accounts")
	}

	return nil
}

func (s *client) Tick() {
	C.tick(s.ctx)
}

func (s *client) Deinit() {
	C.deinit(s.ctx)
}

func storeUserData(userData *UserData) uint32 {
	mutex.Lock()
	defer mutex.Unlock()

	userDataIndex++
	userDataMap[userDataIndex] = userData

	return userDataIndex
}

func getUserData(index uint32) *UserData {
	mutex.Lock()
	defer mutex.Unlock()

	userData, found := userDataMap[index]
	if !found {
		return nil
	}

	delete(userDataMap, index)

	return userData
}

// From here on are wrappers that provide typings and use channels to block until the reponse is received from the
// server.

type resultsOrError struct {
	results []types.EventResult
	err     error
}

func onCreateAccounts(ctx interface{}, err error, operation types.Operation, results []byte) {
	responseChannel := ctx.(chan resultsOrError)
	defer close(responseChannel)

	if operation != types.CREATE_ACCOUNT || len(results)%8 != 0 {
		responseChannel <- resultsOrError{
			results: nil,
			err:     errors.New("Did not receive a create account result."),
		}
		return
	}

	// TODO: convert to EventResults without copying
	events := len(results) / 8 // event result { index: u32, code: u32 }
	ret := make([]types.EventResult, events)
	r := bytes.NewReader(results)
	err = binary.Read(r, binary.LittleEndian, &ret)
	if err != nil {
		responseChannel <- resultsOrError{
			results: nil,
			err:     err,
		}
		return
	}

	responseChannel <- resultsOrError{
		results: ret,
		err:     nil,
	}
}

func (s *client) CreateAccounts(batch []types.Account) ([]types.EventResult, error) {
	// TODO: convert to []byte without copying
	buf := new(bytes.Buffer)
	for _, account := range batch {
		err := binary.Write(buf, binary.LittleEndian, account)
		if err != nil {
			return nil, err
		}
	}

	responseChannel := make(chan resultsOrError)

	err := s.Request(responseChannel, onCreateAccounts, types.CREATE_ACCOUNT, buf.Bytes())
	if err != nil {
		return nil, err
	}

	for {
		select {
		case res := <-responseChannel:
			if res.err != nil {
				return nil, res.err
			}
			return res.results, nil
		}
	}
}

type accountLookupsOrError struct {
	accounts []types.Account
	err      error
}

func onAccountLookupResults(ctx interface{}, err error, operation types.Operation, results []byte) {
	responseChannel := ctx.(chan accountLookupsOrError)
	defer close(responseChannel)

	if operation != types.ACCOUNT_LOOKUP || len(results)%128 != 0 {
		responseChannel <- accountLookupsOrError{
			accounts: nil,
			err:      errors.New("Did not receive account lookup results."),
		}
		return
	}

	// TODO: convert to Account without copying
	accounts := make([]types.Account, len(results)/128)
	r := bytes.NewReader(results)
	err = binary.Read(r, binary.LittleEndian, &accounts)
	if err != nil {
		responseChannel <- accountLookupsOrError{
			accounts: nil,
			err:      err,
		}
		return
	}

	responseChannel <- accountLookupsOrError{
		accounts: accounts,
		err:      nil,
	}
}

func (s *client) LookupAccounts(batch []types.Uint128) ([]types.Account, error) {
	// TODO: convert to []byte without copying
	buf := new(bytes.Buffer)
	for _, accountID := range batch {
		err := binary.Write(buf, binary.LittleEndian, accountID)
		if err != nil {
			return nil, err
		}
	}

	responseChannel := make(chan accountLookupsOrError)

	err := s.Request(responseChannel, onAccountLookupResults, types.ACCOUNT_LOOKUP, buf.Bytes())
	if err != nil {
		return nil, err
	}

	for {
		select {
		case res := <-responseChannel:
			if res.err != nil {
				return nil, res.err
			}
			return res.accounts, nil
		}
	}
}

func onCreateTransfer(ctx interface{}, err error, operation types.Operation, results []byte) {
	responseChannel := ctx.(chan resultsOrError)
	defer close(responseChannel)

	if operation != types.CREATE_TRANSFER || len(results)%8 != 0 {
		responseChannel <- resultsOrError{
			results: nil,
			err:     errors.New("Did not receive a create transfer result."),
		}
		return
	}

	// TODO: convert to EventResults without copying
	events := len(results) / 8 // event result { index: u32, code: u32 }
	ret := make([]types.EventResult, events)
	r := bytes.NewReader(results)
	err = binary.Read(r, binary.LittleEndian, &ret)
	if err != nil {
		responseChannel <- resultsOrError{
			results: nil,
			err:     err,
		}
		return
	}

	responseChannel <- resultsOrError{
		results: ret,
		err:     nil,
	}
}

func (s *client) CreateTransfers(batch []types.Transfer) ([]types.EventResult, error) {
	// TODO: convert to []byte without copying
	buf := new(bytes.Buffer)
	for _, transfer := range batch {
		err := binary.Write(buf, binary.LittleEndian, transfer)
		if err != nil {
			return nil, err
		}
	}

	responseChannel := make(chan resultsOrError)

	err := s.Request(responseChannel, onCreateTransfer, types.CREATE_TRANSFER, buf.Bytes())
	if err != nil {
		return nil, err
	}

	for {
		select {
		case res := <-responseChannel:
			if res.err != nil {
				return nil, res.err
			}
			return res.results, nil
		}
	}
}

func onCommitTransfer(ctx interface{}, err error, operation types.Operation, results []byte) {
	responseChannel := ctx.(chan resultsOrError)
	defer close(responseChannel)

	if operation != types.COMMIT_TRANSFER || len(results)%8 != 0 {
		responseChannel <- resultsOrError{
			results: nil,
			err:     errors.New("Did not receive a commit transfer result."),
		}
		return
	}

	// TODO: convert to EventResults without copying
	events := len(results) / 8 // event result { index: u32, code: u32 }
	ret := make([]types.EventResult, events)
	r := bytes.NewReader(results)
	err = binary.Read(r, binary.LittleEndian, &ret)
	if err != nil {
		responseChannel <- resultsOrError{
			results: nil,
			err:     err,
		}
		return
	}

	responseChannel <- resultsOrError{
		results: ret,
		err:     nil,
	}
}

func (s *client) CommitTransfers(batch []types.Commit) ([]types.EventResult, error) {
	// TODO: convert to []byte without copying
	buf := new(bytes.Buffer)
	for _, commit := range batch {
		err := binary.Write(buf, binary.LittleEndian, commit)
		if err != nil {
			return nil, err
		}
	}

	responseChannel := make(chan resultsOrError)

	err := s.Request(responseChannel, onCommitTransfer, types.COMMIT_TRANSFER, buf.Bytes())
	if err != nil {
		return nil, err
	}

	for {
		select {
		case res := <-responseChannel:
			if res.err != nil {
				return nil, res.err
			}
			return res.results, nil
		}
	}
}

type transferLookupsOrError struct {
	transfers []types.Transfer
	err       error
}

func onTransferLookup(ctx interface{}, err error, operation types.Operation, results []byte) {
	responseChannel := ctx.(chan transferLookupsOrError)
	defer close(responseChannel)

	if operation != types.TRANSFER_LOOKUP || len(results)%128 != 0 {
		responseChannel <- transferLookupsOrError{
			transfers: nil,
			err:       errors.New("Did not receive transfer lookup results."),
		}
		return
	}

	// TODO: convert to Account without copying
	transfers := make([]types.Transfer, len(results)/128)
	r := bytes.NewReader(results)
	err = binary.Read(r, binary.LittleEndian, &transfers)
	if err != nil {
		responseChannel <- transferLookupsOrError{
			transfers: nil,
			err:       err,
		}
		return
	}

	responseChannel <- transferLookupsOrError{
		transfers: transfers,
		err:       nil,
	}
}

func (s *client) LookupTransfers(batch []types.Uint128) ([]types.Transfer, error) {
	// TODO: convert to []byte without copying
	buf := new(bytes.Buffer)
	for _, transferID := range batch {
		err := binary.Write(buf, binary.LittleEndian, transferID)
		if err != nil {
			return nil, err
		}
	}

	responseChannel := make(chan transferLookupsOrError)

	err := s.Request(responseChannel, onTransferLookup, types.TRANSFER_LOOKUP, buf.Bytes())
	if err != nil {
		return nil, err
	}

	for {
		select {
		case res := <-responseChannel:
			if res.err != nil {
				return nil, res.err
			}
			return res.transfers, nil
		}
	}
}
