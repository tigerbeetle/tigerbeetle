package main

/*
#cgo LDFLAGS: -L./ -lclient_c

#include <stdint.h>
#include "./internal/client_c/client.h"

void onResult(uint32_t in); // forward declaration
*/
import "C"
import (
	"errors"
	"fmt"
	"log"
	"unsafe"
)

//export onResult
func onResult(id uint32) {
	fmt.Println("id: ", id)
}

type Client interface {
	// CreateAccounts(batch []uint32)
	Request() error
}

type client struct {
	ctx *C.TB_Context
}

func NewClient() (Client, error) {
	tbCtx := &C.TB_Context{}

	err := C.init(tbCtx)
	if err != 0 {
		return nil, errors.New("Failed to create client.")
	}

	return &client{ctx: tbCtx}, nil
}

func (s *client) Request() error {
	err := C.request(s.ctx, (C.callback_fcn)(unsafe.Pointer(C.onResult)))
	if err != 0 {
		return errors.New("Failed to make request.")
	}

	return nil
}

func main() {
	client, err := NewClient()
	if err != nil {
		log.Fatal(err)
	}

	err = client.Request()
	if err != nil {
		log.Fatal(err)
	}
}
