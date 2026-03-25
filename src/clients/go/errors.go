package tigerbeetle_go

import "errors"

var (
	ErrUnexpected           = errors.New("unexpected internal error")
	ErrOutOfMemory          = errors.New("internal client ran out of memory")
	ErrSystemResources      = errors.New("internal client ran out of system resources")
	ErrNetworkSubsystem     = errors.New("internal client had unexpected networking issues")
	ErrAddressLimitExceeded = errors.New("too many addresses provided")
	ErrInvalidAddress       = errors.New("invalid client cluster address")
	ErrClientEvicted        = errors.New("client was evicted")
	ErrClientReleaseTooLow  = errors.New("client was evicted: release too old")
	ErrClientReleaseTooHigh = errors.New("Client was evicted; release too new")
	ErrClientClosed         = errors.New("client was closed")
	ErrInvalidOperation     = errors.New("internal operation provided was invalid")
	ErrTooMuchData          = errors.New("maximum batch size exceeded")
)
