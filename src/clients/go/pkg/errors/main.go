package errors

type ErrUnexpected struct{}

func (s ErrUnexpected) Error() string { return "Unexpected internal error." }

type ErrOutOfMemory struct{}

func (s ErrOutOfMemory) Error() string { return "Internal client ran out of memory." }

type ErrSystemResources struct{}

func (s ErrSystemResources) Error() string { return "Internal client ran out of system resources." }

type ErrNetworkSubsystem struct{}

func (s ErrNetworkSubsystem) Error() string {
	return "Internal client had unexpected networking issues."
}

type ErrInvalidConcurrencyMax struct{}

func (s ErrInvalidConcurrencyMax) Error() string { return "Concurrency max is out of range." }

type ErrAddressLimitExceeded struct{}

func (s ErrAddressLimitExceeded) Error() string { return "Too many addresses provided." }

type ErrInvalidAddress struct{}

func (s ErrInvalidAddress) Error() string { return "Invalid client cluster address." }

type ErrClientEvicted struct{}

func (s ErrClientEvicted) Error() string { return "Client was evicted." }

type ErrClientReleaseTooLow struct{}

func (s ErrClientReleaseTooLow) Error() string { return "Client was evicted: release too old." }

type ErrClientReleaseTooHigh struct{}

func (s ErrClientReleaseTooHigh) Error() string { return "Client was evicted; release too new." }

type ErrClientClosed struct{}

func (s ErrClientClosed) Error() string { return "Client was closed." }

type ErrInvalidOperation struct{}

func (s ErrInvalidOperation) Error() string { return "internal operation provided was invalid." }

type ErrMaximumBatchSizeExceeded struct{}

func (s ErrMaximumBatchSizeExceeded) Error() string { return "Maximum batch size exceeded." }
