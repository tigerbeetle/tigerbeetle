package errors

type ErrUnexpected struct {}

func (s ErrUnexpected) Error() string { return "Unexpected internal error." }

type ErrOutOfMemory struct {}

func (s ErrOutOfMemory) Error() string { return "Internal client ran out of memory." }

type ErrSystemResources struct {}

func (s ErrSystemResources) Error() string { return "Internal client ran out of system resources." }

type ErrNetworkSubsystem struct {}

func (s ErrNetworkSubsystem) Error() string { return "Internal client had unexpected networking issues." }

type ErrInvalidAddress struct {}

func (s ErrInvalidAddress) Error() string { return "Invalid client cluster address." }

type ErrClientClosed struct {}

func (s ErrClientClosed) Error() string { return "Client was closed." }

type ErrEmptyBatch struct{}

func (s ErrEmptyBatch) Error() string { return "Empty batch." }

type ErrMaximumBatchSizeExceeded struct{}

func (s ErrMaximumBatchSizeExceeded) Error() string { return "Maximum batch size exceeded." }
