package errors

import "errors"

type ErrInvalidOperation struct{}

func (s ErrInvalidOperation) Error() string { return "Invalid operation." }

type ErrInvalidBatchSize struct{}

func (s ErrInvalidBatchSize) Error() string { return "Invalid batch size." }

type ErrTooManyOutstandingRequests struct{}

func (s ErrTooManyOutstandingRequests) Error() string { return "Too many outstanding requests." }

type ErrEmptyBatch struct{}

func (s ErrEmptyBatch) Error() string { return "Empty batch." }

type ErrMaximumBatchSizeExceeded struct{}

func (s ErrMaximumBatchSizeExceeded) Error() string { return "Maximum batch size exceeded." }

type ErrFailedToTickIO struct{}

func (s ErrFailedToTickIO) Error() string { return "Failed to tick IO." }

func ErrorCast(error int, defaultMessage string) error {
	switch error {
	case 2:
		return ErrInvalidOperation{}
	case 3:
		return ErrTooManyOutstandingRequests{}
	case 4:
		return ErrEmptyBatch{}
	case 5:
		return ErrMaximumBatchSizeExceeded{}
	case 6:
		return ErrInvalidBatchSize{}
	case 7:
		return ErrFailedToTickIO{}
	default:
		return errors.New(defaultMessage)
	}
}
