package processor

import "errors"

// nonRetryableError wraps an error indicating the message is permanently
// invalid (e.g., malformed JSON, missing required fields). The consumer
// should skip retries and route the message directly to the DLQ.
type nonRetryableError struct{ err error }

func (e *nonRetryableError) Error() string { return e.err.Error() }
func (e *nonRetryableError) Unwrap() error { return e.err }

// NonRetryable marks err as a permanent message-level failure that should
// bypass the retry loop. On the primary consumer the message is routed
// to the DLQ immediately; on the DLQ consumer (no DLQ) the error
// propagates to errCh and stops the consumer.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

// IsNonRetryable reports whether any error in err's chain is non-retryable.
func IsNonRetryable(err error) bool {
	var target *nonRetryableError
	return errors.As(err, &target)
}

// fatalError wraps an error indicating a systemic failure that requires
// the consumer to shut down (e.g., authentication failure, schema
// mismatch). The message must NOT be sent to the DLQ.
type fatalError struct{ err error }

func (e *fatalError) Error() string { return e.err.Error() }
func (e *fatalError) Unwrap() error { return e.err }

// Fatal marks err as a systemic failure requiring consumer shutdown.
// Fatal errors always bypass retries and DLQ — the consumer stops.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	return &fatalError{err: err}
}

// IsFatal reports whether any error in err's chain is fatal.
func IsFatal(err error) bool {
	var target *fatalError
	return errors.As(err, &target)
}
