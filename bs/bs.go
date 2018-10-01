/*
	Package bs provides a richer and/or more domain-specific layer over
	github.com/kr/beanstalk, including active-record style Job type.
*/
package bs

import (
	"errors"
	"time"

	"github.com/kr/beanstalk"
)

const (
	// deadlineSoonDelay defines a period to sleep between receiving
	// DEADLINE_SOON in response to reserve, and re-attempting the reserve.
	DeadlineSoonDelay = 1 * time.Second
)

var (
	ErrTimeout = errors.New("timeout for reserving a job")
)

// reserve-with-timeout until there's a job or something panic-worthy.
// Handles beanstalk.ErrTimeout by retrying immediately.
// Handles beanstalk.ErrDeadline by sleeping DeadlineSoonDelay before retry.
// panics for other errors.
func MustReserveWithTimeout(ts *beanstalk.TubeSet, timeout time.Duration) (id uint64, body []byte, err error) {
	for {
		id, body, err = ts.Reserve(timeout)
		if err == nil {
			return
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrTimeout {
			err = ErrTimeout
			return
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrDeadline {
			time.Sleep(DeadlineSoonDelay)
			continue
		} else {
			panic(err)
		}
	}
}
