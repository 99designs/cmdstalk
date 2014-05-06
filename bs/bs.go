/*
	Package bs provides a richer and/or more domain-specific layer over
	github.com/kr/beanstalk, including active-record style Job type.
*/
package bs

import (
	"time"

	"github.com/kr/beanstalk"
)

const (
	// deadlineSoonDelay defines a period to sleep between receiving
	// DEADLINE_SOON in response to reserve, and re-attempting the reserve.
	deadlineSoonDelay = 1 * time.Second
)

// reserve-with-timeout until there's a job or something panic-worthy.
func MustReserveWithoutTimeout(ts *beanstalk.TubeSet) (id uint64, body []byte) {
	var err error
	for {
		id, body, err = ts.Reserve(1 * time.Hour)
		if err == nil {
			return
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrTimeout {
			continue
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrDeadline {
			time.Sleep(deadlineSoonDelay)
			continue
		} else {
			panic(err)
		}
	}
}
