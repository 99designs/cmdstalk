package bs

import (
	"fmt"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
)

// Job represents a beanstalkd job, and holds a reference to the connection so
// that server actions can be taken as methods on the job.
type Job struct {

	// The numeric beanstalkd-assigned job ID.
	Id uint64

	// The job payload data.
	Body []byte

	conn *beanstalk.Conn
}

// Create a Job instance.
func NewJob(id uint64, body []byte, conn *beanstalk.Conn) Job {
	return Job{
		Body: body,
		Id:   id,
		conn: conn,
	}
}

// Bury the job, with its original priority.
func (j Job) Bury() error {
	pri, err := j.Priority()
	if err != nil {
		return err
	}
	return j.conn.Bury(j.Id, pri)
}

// Delete the job.
func (j Job) Delete() error {
	return j.conn.Delete(j.Id)
}

// Kicks counts how many times the job has been kicked from a buried state.
func (j Job) Kicks() (uint64, error) {
	return j.uint64Stat("kicks")
}

// Priority of the job, zero is most urgent, 4,294,967,295 is least.
func (j Job) Priority() (uint32, error) {
	pri64, err := j.uint64Stat("pri")
	return uint32(pri64), err
}

// Release the job, with its original priority and no delay.
func (j Job) Release(delay time.Duration) error {
	pri, err := j.Priority()
	if err != nil {
		return err
	}
	return j.conn.Release(j.Id, pri, delay)
}

// Releases counts how many times the job has been released back to the tube.
func (j Job) Releases() (uint64, error) {
	return j.uint64Stat("releases")
}

func (j Job) String() string {
	stats, err := j.conn.StatsJob(j.Id)
	if err == nil {
		return fmt.Sprintf("Job %d %#v", j.Id, stats)
	} else {
		return fmt.Sprintf("Job %d (stats-job failed: %s)", j.Id, err)
	}
}

// TimeLeft as reported by beanstalkd, as a time.Duration.
// beanstalkd reports as int(seconds), which defines the (low) precision.
// Less than 1.0 seconds remaining will be reported as zero.
func (j Job) TimeLeft() (time.Duration, error) {
	stats, err := j.conn.StatsJob(j.Id)
	if err != nil {
		return 0, err
	}
	return time.ParseDuration(stats["time-left"] + "s")
}

// Timeouts counts how many times the job has been reserved and reached TTR.
func (j Job) Timeouts() (uint64, error) {
	return j.uint64Stat("timeouts")
}

func (j Job) uint64Stat(key string) (uint64, error) {
	stats, err := j.conn.StatsJob(j.Id)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(stats[key], 10, 64)
}
