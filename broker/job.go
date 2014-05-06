package broker

import (
	"fmt"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
)

type job struct {
	conn *beanstalk.Conn
	body []byte
	id   uint64
}

func (j job) bury() error {
	pri, err := j.priority()
	if err != nil {
		return err
	}
	return j.conn.Bury(j.id, pri)
}

func (j job) delete() error {
	return j.conn.Delete(j.id)
}

func (j job) priority() (uint32, error) {
	pri64, err := j.uint64Stat("pri")
	return uint32(pri64), err
}

func (j job) release() error {
	pri, err := j.priority()
	if err != nil {
		return err
	}
	return j.conn.Release(j.id, pri, 0)
}

func (j job) String() string {
	stats, err := j.conn.StatsJob(j.id)
	if err == nil {
		return fmt.Sprintf("Job %d %#v", j.id, stats)
	} else {
		return fmt.Sprintf("Job %d (stats-job failed: %s)", j.id, err)
	}
}

// time-left as reported by beanstalkd; floor(seconds)
func (j job) timeLeft() (time.Duration, error) {
	stats, err := j.conn.StatsJob(j.id)
	if err != nil {
		return 0, err
	}
	return time.ParseDuration(stats["time-left"] + "s")
}

func (j job) timeouts() (uint64, error) {
	return j.uint64Stat("timeouts")
}

func (j job) uint64Stat(key string) (uint64, error) {
	stats, err := j.conn.StatsJob(j.id)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(stats[key], 10, 64)
}
