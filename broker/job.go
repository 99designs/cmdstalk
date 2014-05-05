package broker

import (
	"strconv"

	"github.com/kr/beanstalk"
)

type job struct {
	conn *beanstalk.Conn
	body []byte
	id   uint64
}

func (j job) priority() (uint32, error) {

	stats, err := j.conn.StatsJob(j.id)
	if err != nil {
		return 0, err
	}

	pri64, err := strconv.ParseUint(stats["pri"], 10, 32)

	return uint32(pri64), nil
}

func (j job) release() error {
	pri, err := j.priority()
	if err != nil {
		return err
	}
	err = j.conn.Release(j.id, pri, 0)
	if err != nil {
		return err
	}
	return nil
}
