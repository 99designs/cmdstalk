package broker

import (
	"time"

	"github.com/kr/beanstalk"
)

const (
	// ListTubeDelay is the time between sending list-tube to beanstalkd
	// to discover and watch newly created tubes.
	ListTubeDelay = 10 * time.Second
)

type BrokerDispatcher struct {
	address string
	cmd     string
	conn    *beanstalk.Conn
	tubeSet map[string]bool
}

func NewBrokerDispatcher(address, cmd string) *BrokerDispatcher {
	return &BrokerDispatcher{
		address: address,
		cmd:     cmd,
		tubeSet: make(map[string]bool),
	}
}

// RunTube runs a broker for the specified tube.
func (bd *BrokerDispatcher) RunTube(tube string) {
	bd.tubeSet[tube] = true
	go func() {
		b := New(bd.address, tube, bd.cmd, nil)
		b.Run(nil)
	}()
}

// RunTube runs a broker for the specified tubes.
func (bd *BrokerDispatcher) RunTubes(tubes []string) {
	for _, tube := range tubes {
		bd.RunTube(tube)
	}
}

// RunAllTubes polls beanstalkd, running a broker as new tubes are created.
func (bd *BrokerDispatcher) RunAllTubes() (err error) {
	conn, err := beanstalk.Dial("tcp", bd.address)
	if err == nil {
		bd.conn = conn
	} else {
		return
	}

	go func() {
		ticker := time.Tick(ListTubeDelay)
		for _ = range ticker {
			if e := bd.watchNewTubes(); e != nil {
				// ignore error
			}
		}
	}()

	return
}

func (bd *BrokerDispatcher) watchNewTubes() (err error) {
	tubes, err := bd.conn.ListTubes()
	if err != nil {
		return
	}

	for _, tube := range tubes {
		if !bd.tubeSet[tube] {
			bd.RunTube(tube)
		}
	}

	return
}
