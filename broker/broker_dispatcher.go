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

// BrokerDispatcher manages the running of Broker instances for tubes.  It can
// be manually told tubes to start, or it can poll for tubes as they are
// created. The `perTube` option determines how many brokers are started for
// each tube.
type BrokerDispatcher struct {
	address string
	cmd     string
	conn    *beanstalk.Conn
	perTube uint64
	tubeSet map[string]bool
}

func NewBrokerDispatcher(address, cmd string, perTube uint64) *BrokerDispatcher {
	return &BrokerDispatcher{
		address: address,
		cmd:     cmd,
		perTube: perTube,
		tubeSet: make(map[string]bool),
	}
}

// RunTube runs broker(s) for the specified tube.
// The number of brokers started is determined by the perTube argument to
// NewBrokerDispatcher.
func (bd *BrokerDispatcher) RunTube(tube string) {
	bd.tubeSet[tube] = true
	for i := uint64(0); i < bd.perTube; i++ {
		bd.runBroker(tube, i)
	}
}

// RunTube runs brokers for the specified tubes.
func (bd *BrokerDispatcher) RunTubes(tubes []string) {
	for _, tube := range tubes {
		bd.RunTube(tube)
	}
}

// RunAllTubes polls beanstalkd, running broker as new tubes are created.
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

func (bd *BrokerDispatcher) runBroker(tube string, slot uint64) {
	go func() {
		b := New(bd.address, tube, slot, bd.cmd, nil)
		b.Run(nil)
	}()
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
