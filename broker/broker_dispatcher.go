package broker

type BrokerDispatcher struct {
	address string
	cmd     string
}

func NewBrokerDispatcher(address, cmd string) *BrokerDispatcher {
	return &BrokerDispatcher{
		address: address,
		cmd:     cmd,
	}
}

// RunTube runs a broker for the specified tube.
func (bd *BrokerDispatcher) RunTube(tube string) {
	go func() {
		b := New(bd.address, tube, bd.cmd, nil)
		b.Run(nil)
	}()
}
