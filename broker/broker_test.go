package broker

import "testing"

const (
	address = "127.0.0.1:11300"
)

func TestBroker(t *testing.T) {
	b := New(address, "cmdstalk-TestBroker", "hexdump -C", nil)
	go b.Run()
}
