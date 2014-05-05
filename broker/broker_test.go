package broker

import "testing"

func TestBroker(t *testing.T) {
	b := New("127.0.0.1:11300", "cmdstalk-TestBroker", "hexdump -C")
	go b.Run()
}
