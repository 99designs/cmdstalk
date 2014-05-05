package broker

import (
	"testing"

	"github.com/99designs/cmdstalk/cli"
)

func TestBroker(t *testing.T) {
	cmd := cli.CommandWithArgs{Name: "hexdump", Args: []string{"-C"}}
	b := New("127.0.0.1:11300", "cmdstalk-TestBroker", cmd)
	go b.Run()
}
