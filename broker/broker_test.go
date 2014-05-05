package broker

import (
	"testing"
	"time"

	"github.com/kr/beanstalk"
)

const (
	address = "127.0.0.1:11300"
)

func TestBroker(t *testing.T) {
	b := New(address, "cmdstalk-TestBroker", "hexdump -C", nil)
	go b.Run()
}

func TestWork(t *testing.T) {
	// TODO: ensure queue is empty

	cmd := "tr [a-z] [A-Z]"
	results := make(chan *JobResult)
	b := New(address, "cmdstalk-TestWork", cmd, results)
	go b.Run()

	id := queueJob("cmdstalk-TestWork", "hello world")
	result := <-results

	if result.JobId != id {
		t.Fatalf("result.JobId %d != queueJob id %d", result.JobId, id)
	}
	if result.Stdout != "HELLO WORLD" {
		t.Fatal("Stdout does not match")
	}
}

func queueJob(tubeName, body string) uint64 {
	c, err := beanstalk.Dial("tcp", address)
	if err != nil {
		panic(err)
	}

	tube := beanstalk.Tube{Conn: c, Name: tubeName}

	id, err := tube.Put([]byte(body), 1, 0, 120*time.Second)
	if err != nil {
		panic(err)
	}

	return id
}
