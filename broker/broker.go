/*
	Package broker reserves jobs from beanstalkd, spawns worker processes,
	and manages the interaction between the two.
*/
package broker

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/kr/beanstalk"
)

type Broker struct {

	// Address of the beanstalkd server.
	Address string

	// The shell command to execute for each job.
	Cmd string

	// Tube name this broker will service.
	Tube string

	log     *log.Logger
	results chan<- *JobResult
}

type JobResult struct {

	// ExitStatus of the command; 0 for success.
	ExitStatus int

	// JobId from beanstalkd.
	JobId uint64

	// Stdout of the command.
	Stdout string
}

// New broker instance.
func New(address, tube string, cmd string, results chan<- *JobResult) (b Broker) {
	b.Address = address
	b.Tube = tube
	b.Cmd = cmd

	b.log = log.New(os.Stdout, fmt.Sprintf("[%s] ", tube), log.LstdFlags)
	b.results = results
	return
}

// Run connects to beanstalkd and starts broking.
func (b *Broker) Run() {
	b.log.Println("connecting to", b.Address)
	c, err := beanstalk.Dial("tcp", b.Address)
	if err != nil {
		panic(err)
	}

	b.log.Println("watching", b.Tube)
	ts := beanstalk.NewTubeSet(c, b.Tube)

	for {
		id, body, err := ts.Reserve(24 * time.Hour)
		if err != nil {
			b.log.Fatal(err)
		}
		result, err := b.handleJob(id, body, b.Cmd)
		if err != nil {
			log.Fatal(err)
		}
		ts.Conn.Delete(id)
		if b.results != nil {
			b.results <- result
		}
	}
}

func (b *Broker) handleJob(id uint64, body []byte, shellCmd string) (*JobResult, error) {

	cmd := exec.Command("/bin/bash", "-c", shellCmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	// write into stdin
	written, err := stdin.Write(body)
	if err == nil {
		b.log.Println(written, "bytes written")
	} else {
		return nil, err
	}
	stdin.Close()

	// read from stdout
	stdoutBuffer := new(bytes.Buffer)
	read, err := io.Copy(stdoutBuffer, stdout)
	if err == nil {
		b.log.Println(read, "bytes read")
	} else {
		return nil, err
	}

	err = cmd.Wait()
	if err != nil {
		return nil, err
	}

	return &JobResult{
		ExitStatus: 0,
		JobId:      id,
		Stdout:     stdoutBuffer.String(),
	}, nil
}
