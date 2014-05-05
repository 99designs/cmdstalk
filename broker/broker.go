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
	"syscall"
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
// If ticks channel is present, one job is processed per tick.
func (b *Broker) Run(ticks chan bool) {
	b.log.Println("connecting to", b.Address)
	conn, err := beanstalk.Dial("tcp", b.Address)
	if err != nil {
		panic(err)
	}

	b.log.Println("watching", b.Tube)
	ts := beanstalk.NewTubeSet(conn, b.Tube)

	for {
		if ticks != nil {
			if _, ok := <-ticks; !ok {
				break
			}
		}

		err = b.doTick(conn, ts)
		if err != nil {
			log.Panic(err)
		}
	}

	b.log.Println("broker finished")
}

func (b *Broker) doTick(conn *beanstalk.Conn, ts *beanstalk.TubeSet) error {
	id, body, err := ts.Reserve(24 * time.Hour)
	if err != nil {
		return err
	}

	job := job{id: id, body: body, conn: conn}

	result, err := b.handleJob(job, b.Cmd)
	if err != nil {
		return err
	}

	b.log.Printf("job %d finished with exit(%d)", id, result.ExitStatus)
	if result.ExitStatus == 0 {
		ts.Conn.Delete(id)
	} else if result.ExitStatus == 1 {
		job.release()
	} else {
		return fmt.Errorf("Unhandled exit status %d", result.ExitStatus)
	}

	if b.results != nil {
		b.results <- result
	}

	return nil
}

func (b *Broker) handleJob(job job, shellCmd string) (*JobResult, error) {

	result := &JobResult{JobId: job.id}

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
	written, err := stdin.Write(job.body)
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

	if e1, ok := err.(*exec.ExitError); ok {
		result.ExitStatus = e1.Sys().(syscall.WaitStatus).ExitStatus()
	} else {
		result.ExitStatus = 0
	}

	result.Stdout = stdoutBuffer.String()

	return result, nil
}
