/*
	Package broker reserves jobs from beanstalkd, spawns worker processes,
	and manages the interaction between the two.
*/
package broker

import (
	"fmt"
	"io"
	"io/ioutil"
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

	// Error raised while attempting to handle the job.
	Error error
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

func (b *Broker) doTick(conn *beanstalk.Conn, ts *beanstalk.TubeSet) (err error) {
	id, body, err := ts.Reserve(24 * time.Hour)
	if err != nil {
		return
	}

	job := job{id: id, body: body, conn: conn}

	b.log.Printf("handling job %d", job.id)
	result, err := b.handleJob(job, b.Cmd)
	if err != nil {
		return
	}
	if result.Error != nil {
		b.log.Println("result had error")
	}

	b.log.Printf("job %d finished with exit(%d)", id, result.ExitStatus)
	switch result.ExitStatus {
	case 0:
		err = job.delete()
	case 1:
		err = job.release()
	default:
		err = fmt.Errorf("Unhandled exit status %d", result.ExitStatus)
	}

	if b.results != nil {
		b.results <- result
	}

	return
}

func (b *Broker) handleJob(job job, shellCmd string) (result *JobResult, err error) {
	result = &JobResult{JobId: job.id}

	cmd, stdout, err := startCommand(shellCmd, job.body)
	if err != nil {
		return
	}

	log.Println("reading stdout")
	bytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		return
	}

	result.Stdout = string(bytes)

	log.Println("waiting on cmd")
	err = cmd.Wait()

	if e1, ok := err.(*exec.ExitError); ok {
		result.ExitStatus = e1.Sys().(syscall.WaitStatus).ExitStatus()
		err = nil // not a handleJob error
	}

	return
}

func startCommand(shellCmd string, input []byte) (cmd *exec.Cmd, stdout io.ReadCloser, err error) {
	cmd = exec.Command("/bin/bash", "-c", shellCmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return
	}

	stdout, err = cmd.StdoutPipe()
	if err != nil {
		return
	}

	log.Println("starting cmd")
	err = cmd.Start()
	if err != nil {
		return
	}

	log.Println("writing to stdin")
	_, err = stdin.Write(input)
	if err != nil {
		return
	}
	log.Println("closing stdin")
	stdin.Close()

	return
}
