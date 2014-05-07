/*
	Package broker reserves jobs from beanstalkd, spawns worker processes,
	and manages the interaction between the two.
*/
package broker

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/99designs/cmdstalk/bs"
	"github.com/99designs/cmdstalk/cmd"
	"github.com/kr/beanstalk"
)

const (
	// ttrMargin compensates for beanstalkd's integer precision.
	// e.g. reserving a TTR=1 job will show time-left=0.
	// We need to set our SIGTERM timer to time-left + ttrMargin.
	ttrMargin = 1 * time.Second

	// TimeoutTries is the number of timeouts a job must reach before it is
	// buried. Zero means never execute.
	TimeoutTries = 1

	// ReleaseTries is the number of releases a job must reach before it is
	// buried. Zero means never execute.
	ReleaseTries = 10
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

	// Buried is true if the job was buried.
	Buried bool

	// Executed is true if the job command was executed (or attempted).
	Executed bool

	// ExitStatus of the command; 0 for success.
	ExitStatus int

	// JobId from beanstalkd.
	JobId uint64

	// Stdout of the command.
	Stdout []byte

	// TimedOut indicates the worker exceeded TTR for the job.
	// Note this is tracked by a timer, separately to beanstalkd.
	TimedOut bool

	// Error raised while attempting to handle the job.
	Error error
}

// New broker instance.
func New(address, tube string, slot uint64, cmd string, results chan<- *JobResult) (b Broker) {
	b.Address = address
	b.Tube = tube
	b.Cmd = cmd

	b.log = log.New(os.Stdout, fmt.Sprintf("[%s:%d] ", tube, slot), log.LstdFlags)
	b.results = results
	return
}

// Run connects to beanstalkd and starts broking.
// If ticks channel is present, one job is processed per tick.
func (b *Broker) Run(ticks chan bool) {
	b.log.Println("command:", b.Cmd)
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

		b.log.Println("reserve (waiting for job)")
		id, body := bs.MustReserveWithoutTimeout(ts)
		job := bs.NewJob(id, body, conn)

		t, err := job.Timeouts()
		if err != nil {
			b.log.Panic(err)
		}
		if t >= TimeoutTries {
			b.log.Printf("job %d has %d timeouts, burying", job.Id, t)
			job.Bury()
			if b.results != nil {
				b.results <- &JobResult{JobId: job.Id, Buried: true}
			}
			continue
		}

		releases, err := job.Releases()
		if err != nil {
			b.log.Panic(err)
		}
		if releases >= ReleaseTries {
			b.log.Printf("job %d has %d releases, burying", job.Id, releases)
			job.Bury()
			if b.results != nil {
				b.results <- &JobResult{JobId: job.Id, Buried: true}
			}
			continue
		}

		b.log.Printf("executing job %d", job.Id)
		result, err := b.executeJob(job, b.Cmd)
		if err != nil {
			log.Panic(err)
		}

		err = b.handleResult(job, result)
		if err != nil {
			log.Panic(err)
		}

		if result.Error != nil {
			b.log.Println("result had error:", result.Error)
		}

		if b.results != nil {
			b.results <- result
		}
	}

	b.log.Println("broker finished")
}

func (b *Broker) executeJob(job bs.Job, shellCmd string) (result *JobResult, err error) {
	result = &JobResult{JobId: job.Id, Executed: true}

	ttr, err := job.TimeLeft()
	timer := time.NewTimer(ttr + ttrMargin)
	if err != nil {
		return
	}

	cmd, out, err := cmd.NewCommand(shellCmd)
	if err != nil {
		return
	}

	if err = cmd.StartWithStdin(job.Body); err != nil {
		return
	}

	// TODO: end loop when stdout closes
stdoutReader:
	for {
		select {
		case <-timer.C:
			if err = cmd.Terminate(); err != nil {
				return
			}
			result.TimedOut = true
		case data, ok := <-out:
			if !ok {
				break stdoutReader
			}
			b.log.Printf("stdout: %s", data)
			result.Stdout = append(result.Stdout, data...)
		}
	}

	waitC := cmd.WaitChan()

waitLoop:
	for {
		select {
		case wr := <-waitC:
			timer.Stop()
			if wr.Err == nil {
				err = wr.Err
			}
			result.ExitStatus = wr.Status
			break waitLoop
		case <-timer.C:
			cmd.Terminate()
			result.TimedOut = true
		}
	}

	return
}

func (b *Broker) handleResult(job bs.Job, result *JobResult) (err error) {
	if result.TimedOut {
		b.log.Printf("job %d timed out", job.Id)
		return
	}
	b.log.Printf("job %d finished with exit(%d)", job.Id, result.ExitStatus)
	switch result.ExitStatus {
	case 0:
		b.log.Printf("deleting job %d", job.Id)
		err = job.Delete()
	default:
		r, err := job.Releases()
		if err != nil {
			r = ReleaseTries
		}
		// r*r*r*r means final of 10 tries has 1h49m21s delay, 4h15m33s total.
		// See: http://play.golang.org/p/I15lUWoabI
		delay := time.Duration(r*r*r*r) * time.Second
		b.log.Printf("releasing job %d with %v delay (%d retries)", job.Id, delay, r)
		err = job.Release(delay)
	}
	return
}
