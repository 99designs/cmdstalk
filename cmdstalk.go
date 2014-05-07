// cmdstalk is a beanstalkd queue broker. It connects to beanstalkd, watches
// tubes, reserves jobs, and spawns subcommands to process the work.
//
// cmdstalk monitors the exit status of the worker process, and manages the
// beanstalkd job accordingly.
//
// If a job TTR is reached, cmdstalk will send SIGTERM then SIGKILL to the
// worker, and allow the job to time out.
//
// Worker exit(0) tells cmdstalk to delete the job.
// Worker exit(1) tells cmdstalk to release the job for reprocessing.
// Worker exit(2) tells cmdstalk to bury the job.
//
// Stderr from the workers is sent to cmdstalk stderr.
package main

import (
	"github.com/99designs/cmdstalk/broker"
	"github.com/99designs/cmdstalk/cli"
)

func main() {
	opts := cli.MustParseFlags()

	bd := broker.NewBrokerDispatcher(opts.Address, opts.Cmd)

	if opts.All {
		bd.RunAllTubes()
	} else {
		bd.RunTubes(opts.Tubes)
	}

	// TODO: wire up to SIGTERM handler etc.
	exitChan := make(chan bool)
	<-exitChan
}
