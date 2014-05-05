// cmdstalk is a beanstalkd queue broker. It connects to beanstalkd, watches
// tubes, reserves jobs, and spawns subcommands to process the work.
//
// cmdstalk monitors the stdout and exit status of the worker process, and
// manages the beanstalkd job accordingly.
//
// Output from the worker process causes cmdstalk to touch the beanstalkd job,
// refreshing the TTR. If TTR is reached with no output, cmdstalk will send
// SIGTERM then SIGKILL to the worker, and allow the job to time out.
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

	for _, tube := range opts.Tubes {
		go func(tube string) {
			b := broker.New(opts.Address, tube, opts.Cmd, nil)
			b.Run(nil)
		}(tube)
	}

	// TODO: wire up to SIGTERM handler etc.
	exitChan := make(chan bool)
	<-exitChan
}
