// gobean is a beanstalkd queue broker. It connects to beanstalkd, watches
// tubes, reserves jobs, and spawns subcommands to process the work.
//
// gobean monitors the stdout and exit status of the worker process, and
// manages the beanstalkd job accordingly.
//
// Output from the worker process causes gobean to touch the beanstalkd job,
// refreshing the TTR. If TTR is reached with no output, gobean will send
// SIGTERM then SIGKILL to the worker, and allow the job to time out.
//
// Worker exit(0) tells gobean to delete the job.
// Worker exit(1) tells gobean to release the job for reprocessing.
// Worker exit(2) tells gobean to bury the job.
//
// Stderr from the workers is sent to gobean stderr.
package main

import (
	"github.com/99designs/gobean/broker"
	"github.com/99designs/gobean/cli"
)

func main() {
	opts := cli.MustParseFlags()

	for _, tube := range opts.Tubes {
		go func(tube string) {
			b := broker.New(opts.Address, tube, opts.Cmd)
			b.Run()
		}(tube)
	}

	exitChan := make(chan bool)
	<-exitChan
}
