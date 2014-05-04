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
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/99designs/gobean/cli"
	"github.com/kr/beanstalk"
)

func main() {
	opts := cli.MustParseFlags()

	log.Println("Connecting to", opts.Address)
	c, err := beanstalk.Dial("tcp", opts.Address)
	if err != nil {
		panic(err)
	}

	log.Println("watching", opts.Tubes)
	ts := beanstalk.NewTubeSet(c, opts.Tubes...)

	for {
		id, body, err := ts.Reserve(24 * time.Hour)
		if err != nil {
			log.Fatal(err)
		}
		handleJob(id, body, opts.Cmd.Name, opts.Cmd.Args)
		ts.Conn.Delete(id)
	}
}

func handleJob(id uint64, body []byte, name string, args []string) {
	cmd := exec.Command(name, args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	// write into stdin
	written, err := stdin.Write(body)
	if err == nil {
		log.Println(written, "bytes written")
	} else {
		log.Fatal(err)
	}
	stdin.Close()

	// read from stdout
	read, err := io.Copy(os.Stdout, stdout)
	if err == nil {
		log.Println(read, "bytes read")
	} else {
		log.Fatal(err)
	}

	err = cmd.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
