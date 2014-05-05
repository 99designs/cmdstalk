/*
	Package broker reserves jobs from beanstalkd, spawns worker processes,
	and manages the interaction between the two.
*/
package broker

import (
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/99designs/cmdstalk/cli"
	"github.com/kr/beanstalk"
)

type Broker struct {

	// Address of the beanstalkd server.
	Address string

	// The command to execute for each job.
	Cmd cli.CommandWithArgs

	// Tube name this broker will service.
	Tube string
}

// New broker instance.
func New(address, tube string, cmd cli.CommandWithArgs) (b Broker) {
	b.Address = address
	b.Tube = tube
	b.Cmd = cmd
	return
}

// Run connects to beanstalkd and starts broking.
func (b *Broker) Run() {
	log.Println("Connecting to", b.Address)
	c, err := beanstalk.Dial("tcp", b.Address)
	if err != nil {
		panic(err)
	}

	log.Println("watching", b.Tube)
	ts := beanstalk.NewTubeSet(c, b.Tube)

	for {
		id, body, err := ts.Reserve(24 * time.Hour)
		if err != nil {
			log.Fatal(err)
		}
		handleJob(id, body, b.Cmd.Name, b.Cmd.Args)
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
