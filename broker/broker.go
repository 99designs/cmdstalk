/*
	Package broker reserves jobs from beanstalkd, spawns worker processes,
	and manages the interaction between the two.
*/
package broker

import (
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

	log *log.Logger
}

// New broker instance.
func New(address, tube string, cmd string) (b Broker) {
	b.Address = address
	b.Tube = tube
	b.Cmd = cmd

	b.log = log.New(os.Stdout, fmt.Sprintf("[%s] ", tube), log.LstdFlags)
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
		b.handleJob(id, body, b.Cmd)
		ts.Conn.Delete(id)
	}
}

func (b *Broker) handleJob(id uint64, body []byte, shellCmd string) {

	cmd := exec.Command("/bin/bash", "-c", shellCmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		b.log.Fatal(err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		b.log.Fatal(err)
	}

	err = cmd.Start()
	if err != nil {
		b.log.Fatal(err)
	}

	// write into stdin
	written, err := stdin.Write(body)
	if err == nil {
		b.log.Println(written, "bytes written")
	} else {
		b.log.Fatal(err)
	}
	stdin.Close()

	// read from stdout
	read, err := io.Copy(os.Stdout, stdout)
	if err == nil {
		b.log.Println(read, "bytes read")
	} else {
		b.log.Fatal(err)
	}

	err = cmd.Wait()
	if err != nil {
		b.log.Fatal(err)
	}
}
