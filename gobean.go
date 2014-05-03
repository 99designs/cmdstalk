package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/99designs/gobean/cli"
	"github.com/kr/beanstalk"
)

func main() {
	opts := cli.ParseFlags()
	name, args := parseCommand(opts.Cmd)

	c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
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
		handleJob(id, body, name, args)
		ts.Conn.Delete(id)
	}
}

// TODO: integrate into options.go
func parseCommand(line string) (name string, args []string) {
	s := strings.Fields(line)
	name = s[0]
	args = s[1:]
	return
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
