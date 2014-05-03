package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"
)
import "github.com/kr/beanstalk"

func main() {
	tubes := tubesFlag()

	c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		panic(err)
	}

	log.Println("watching", tubes)
	ts := beanstalk.NewTubeSet(c, tubes...)

	for {
		id, body, err := ts.Reserve(24 * time.Hour)
		fmt.Println(id, string(body), err)
		ts.Conn.Delete(id)
	}
}

func tubesFlag() []string {
	var raw string
	flag.StringVar(&raw, "tubes", "default", "Comma-separated list of tubes to watch")
	flag.Parse()
	return strings.Split(raw, ",")
}
