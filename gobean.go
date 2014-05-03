package main

import (
	"fmt"
	"time"
)
import "github.com/kr/beanstalk"

func main() {
	fmt.Println("hello gobean")

	c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		panic(err)
	}

	for {
		id, body, err := c.Reserve(24 * time.Hour)
		fmt.Println(id, string(body), err)
		c.Delete(id)
	}
}
