package cli

import (
	"flag"
	"fmt"
	"strings"
)

type Options struct {
	Tubes TubeList
	Cmd   string
}

type TubeList []string

func ParseFlags() (o Options) {

	flag.StringVar(&o.Cmd, "cmd", "", "Command to run in worker.")

	flag.Var(&o.Tubes, "tubes", "Comma separated list of tubes.")

	flag.Parse()

	return
}

func (t *TubeList) Set(value string) error {
	list := strings.Split(value, ",")
	for i, value := range list {
		list[i] = value
	}
	*t = list
	return nil
}

func (t *TubeList) String() string {
	return fmt.Sprint(*t)
}
