package cli

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

type Options struct {
	Tubes TubeList
	Cmd   CommandWithArgs
}

type CommandWithArgs struct {
	Name string
	Args []string
}

type TubeList []string

func ParseFlags() (o Options) {
	o.Tubes = TubeList{"default"}

	flag.Var(&o.Cmd, "cmd", "Command to run in worker.")
	flag.Var(&o.Tubes, "tubes", "Comma separated list of tubes.")
	flag.Parse()

	validateOptions(o)

	return
}

func validateOptions(o Options) {
	log.Printf("%#v", o)
	if o.Cmd.Name == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
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

func (c *CommandWithArgs) Set(value string) error {
	parts := strings.Fields(value)
	c.Name = parts[0]
	c.Args = parts[1:]
	return nil
}

func (c *CommandWithArgs) String() string {
	if len(c.Name) == 0 {
		return "\"\""
	} else if len(c.Args) == 0 {
		return c.Name
	} else {
		return fmt.Sprintf("%s %s", c.Name, strings.Join(c.Args, " "))
	}
}
