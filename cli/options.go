package cli

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

// Options contains runtime configuration, and is generally the result of
// parsing command line flags.
type Options struct {

	// The beanstalkd TCP address.
	Address string

	// The command to execute for each job.
	Cmd CommandWithArgs

	// The beanstalkd tubes to watch.
	Tubes TubeList
}

// CommandWithArgs represents a process command and its arguments, in a
// exec.Command() friendly format.
type CommandWithArgs struct {
	Name string
	Args []string
}

// TubeList is a list of beanstalkd tube names.
type TubeList []string

// ParseFlags parses and validates CLI flags into an Options struct.
// It may exit(1) if CLI validation fails.
func ParseFlags() (o Options) {
	o.Tubes = TubeList{"default"}

	flag.StringVar(&o.Address, "address", "127.0.0.1:11300", "beanstalkd TCP address.")
	flag.Var(&o.Cmd, "cmd", "Command to run in worker.")
	flag.Var(&o.Tubes, "tubes", "Comma separated list of tubes.")
	flag.Parse()

	validateOptions(o)

	return
}

// TODO: return an error instead of os.Exit(1)
func validateOptions(o Options) {
	log.Printf("%#v", o)
	if o.Cmd.Name == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if o.Address == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

// Set replaces the TubeList by parsing the comma-separated value string.
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

// Set replaces the CommandWithArgs by parsing the value string.
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
