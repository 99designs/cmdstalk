/*
	Package cli provides command line support for cmdstalk.
*/
package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

// Options contains runtime configuration, and is generally the result of
// parsing command line flags.
type Options struct {

	// The beanstalkd TCP address.
	Address string

	// All == true means all tubes will be watched.
	All bool

	// The shell command to execute for each job.
	Cmd string

	// The beanstalkd tubes to watch.
	Tubes TubeList
}

// TubeList is a list of beanstalkd tube names.
type TubeList []string

// Calls ParseFlags(), os.Exit(1) on error.
func MustParseFlags() (o Options) {
	o, err := ParseFlags()
	if err != nil {
		flag.PrintDefaults()
		fmt.Println()
		fmt.Println(err)
		os.Exit(1)
	}
	return
}

// ParseFlags parses and validates CLI flags into an Options struct.
func ParseFlags() (o Options, err error) {
	o.Tubes = TubeList{"default"}

	flag.StringVar(&o.Address, "address", "127.0.0.1:11300", "beanstalkd TCP address.")
	flag.BoolVar(&o.All, "all", false, "Listen to all tubes, instead of -tubes=...")
	flag.StringVar(&o.Cmd, "cmd", "", "Command to run in worker.")
	flag.Var(&o.Tubes, "tubes", "Comma separated list of tubes.")
	flag.Parse()

	err = validateOptions(o)

	return
}

func validateOptions(o Options) error {
	msgs := make([]string, 0)

	if o.Cmd == "" {
		msgs = append(msgs, "Command must not be empty.")
	}

	if o.Address == "" {
		msgs = append(msgs, "Address must not be empty.")
	}

	if len(msgs) == 0 {
		return nil
	} else {
		return errors.New(strings.Join(msgs, "\n"))
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
