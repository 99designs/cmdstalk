/*
	Package cmd provides a more domain-specific layer over exec.Cmd.
*/
package cmd

import (
	"io"
	"os"
	"os/exec"
	"syscall"
)

const (
	// Shell will have the command line passed to its `-c` option.
	Shell = "/bin/bash"
)

type Cmd struct {
	cmd        *exec.Cmd
	stderrPipe io.ReadCloser
	stdinPipe  io.WriteCloser
	stdoutPipe io.ReadCloser
}

// WaitResult is sent to the channel returned by WaitChan().
// It indicates the exit status, or a non-exit-status error e.g. IO error.
// In the case of a non-exit-status, Status is -1
type WaitResult struct {
	Status int
	Err    error
}

// NewCommand returns a Cmd with IO configured, but not started.
func NewCommand(shellCmd string) (cmd *Cmd, out <-chan []byte, err error) {
	cmd = &Cmd{}
	cmd.cmd = exec.Command(Shell, "-c", shellCmd)

	stdin, err := cmd.cmd.StdinPipe()
	if err == nil {
		cmd.stdinPipe = stdin
	} else {
		return
	}

	stdout, err := cmd.cmd.StdoutPipe()
	if err == nil {
		cmd.stdoutPipe = stdout
	} else {
		return
	}

	cmd.cmd.Stderr = os.Stderr
	cmd.stderrPipe = os.Stderr

	out = readerToChannel(cmd.stdoutPipe)
	return
}

// Start the process, write input to stdin, then close stdin.
func (c *Cmd) StartWithStdin(input []byte) (err error) {
	err = c.cmd.Start()
	if err != nil {
		return
	}
	_, err = c.stdinPipe.Write(input)
	if err != nil {
		return
	}
	c.stdinPipe.Close()
	return nil
}

// Terminate the process with SIGTERM.
// TODO: follow up with SIGKILL if still running.
func (c *Cmd) Terminate() (err error) {
	return c.cmd.Process.Signal(syscall.SIGTERM)
}

// WaitChan starts a goroutine to wait for the command to exit, and returns
// a channel over which will be sent the WaitResult, containing either the
// exit status (0 for success) or a non-exit error, e.g. IO error.
func (cmd *Cmd) WaitChan() <-chan WaitResult {
	ch := make(chan WaitResult)
	go func() {
		err := cmd.cmd.Wait()
		if err == nil {
			ch <- WaitResult{0, nil}
		} else if e1, ok := err.(*exec.ExitError); ok {
			status := e1.Sys().(syscall.WaitStatus).ExitStatus()
			ch <- WaitResult{status, nil}
		} else {
			ch <- WaitResult{-1, err}
		}
	}()
	return ch
}

func readerToChannel(reader io.Reader) <-chan []byte {
	c := make(chan []byte)
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				res := make([]byte, n)
				copy(res, buf[:n])
				c <- res
			}
			if err != nil {
				close(c)
				break
			}
		}
	}()
	return c
}
