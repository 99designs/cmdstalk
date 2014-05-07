cmdstalk
========

Cmdstalk is a unix-process-based [beanstalkd][beanstalkd] queue broker.

Written in [Go][golang], cmdstalk uses the [kr/beanstalk][beanstalk]
library to interact with the [beanstalkd][beanstalkd] queue daemon.

Each job is passed as stdin to a new instance of the configured worker command.
On `exit(0)` the job is deleted. On `exit(1)` the job is released with an
exponential-backoff delay (releases^4), up to 10 times.

If the worker has not finished by the time the job TTR is reached, the worker
is killed (SIGTERM, SIGKILL) and the job is allowed to time out. When the
job is subsequently reserved, the `timeouts: 1` will cause it to be buried.

In this way, job workers can be arbitrary commands, and queue semantics are
reduced down to basic unix concepts of exit status and signals.


Install
-------

From source:

```sh
# Make sure you have a sane $GOPATH
go get github.com/99designs/cmdstalk
```

From binary:

```sh
# TODO
```


Usage
-----

```sh
cmdstalk -help
# Usage of ./cmdstalk:
#   -address="127.0.0.1:11300": beanstalkd TCP address.
#   -all=false: Listen to all tubes, instead of -tubes=...
#   -cmd="": Command to run in worker.
#   -per-tube=1: Number of workers per tube.
#   -tubes=[default]: Comma separated list of tubes.

# Watch three specific tubes.
cmdstalk -cmd="/path/to/your/worker --your=flags --here" -tubes="one,two,three"

# Watch all current and future tubes, four workers per tube.
cmdstalk -all -cmd="cat" -per-tube=4
```


Dev
---

[gpm](https://github.com/pote/gpm) is used to track dependencies. Make sure
you have a sane `$GOPATH` etc, then `brew install gpm` and `gpm install`.

```sh
# Run all tests, with minimal/buffered output.
go test ./...

# Run tests in the broker package with steaming output.
(cd broker && go test -v)

# Run cmdstalk from source.
go run cmdstalk.go -cmd='hexdump -C' -tubes="default,another"

# Build and run a binary.
go build
file cmdstalk # cmdstalk: Mach-O 64-bit executable x86_64
```


TODO
----

* Graceful shutdown.
* SIGKILL recalcitrant worker processes.
* Handle stdout.
* Ship linux binary; GitHub releases?


[beanstalkd]: http://kr.github.io/beanstalkd/
[beanstalk]: http://godoc.org/github.com/kr/beanstalk
[golang]: http://golang.org/
