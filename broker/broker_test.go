package broker

import (
	"bytes"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/kr/beanstalk"
)

const (
	address    = "127.0.0.1:11300"
	defaultTtr = 10 * time.Second
)

// TestWorkerSuccess demonstrates a successful exit(0) task (delete).
func TestWorkerSuccess(t *testing.T) {
	tube, id := queueJob("hello world", 10, defaultTtr)
	expectStdout := []byte("HELLO WORLD")

	cmd := "tr [a-z] [A-Z]"
	results := make(chan *JobResult)
	b := New(address, tube, 0, cmd, results)

	ticks := make(chan bool)
	defer close(ticks)
	go b.Run(ticks)
	ticks <- true // handle a single job

	result := <-results

	if result.JobId != id {
		t.Fatalf("result.JobId %d != queueJob id %d", result.JobId, id)
	}
	if !bytes.Equal(result.Stdout, expectStdout) {
		t.Fatalf("Stdout mismatch: '%s' != '%s'\n", result.Stdout, expectStdout)
	}
	if result.ExitStatus != 0 {
		t.Fatalf("Unexpected exit status %d", result.ExitStatus)
	}

	assertTubeEmpty(tube)
}

// TestWorkerFailure demonstrates a failed exit(1) task (release).
func TestWorkerFailure(t *testing.T) {
	tube, id := queueJob("hello world", 10, defaultTtr)

	cmd := "false"
	results := make(chan *JobResult)
	b := New(address, tube, 0, cmd, results)

	ticks := make(chan bool)
	defer close(ticks)
	go b.Run(ticks)
	ticks <- true // handle a single job

	result := <-results

	if result.JobId != id {
		t.Fatalf("result.JobId %d != queueJob id %d", result.JobId, id)
	}

	if result.ExitStatus != 1 {
		t.Fatalf("result.ExitStatus %d, expected 1", result.ExitStatus)
	}

	assertJobStat(t, id, "state", "ready")
	assertJobStat(t, id, "releases", "1")
	assertJobStat(t, id, "pri", "10")
}

func TestWorkerTimeout(t *testing.T) {
	ttr := 1 * time.Second
	tube, id := queueJob("TestWorkerTimeout", 10, ttr)

	cmd := "sleep 4"
	results := make(chan *JobResult)
	b := New(address, tube, 0, cmd, results)

	ticks := make(chan bool)
	defer close(ticks)
	go b.Run(ticks)

	start := time.Now()
	ticks <- true // handle a single job
	result := <-results
	duration := time.Since(start)

	if duration < 1*time.Second {
		t.Fatalf("%v too short to have timed out correctly", duration)
	}

	if !result.TimedOut {
		t.Fatalf("Expected job %d JobResult.TimedOut to be true", id)
	}

	assertJobStat(t, id, "state", "ready")
	assertJobStat(t, id, "timeouts", "1")

	ticks <- true // handle another job
	result = <-results
	if result.Buried {
		t.Fatalf("Expected job %d not to be buried", id)
	}
	assertJobStat(t, id, "state", "ready")
	assertJobStat(t, id, "timeouts", "2")
}

func queueJob(body string, priority uint32, ttr time.Duration) (string, uint64) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tubeName := "cmdstalk-test-" + strconv.FormatInt(r.Int63(), 16)
	assertTubeEmpty(tubeName)

	c, err := beanstalk.Dial("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	tube := beanstalk.Tube{Conn: c, Name: tubeName}

	id, err := tube.Put([]byte(body), priority, 0, ttr)
	if err != nil {
		log.Fatal(err)
	}

	return tubeName, id
}

func assertTubeEmpty(tubeName string) {
	// TODO
}

func assertJobStat(t *testing.T, id uint64, key, value string) {
	c, err := beanstalk.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := c.StatsJob(id)
	if err != nil {
		t.Fatal(err)
	}
	if stats[key] != value {
		t.Fatalf("job %d %s = %s, expected %s", id, key, stats[key], value)
	}
}
