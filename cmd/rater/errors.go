package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"
)

// signalContext returns a context cancelled on SIGTERM/SIGINT, for graceful
// shutdown mid-run (a CronJob pod may be evicted).
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}

// configError mirrors the drainer's typed config error: a semantic error for a
// malformed settings value, with Unwrap so errors.Is reaches the cause.
type configError struct {
	key string
	err error
}

func (e *configError) Error() string { return "invalid " + e.key + ": " + e.err.Error() }
func (e *configError) Unwrap() error { return e.err }

func errInvalidDuration(key string, err error) error {
	return &configError{key: key, err: err}
}

func errInvalidTrailingHours(n int) error {
	return &configError{
		key: "rateTrailingHours",
		err: fmt.Errorf("%d is out of range: must be >= 1 (the trailing-window width in complete hours; shrinking it below 1 would rate nothing, and the wider it is the more late-drained events each run catches)", n),
	}
}

// windowError is a semantic error for a bad/inverted rating window.
type windowError struct{ msg string }

func (e *windowError) Error() string { return e.msg }

func errBadWindow(flag, val string, err error) error {
	return &windowError{msg: "invalid --" + flag + " " + val + ": " + err.Error()}
}

func errInvertedWindow(start, end time.Time) error {
	return &windowError{
		msg: "empty/inverted window [" + start.Format(time.RFC3339) + "," + end.Format(time.RFC3339) + "): start must be before end",
	}
}

func errUnalignedWindow(which string, t time.Time) error {
	return &windowError{
		msg: "--" + which + " " + t.Format(time.RFC3339) + " is not hour-aligned: the rater's grain is the whole hour (rollups bucket by date_trunc('hour') and the upsert REPLACES a bucket), so a sub-hour window would overwrite a complete hourly rollup with a partial sum — pass hour-aligned bounds (minutes/seconds = 0)",
	}
}
