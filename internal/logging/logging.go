// Package logging provides a simple leveled logger, mirroring the convention
// used across Saturn's Go services (see auth-server's util.Logger).
package logging

import (
	"io"
	"log"
	"os"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

func output(level, compare Level) io.Writer {
	if level <= compare {
		if level == ERROR {
			return os.Stderr
		}
		return os.Stdout
	}
	return io.Discard
}

// New initializes a leveled logger. Messages below the given level are discarded.
func New(level Level) *Logger {
	logFlags := log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC | log.Lshortfile
	return &Logger{
		Debug: log.New(output(level, DEBUG), "DEBUG: ", logFlags),
		Info:  log.New(output(level, INFO), "INFO:  ", logFlags),
		Warn:  log.New(output(level, WARN), "WARN:  ", logFlags),
		Error: log.New(output(level, ERROR), "ERROR: ", logFlags),
	}
}

// Logger is a simple leveled logging utility.
type Logger struct {
	Debug *log.Logger
	Info  *log.Logger
	Warn  *log.Logger
	Error *log.Logger
}

func (l *Logger) SetLevel(level Level) {
	l.Debug.SetOutput(output(level, DEBUG))
	l.Info.SetOutput(output(level, INFO))
	l.Warn.SetOutput(output(level, WARN))
	l.Error.SetOutput(output(level, ERROR))
}
