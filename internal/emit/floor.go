package emit

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// errNoDurableEmitter is what FloorEmitter.Probe reports, so the scrape reads
// metering_valkey_up 0.
var errNoDurableEmitter = errors.New("emit: durable emitter failed to initialise")

// FloorEmitter is the metering emitter a replica runs when the durable emitter
// could not start: a corrupt WAL that cannot be quarantined, or a legacy WAL import
// that fails. (A WAL that cannot be opened for any other reason does not land here:
// the durable emitter starts with a poisoned wal and retries the open.) Serving
// continues, so every event goes straight to the log floor, in the same format
// DurableEmitter uses and internal/recovery parses.
//
// It reports its health through the same methods as DurableEmitter, so the
// metering metrics stay present and the alerts on them fire. A missing series
// would make those alerts silent during this failure.
type FloorEmitter struct {
	log         *logging.Logger
	walPath     string
	floorEvents atomic.Uint64

	// quarantineLog throttles the log line of a failed WALQuarantinedDirs count.
	quarantineLog quarantineCountLog
}

var _ metering.Emitter = (*FloorEmitter)(nil)

// NewFloorEmitter returns a FloorEmitter that writes to log. walPath is the WAL
// path the durable emitter failed to open, or "" if there is none; it is only read
// to count the corrupt directories moved aside from it.
func NewFloorEmitter(log *logging.Logger, walPath string) *FloorEmitter {
	return &FloorEmitter{log: log, walPath: walPath}
}

// Emit satisfies metering.Emitter. It stamps TimestampUnixMs as DurableEmitter
// does, because the floor line is the only copy of the record.
func (f *FloorEmitter) Emit(_ context.Context, ev metering.Event) {
	ev.TimestampUnixMs = time.Now().UnixMilli()
	f.floorEvents.Add(1)
	logFloorEvent(f.log, ev)
}

// WALPending reports the count as untrustworthy. No WAL is open, and the failed
// start may have left unshipped events on disk, so this must not read as an
// empty WAL.
func (f *FloorEmitter) WALPending() (uint64, bool) { return 0, false }

// WALPoisoned reports true: there is no open log, new events reach the log floor,
// and events on disk are not shipped.
func (f *FloorEmitter) WALPoisoned() bool { return true }

// WALQuarantinedDirs reads the disk on each scrape and counts the corrupt WAL
// directories moved aside, by the failed start or an earlier run. They hold unbilled
// events whether or not a durable emitter is running. A negative result means the
// directory could not be read and the count is unknown. With no WAL path it is 0.
func (f *FloorEmitter) WALQuarantinedDirs() int {
	if f.walPath == "" {
		return 0
	}
	return quarantinedDirsOrUnknown(f.walPath, f.log, &f.quarantineLog)
}

// WALSkippedEntries returns 0: this emitter reads no WAL entries.
func (f *FloorEmitter) WALSkippedEntries() uint64 { return 0 }

// FloorEvents returns how many events reached the log floor since start.
func (f *FloorEmitter) FloorEvents() uint64 { return f.floorEvents.Load() }

// Probe always fails: this emitter has no Valkey client.
func (f *FloorEmitter) Probe(context.Context) (MeteringProbe, error) {
	return MeteringProbe{}, errNoDurableEmitter
}

// logFloorEvent writes the structured line internal/recovery parses.
func logFloorEvent(log *logging.Logger, ev metering.Event) {
	data, _ := json.Marshal(ev)
	log.Error.Printf("METERING_FLOOR request_id=%s event=%s", ev.RequestID, data)
}
