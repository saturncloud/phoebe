// Package recovery validates and replays metering evidence that fell below
// the normal Valkey -> Postgres durability path.
package recovery

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
	tidwall "github.com/tidwall/wal"

	"github.com/saturncloud/phoebe/internal/metering"
)

// Evidence is the validated, de-duplicated content of one recovery artifact.
type Evidence struct {
	Events     []metering.Event
	Records    int
	Duplicates int
}

// Digest returns a stable SHA-256 digest binding the COMPLETE validated,
// de-duplicated event set — not merely its request-id set. -apply compares this
// value so that the artifact an operator reviewed during dry-run is provably
// the artifact that gets replayed: swapping in a different set with the same
// cardinality, or altering token counts / org attribution under the same
// request ids, changes the digest and is refused before any write.
//
// Events are canonically serialized (encoding/json emits struct fields in
// declaration order, so a given build produces one byte sequence per event) and
// sorted by the trusted request id, which validateAndDedupe has already proven
// unique. Framing each record with its length keeps concatenation unambiguous.
func (e Evidence) Digest() string {
	encoded := make([][]byte, 0, len(e.Events))
	for i := range e.Events {
		data, err := json.Marshal(e.Events[i])
		if err != nil {
			// metering.Event is a flat struct of JSON-representable scalars, so
			// this is unreachable; degrade closed rather than emit a digest
			// that would falsely certify an unserializable set.
			return ""
		}
		encoded = append(encoded, data)
	}
	sort.Slice(encoded, func(i, j int) bool { return bytes.Compare(encoded[i], encoded[j]) < 0 })
	h := sha256.New()
	for _, data := range encoded {
		_, _ = fmt.Fprintf(h, "%d:", len(data))
		_, _ = h.Write(data)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Load reads a legacy/imported JSONL file, log output containing
// METERING_FLOOR records, or a tidwall WAL directory. WAL directories are
// copied to a temporary directory before opening because tidwall.Open is not a
// read-only API; the forensic source is never mutated.
func Load(path string) (Evidence, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Evidence{}, fmt.Errorf("stat recovery input %s: %w", path, err)
	}
	var events []metering.Event
	if info.IsDir() {
		events, err = loadWALCopy(path)
	} else if info.Mode().IsRegular() {
		events, err = loadLines(path)
	} else {
		return Evidence{}, fmt.Errorf("recovery input %s is neither a regular file nor a directory", path)
	}
	if err != nil {
		return Evidence{}, err
	}
	return validateAndDedupe(events)
}

func loadLines(path string) ([]metering.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open recovery input %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	r := bufio.NewReaderSize(f, 64*1024)
	var events []metering.Event
	for lineNo := 1; ; lineNo++ {
		line, readErr := r.ReadBytes('\n')
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			if marker := bytes.Index(line, []byte("METERING_FLOOR ")); marker >= 0 {
				line = line[marker:]
				eventAt := bytes.Index(line, []byte(" event="))
				if eventAt < 0 {
					return nil, fmt.Errorf("%s:%d: METERING_FLOOR record has no event field", path, lineNo)
				}
				line = line[eventAt+len(" event="):]
			} else if line[0] != '{' {
				// A log export normally includes unrelated application lines.
				// Only structured floor records and bare JSONL objects are input.
				if readErr == io.EOF {
					break
				}
				continue
			}
			ev, err := decodeEvent(line)
			if err != nil {
				return nil, fmt.Errorf("%s:%d: %w", path, lineNo, err)
			}
			events = append(events, ev)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read recovery input %s: %w", path, readErr)
		}
	}
	return events, nil
}

func loadWALCopy(source string) ([]metering.Event, error) {
	tmp, err := os.MkdirTemp("", "phoebe-recover-wal-")
	if err != nil {
		return nil, fmt.Errorf("create temporary WAL copy: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	copyDir := filepath.Join(tmp, "wal")
	if err := copyTree(source, copyDir); err != nil {
		return nil, fmt.Errorf("copy WAL evidence: %w", err)
	}

	log, err := tidwall.Open(copyDir, nil)
	if err != nil {
		return nil, fmt.Errorf("open copied WAL (source left unchanged): %w", err)
	}
	defer func() { _ = log.Close() }()
	first, err := log.FirstIndex()
	if err != nil {
		return nil, fmt.Errorf("read copied WAL first index: %w", err)
	}
	last, err := log.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("read copied WAL last index: %w", err)
	}
	if last == 0 {
		return nil, nil
	}
	events := make([]metering.Event, 0, last-first+1)
	for index := first; index <= last; index++ {
		data, err := log.Read(index)
		if err != nil {
			return nil, fmt.Errorf("read copied WAL index %d: %w", index, err)
		}
		ev, err := decodeEvent(data)
		if err != nil {
			return nil, fmt.Errorf("copied WAL index %d: %w", index, err)
		}
		events = append(events, ev)
	}
	return events, nil
}

func copyTree(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("refuse non-regular WAL entry %s", path)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
		if err != nil {
			_ = in.Close()
			return err
		}
		_, copyErr := io.Copy(out, in)
		inCloseErr := in.Close()
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if inCloseErr != nil {
			return inCloseErr
		}
		return closeErr
	})
}

func decodeEvent(data []byte) (metering.Event, error) {
	var ev metering.Event
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&ev); err != nil {
		return ev, fmt.Errorf("decode event JSON: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return ev, fmt.Errorf("event JSON has trailing value")
		}
		return ev, fmt.Errorf("event JSON has trailing data: %w", err)
	}
	return ev, nil
}

func validateAndDedupe(events []metering.Event) (Evidence, error) {
	result := Evidence{Records: len(events)}
	seen := make(map[string]metering.Event, len(events))
	for i, ev := range events {
		if err := validate(ev); err != nil {
			return Evidence{}, fmt.Errorf("record %d: %w", i+1, err)
		}
		if prior, ok := seen[ev.RequestID]; ok {
			if prior != ev {
				return Evidence{}, fmt.Errorf("record %d: conflicting duplicate request_id %q", i+1, ev.RequestID)
			}
			result.Duplicates++
			continue
		}
		seen[ev.RequestID] = ev
		result.Events = append(result.Events, ev)
	}
	return result, nil
}

func validate(ev metering.Event) error {
	if err := printableID("request_id", ev.RequestID); err != nil {
		return err
	}
	if ev.ClientRequestID != "" {
		if err := printableID("client_request_id", ev.ClientRequestID); err != nil {
			return err
		}
	}
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{
		{"auth_id", ev.AuthID, 64}, {"user_id", ev.UserID, 32},
		{"group_id", ev.GroupID, 32}, {"resource_id", ev.ResourceID, 64},
		{"resource_type", ev.ResourceType, 64}, {"org_id", ev.OrgID, 64},
		{"model", ev.Model, 255}, {"base_model", ev.BaseModel, 255},
		{"adapter", ev.Adapter, 255}, {"serving_mode", ev.ServingMode, 32},
		{"finish_reason", ev.FinishReason, 64}, {"gpu_type", ev.GPUType, 64},
	} {
		if !utf8.ValidString(field.value) {
			return fmt.Errorf("%s is not valid UTF-8", field.name)
		}
		if strings.ContainsRune(field.value, '\x00') {
			return fmt.Errorf("%s contains a NUL byte unsupported by PostgreSQL text", field.name)
		}
		if count := utf8.RuneCountInString(field.value); count > field.max {
			return fmt.Errorf("%s is %d characters; database maximum is %d", field.name, count, field.max)
		}
	}
	if ev.TimestampUnixMs <= 0 {
		return fmt.Errorf("timestamp_unix_ms must be positive")
	}
	// billing_event_status_code_ck admits NULL or 100..599. Zero marshals as
	// the omitted/NULL case, but any other out-of-range value would replay into
	// Valkey successfully and then poison the drainer on insert, so reject it
	// here where the failure is still an operator-visible dry-run error.
	if ev.StatusCode != 0 && (ev.StatusCode < 100 || ev.StatusCode > 599) {
		return fmt.Errorf("status_code %d is outside the database range 100..599", ev.StatusCode)
	}
	return nil
}

func printableID(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) >= 255 {
		return fmt.Errorf("%s is %d bytes; must be fewer than 255", name, len(value))
	}
	if strings.IndexFunc(value, func(r rune) bool { return r < 0x20 || r > 0x7e }) >= 0 {
		return fmt.Errorf("%s must contain printable ASCII only", name)
	}
	return nil
}

// Replay XADDs each event under the same stream shape as the normal emitter.
// A rerun is safe because the Postgres drainer is idempotent on request_id.
func Replay(ctx context.Context, rdb redis.Cmdable, stream string, timeout time.Duration, events []metering.Event) (int, error) {
	if rdb == nil {
		return 0, fmt.Errorf("redis client is required")
	}
	if stream == "" {
		return 0, fmt.Errorf("stream is required")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	for i, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			return i, fmt.Errorf("marshal request_id %s: %w", ev.RequestID, err)
		}
		writeCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err = rdb.XAdd(writeCtx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"event": string(data)},
		}).Result()
		cancel()
		if err != nil {
			return i, fmt.Errorf("xadd request_id %s after %d successful writes: %w", ev.RequestID, i, err)
		}
	}
	return len(events), nil
}
