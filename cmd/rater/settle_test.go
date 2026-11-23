package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/rating"
)

type fakeSettleStore struct {
	parked, settled []rating.Parked
	settledKeys     []rating.ParkedKey
	by, reason      string
}

func (f *fakeSettleStore) ListParked(context.Context) ([]rating.Parked, error) { return f.parked, nil }
func (f *fakeSettleStore) ListSettled(context.Context) ([]rating.Parked, error) {
	return f.settled, nil
}
func (f *fakeSettleStore) SettleParked(_ context.Context, k rating.ParkedKey, by, reason string) (time.Time, error) {
	f.settledKeys = append(f.settledKeys, k)
	f.by, f.reason = by, reason
	return time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC), nil
}

func errLogger() (*logging.Logger, *bytes.Buffer) {
	var b bytes.Buffer
	l := logging.New(logging.INFO)
	l.Error = log.New(&b, "", 0)
	return l, &b
}

var settleNow = time.Date(2026, 11, 23, 10, 30, 0, 0, time.UTC)

func uk2Row(hour time.Time) rating.Parked {
	return rating.Parked{Kind: rating.KindToken, WindowStart: hour, AuthID: "a", OwnerType: "user", OwnerID: "u",
		ResourceID: "r", ModelID: "qwen25-7b-profile", ServingMode: "dedicated", Reason: "missing_org",
		EventCount: 3, RawEvents: 3}
}

// The uk2 case of #87: a missing_org row from 2026-09-14, older than 30 days,
// raw events still present. It is settled, with who and why.
func TestSettleParked_SettlesARowOlderThan30Days(t *testing.T) {
	row := uk2Row(time.Date(2026, 9, 14, 4, 0, 0, 0, time.UTC))
	f := &fakeSettleStore{parked: []rating.Parked{row}}
	l, errs := errLogger()
	var out bytes.Buffer
	if code := settleParked(context.Background(), f, &out, l, row.Key(), " ops ", " no payer ", settleNow); code != exitOK {
		t.Fatalf("exit %d, want 0: %s", code, errs.String())
	}
	if len(f.settledKeys) != 1 || f.settledKeys[0] != row.Key() || f.by != "ops" || f.reason != "no payer" {
		t.Fatalf("settled %+v by %q reason %q", f.settledKeys, f.by, f.reason)
	}
	if !strings.Contains(out.String(), "SETTLED_AT") || !strings.Contains(out.String(), "no payer") {
		t.Fatalf("output lacks the settled row:\n%s", out.String())
	}
}

// Ruling 2026-10-27: settled by hand after 30 days. A younger row whose raw
// events are all present can still be billed, so it is refused.
func TestSettleParked_RefusesAYoungHourWhoseRawEventsArePresent(t *testing.T) {
	row := uk2Row(settleNow.Truncate(time.Hour).Add(-10 * 24 * time.Hour))
	f := &fakeSettleStore{parked: []rating.Parked{row}}
	l, errs := errLogger()
	if code := settleParked(context.Background(), f, io.Discard, l, row.Key(), "ops", "x", settleNow); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(f.settledKeys) != 0 || !strings.Contains(errs.String(), "can still be billed") {
		t.Fatalf("settled %v; log %q", f.settledKeys, errs.String())
	}
}

// An install whose pruner horizon is shorter than 30 days: once the raw events
// are pruned the hour can never be re-aggregated, so it can be settled.
func TestSettleParked_SettlesAPrunedRowYoungerThan30Days(t *testing.T) {
	row := uk2Row(settleNow.Truncate(time.Hour).Add(-10 * 24 * time.Hour))
	row.RawEvents, row.RawEventsPruned = 0, true
	f := &fakeSettleStore{parked: []rating.Parked{row}}
	l, errs := errLogger()
	if code := settleParked(context.Background(), f, io.Discard, l, row.Key(), "ops", "x", settleNow); code != exitOK {
		t.Fatalf("exit %d, want 0: %s", code, errs.String())
	}
}

func TestSettleParked_RefusesAnAlreadySettledRow(t *testing.T) {
	row := uk2Row(time.Date(2026, 9, 14, 4, 0, 0, 0, time.UTC))
	row.SettledAt, row.SettledBy = settleNow.Add(-time.Hour), "alice"
	f := &fakeSettleStore{settled: []rating.Parked{row}}
	l, errs := errLogger()
	if code := settleParked(context.Background(), f, io.Discard, l, row.Key(), "ops", "x", settleNow); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(f.settledKeys) != 0 || !strings.Contains(errs.String(), `already settled`) || !strings.Contains(errs.String(), "alice") {
		t.Fatalf("settled %v; log %q", f.settledKeys, errs.String())
	}
}

// Every identity field must match: a key that differs in one field settles
// nothing.
func TestSettleParked_RefusesWhenNoRowMatchesEveryField(t *testing.T) {
	row := uk2Row(time.Date(2026, 9, 14, 4, 0, 0, 0, time.UTC))
	k := row.Key()
	k.ServingMode = "shared"
	f := &fakeSettleStore{parked: []rating.Parked{row}}
	l, errs := errLogger()
	if code := settleParked(context.Background(), f, io.Discard, l, k, "ops", "x", settleNow); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(f.settledKeys) != 0 || !strings.Contains(errs.String(), "no unresolved parked row matches") {
		t.Fatalf("settled %v; log %q", f.settledKeys, errs.String())
	}
}

func parseSettle(t *testing.T, args ...string) (rating.ParkedKey, error) {
	t.Helper()
	fs := flag.NewFlagSet("rater", flag.ContinueOnError)
	o := settleFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return o.key()
}

func TestSettleFlags_RequireEveryIdentityFieldWhoAndWhy(t *testing.T) {
	_, err := parseSettle(t, "-settle-parked", "-hour", "2026-09-14T04:00:00Z", "-auth-id", "a", "-owner", "user/u",
		"-resource-id", "r", "-model-id", "m", "-serving-mode", "dedicated", "-parked-reason", "missing_org",
		"-settle-reason", "   ")
	if err == nil || !strings.Contains(err.Error(), "-settle-reason") || !strings.Contains(err.Error(), "-settled-by") {
		t.Fatalf("err = %v, want -settled-by and a blank -settle-reason reported missing", err)
	}
	if _, err := parseSettle(t, "-settle-parked", "-hour", "2026-09-14T04:30:00Z", "-auth-id", "a", "-owner", "-",
		"-resource-id", "r", "-model-id", "m", "-serving-mode", "dedicated", "-parked-reason", "owner_conflict",
		"-settled-by", "ops", "-settle-reason", "x"); err == nil {
		t.Fatal("a mid-hour -hour was accepted")
	}
}

// OWNER "-" in -list-parked is the empty owner (an owner_conflict row).
func TestSettleFlags_OwnerDashIsTheEmptyOwner(t *testing.T) {
	k, err := parseSettle(t, "-settle-parked", "-hour", "2026-09-14T04:00:00Z", "-kind", "video", "-auth-id", "a",
		"-owner", "-", "-resource-id", "r", "-model-id", "m", "-serving-mode", "dedicated",
		"-parked-reason", "owner_conflict", "-settled-by", "ops", "-settle-reason", "x")
	if err != nil {
		t.Fatal(err)
	}
	if k.OwnerType != "" || k.OwnerID != "" || k.Kind != rating.KindVideo {
		t.Fatalf("key %+v", k)
	}
	k, err = parseSettle(t, "-settle-parked", "-hour", "2026-09-14T04:00:00Z", "-auth-id", "a",
		"-owner", "group/tf-1", "-resource-id", "r", "-model-id", "m", "-serving-mode", "dedicated",
		"-parked-reason", "missing_org", "-settled-by", "ops", "-settle-reason", "x")
	if err != nil || k.OwnerType != "group" || k.OwnerID != "tf-1" || k.Kind != rating.KindToken {
		t.Fatalf("key %+v err %v", k, err)
	}
}
