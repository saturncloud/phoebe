package prune

import (
	"strings"
	"testing"
)

func TestConfigWithDefaults(t *testing.T) {
	c := Config{}.WithDefaults()
	if c.BillingEventRetentionDays != DefaultBillingEventRetentionDays {
		t.Fatalf("billing_event default = %d, want %d", c.BillingEventRetentionDays, DefaultBillingEventRetentionDays)
	}
	if c.IoLogRetentionDays != DefaultIoLogRetentionDays {
		t.Fatalf("io_log default = %d, want %d", c.IoLogRetentionDays, DefaultIoLogRetentionDays)
	}
	if c.BatchSize != DefaultBatchSize {
		t.Fatalf("batchSize default = %d, want %d", c.BatchSize, DefaultBatchSize)
	}
	// Explicit values survive.
	c = Config{BillingEventRetentionDays: 45, IoLogRetentionDays: 3, BatchSize: 100}.WithDefaults()
	if c.BillingEventRetentionDays != 45 || c.IoLogRetentionDays != 3 || c.BatchSize != 100 {
		t.Fatalf("explicit values clobbered: %+v", c)
	}
}

func TestValidateFloors(t *testing.T) {
	// Below the billing_event hard floor: the money-protecting guard.
	bad := Config{BillingEventRetentionDays: MinBillingEventRetentionDays - 1, IoLogRetentionDays: 7, BatchSize: 100}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "hard floor") {
		t.Fatalf("expected hard-floor error, got %v", err)
	}
	// Below the io_log floor.
	bad = Config{BillingEventRetentionDays: 30, IoLogRetentionDays: 0, BatchSize: 100}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "ioLog") {
		t.Fatalf("expected io_log floor error, got %v", err)
	}
	// Bad batch size.
	bad = Config{BillingEventRetentionDays: 30, IoLogRetentionDays: 7, BatchSize: 0}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "batchSize") {
		t.Fatalf("expected batchSize error, got %v", err)
	}
	// At the floors: valid.
	ok := Config{BillingEventRetentionDays: MinBillingEventRetentionDays, IoLogRetentionDays: MinIoLogRetentionDays, BatchSize: 1}
	if err := ok.Validate(); err != nil {
		t.Fatalf("floor values must be valid: %v", err)
	}
}

func TestTableNameIsClosed(t *testing.T) {
	if BillingEvent.Name() != "billing_event" || IoLog.Name() != "io_log" {
		t.Fatalf("table names drifted: %q %q", BillingEvent.Name(), IoLog.Name())
	}
	if Table(99).minRetentionDays() != 1 {
		t.Fatalf("unknown table must fall back to the conservative floor")
	}
}
