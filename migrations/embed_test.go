package migrations

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"
)

func file() *fstest.MapFile { return &fstest.MapFile{Data: []byte("SELECT 1;")} }

type errFS struct{ fstest.MapFS }

func (errFS) ReadDir(string) ([]fs.DirEntry, error) { return nil, errors.New("boom") }

func TestLatestVersion_ReturnsHighestUpVersion(t *testing.T) {
	got, err := latestVersion(fstest.MapFS{
		"0001_a.up.sql": file(), "0001_a.down.sql": file(),
		"0012_b.up.sql": file(), "0012_b.down.sql": file(),
		"0013_c.down.sql": file(), // a down file alone does not raise the version
		"README.md":       file(),
	})
	if err != nil || got != 12 {
		t.Fatalf("latestVersion = %d, %v; want 12, nil", got, err)
	}
}

// Timestamp-style versions do not fit in 32 bits; they must still count, or
// the gate would accept a schema missing that migration.
func TestLatestVersion_ParsesVersionsWiderThan32Bits(t *testing.T) {
	got, err := latestVersion(fstest.MapFS{
		"0009_a.up.sql":           file(),
		"20261014120000_b.up.sql": file(),
	})
	if err != nil || got != 20261014120000 {
		t.Fatalf("latestVersion = %d, %v; want 20261014120000, nil", got, err)
	}
}

// Every failure returns an error, never 0: a RequiredSchemaVersion of 0
// disables the drainer's schema gate.
func TestLatestVersion_FailsClosedInsteadOfReturningZero(t *testing.T) {
	for name, fsys := range map[string]fs.ReadDirFS{
		"unparseable prefix": fstest.MapFS{"0009_a.up.sql": file(), "V0010__b.up.sql": file()},
		"no underscore":      fstest.MapFS{"0009_a.up.sql": file(), "0010-add_region.up.sql": file()},
		"bad down name":      fstest.MapFS{"0009_a.up.sql": file(), "0009a.down.sql": file()},
		"no up files":        fstest.MapFS{"0001_a.down.sql": file()},
		"empty":              fstest.MapFS{},
		"only version zero":  fstest.MapFS{"0000_a.up.sql": file()},
		"read error":         errFS{},
	} {
		if got, err := latestVersion(fsys); err == nil {
			t.Errorf("%s: latestVersion = %d, nil; want an error", name, got)
		}
	}
}
