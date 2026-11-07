package proxy

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const runbookPath = "../../docs/billing-reconciliation.md"

// runbookDrainQuery returns the PromQL block in the runbook's long-outage section.
func runbookDrainQuery(t *testing.T) string {
	t.Helper()
	doc, err := os.ReadFile(runbookPath)
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(doc), "### A metering outage longer than a day")
	if !ok {
		t.Fatal("runbook section not found")
	}
	m := regexp.MustCompile("(?s)```promql\n(.*?)```").FindStringSubmatch(section)
	if m == nil {
		t.Fatal("no promql block in the runbook section")
	}
	return strings.Join(strings.Fields(m[1]), " ")
}

// TestRunbookDrainQueryNamesServedMetrics: a renamed metric would make the query
// return nothing forever, so every name in it must be one the scrape serves.
func TestRunbookDrainQueryNamesServedMetrics(t *testing.T) {
	query := runbookDrainQuery(t)
	fams := phoebeFamilies(t, newMetricsTestServer(t).WithMeteringHealth(fakeMeteringHealth{}).metrics)
	names := regexp.MustCompile(`phoebe_[a-z_]+`).FindAllString(query, -1)
	if len(names) == 0 {
		t.Fatalf("no metric names in %q", query)
	}
	for _, name := range names {
		if _, ok := fams[name]; !ok {
			t.Errorf("query reads %s, which the scrape does not serve", name)
		}
	}
}

// TestRunbookDrainQueryTreatsAbsentPendingAsNotDrained evaluates the runbook's query
// with promtool: it returns a sample only for a cleared backlog, and a missing pending
// series or an unknown count gives an empty result. Outside CI it is skipped without
// promtool; in CI (`make test` installs promtool) a missing promtool fails the test.
func TestRunbookDrainQueryTreatsAbsentPendingAsNotDrained(t *testing.T) {
	promtool, err := exec.LookPath("promtool")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("promtool not on PATH in CI; run the tests through `make test`, which installs it")
		}
		t.Skip("promtool not on PATH")
	}
	query := runbookDrainQuery(t)

	// replica holds the values of one replica's series; a negative value for any field
	// means that series is absent.
	type replica struct{ healthy, pending, poisoned, quarantined int }
	cleared := replica{healthy: 1}
	cases := []struct {
		name     string
		replicas []replica
		drained  bool
	}{
		{"all replicas cleared", []replica{cleared, cleared}, true},
		{"no series at all", nil, false},
		{"unknown count on one replica", []replica{cleared, {healthy: 0, pending: -1}}, false},
		{"pending missing while healthy", []replica{cleared, {healthy: 1, pending: -1}}, false},
		{"events pending", []replica{cleared, {healthy: 1, pending: 5}}, false},
		{"poisoned", []replica{cleared, {healthy: 1, poisoned: 1}}, false},
		{"quarantined directory", []replica{cleared, {healthy: 1, quarantined: 1}}, false},
		// The exporter omits quarantined_dirs when the directory cannot be read (an
		// unknown count, -1 from WALQuarantinedDirs), so the operator must not read it as 0.
		{"quarantined missing while healthy", []replica{cleared, {healthy: 1, quarantined: -1}}, false},
	}

	var cfg strings.Builder
	cfg.WriteString("rule_files: []\nevaluation_interval: 1m\ntests:\n")
	for _, c := range cases {
		fmt.Fprintf(&cfg, "  # %s\n  - interval: 1m\n    input_series:\n", c.name)
		if len(c.replicas) == 0 {
			cfg.WriteString("      - series: 'unrelated_series'\n        values: '1'\n")
		}
		for i, r := range c.replicas {
			for metric, v := range map[string]int{
				"phoebe_metering_wal_healthy":          r.healthy,
				"phoebe_metering_wal_pending_events":   r.pending,
				"phoebe_metering_wal_poisoned":         r.poisoned,
				"phoebe_metering_wal_quarantined_dirs": r.quarantined,
			} {
				if v >= 0 {
					fmt.Fprintf(&cfg, "      - series: '%s{pod=\"p%d\"}'\n        values: '%d'\n", metric, i, v)
				}
			}
		}
		fmt.Fprintf(&cfg, "    promql_expr_test:\n      - expr: %q\n        eval_time: 0m\n", query)
		if c.drained {
			cfg.WriteString("        exp_samples:\n          - value: 1\n")
		} else {
			cfg.WriteString("        exp_samples: []\n")
		}
	}
	path := filepath.Join(t.TempDir(), "runbook_test.yml")
	if err := os.WriteFile(path, []byte(cfg.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(promtool, "test", "rules", path).CombinedOutput(); err != nil {
		t.Fatalf("promtool: %v\n%s\nconfig:\n%s", err, out, cfg.String())
	}
}
