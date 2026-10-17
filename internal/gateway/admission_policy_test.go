package gateway

import (
	"context"
	stdlog "log"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/testutil"
)

const policyNS = "main-namespace"

type fakePolicyTarget struct {
	mu       sync.Mutex
	cfg      config.AdmissionSettings
	replaces int
}

func (f *fakePolicyTarget) Settings() config.AdmissionSettings {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cfg
}

func (f *fakePolicyTarget) Replace(cfg config.AdmissionSettings) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfg = cfg
	f.replaces++
}

func (f *fakePolicyTarget) replaceCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.replaces
}

func (f *fakePolicyTarget) orgPrefills() int64 {
	cfg := f.Settings()
	if cfg.Organization.MaxConcurrentPrefills == nil {
		return -1
	}
	return *cfg.Organization.MaxConcurrentPrefills
}

func (f *fakePolicyTarget) orgActive() int64 {
	cfg := f.Settings()
	if cfg.Organization.MaxActiveRequests == nil {
		return -1
	}
	return *cfg.Organization.MaxActiveRequests
}

func policyConfigMap(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: policyNS,
			Labels: map[string]string{AdmissionPolicyLabel: "true"},
		},
		Data: data,
	}
}

// helmPolicyTarget is a target whose Helm baseline caps organization
// maxActiveRequests at 10 and sets nothing else.
func helmPolicyTarget() *fakePolicyTarget {
	return &fakePolicyTarget{cfg: config.AdmissionSettings{
		Enabled:      true,
		Organization: config.AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](10), WindowStr: "1m", Window: time.Minute},
		Platform:     config.AdmissionLimits{WindowStr: "1m", Window: time.Minute},
		Graph:        config.AdmissionLimits{WindowStr: "1m", Window: time.Minute},
		OrganizationModel: config.AdmissionLimits{
			WindowStr: "1m", Window: time.Minute,
		},
	}}
}

// directPolicyWatcher builds a watcher with no informer, so a test calls
// apply and deleted synchronously, and captures every log line.
func directPolicyWatcher() (*fakePolicyTarget, *AdmissionPolicyWatcher, *syncBuffer) {
	target := helmPolicyTarget()
	logs := &syncBuffer{}
	log := logging.New(logging.DEBUG)
	for _, l := range []*stdlog.Logger{log.Debug, log.Info, log.Warn, log.Error} {
		l.SetOutput(logs)
	}
	return target, &AdmissionPolicyWatcher{log: log, target: target, base: target.Settings()}, logs
}

func startPolicyWatcher(t *testing.T) (*fake.Clientset, *fakePolicyTarget, *AdmissionPolicyWatcher) {
	t.Helper()
	target := helmPolicyTarget()
	client := fake.NewSimpleClientset()
	w := NewAdmissionPolicyWatcher(client, policyNS, target, logging.New(logging.ERROR))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go w.Run(ctx)
	return client, target, w
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *fakePolicyTarget) orgColdHolds() int64 {
	cfg := f.Settings()
	if cfg.Organization.MaxColdHolds == nil {
		return -1
	}
	return *cfg.Organization.MaxColdHolds
}

func TestAdmissionPolicyWatcherAppliesUpdatesLiveFromTheHelmBaseline(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	cms := client.CoreV1().ConfigMaps(policyNS)

	cm := policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "r1", "organization.maxActiveRequests": "4", "organization.maxColdHolds": "2",
	})
	if _, err := cms.Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision r1", func() bool {
		return w.Revision() == "r1" && target.orgActive() == 4 && target.orgColdHolds() == 2
	})

	// The next update is laid over the Helm baseline, not over r1: a removed
	// key returns to the Helm value (no cold-hold cap in this baseline).
	cm.Data = map[string]string{"revision": "r2", "organization.maxActiveRequests": "5"}
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision r2", func() bool {
		return w.Revision() == "r2" && target.orgActive() == 5 && target.orgColdHolds() == -1
	})
}

func TestAdmissionPolicyWatcherKeepsTheLastGoodValueOfAMalformedEntry(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	cms := client.CoreV1().ConfigMaps(policyNS)

	cm := policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "good", "organization.maxActiveRequests": "3",
	})
	if _, err := cms.Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision good", func() bool { return w.Revision() == "good" && target.orgActive() == 3 })

	// The malformed entry keeps its last good value (3) while the other entry
	// applies; a malformed entry with no earlier value keeps the Helm value.
	cm.Data = map[string]string{
		"revision": "partly-bad", "organization.maxActiveRequests": "unlimited",
		"organization.maxColdHolds": "2", "organization.maxThings": "1",
	}
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision partly-bad", func() bool {
		return w.Revision() == "partly-bad" && target.orgActive() == 3 && target.orgColdHolds() == 2
	})
	// The revision applied only in part, and the watcher says so.
	if got, want := w.Degraded(), []string{"organization.maxActiveRequests", "organization.maxThings"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Degraded() = %v, want %v", got, want)
	}
	if got := w.InForce(); got != `revision "partly-bad" (partial)` {
		t.Fatalf("InForce() = %q, want the partial revision", got)
	}
}

// TestAdmissionPolicyRefusesACombinationWholeAndKeepsLastApplied calls the
// handler directly: a revision whose entries are valid alone but fail the
// cross-field rules never reaches the target, and the last good entries (what
// a later malformed entry falls back to) are those of the applied revision.
func TestAdmissionPolicyRefusesACombinationWholeAndKeepsLastApplied(t *testing.T) {
	target, w, logs := directPolicyWatcher()
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "good", "organization.maxActiveRequests": "3",
	}))
	// Each entry is valid alone; together the prefill cap exceeds the active cap.
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "bad-combination", "organization.maxActiveRequests": "2",
		"organization.maxConcurrentPrefills": "5",
	}))
	if got := target.replaceCount(); got != 1 {
		t.Fatalf("Replace called %d times, want 1 (the refused revision must not reach the target)", got)
	}
	if w.Revision() != "good" || target.orgActive() != 3 {
		t.Fatalf("a refused combination changed the settings in force: revision %q, org cap %d", w.Revision(), target.orgActive())
	}
	if want := map[string]int64{"organization.maxActiveRequests": 3}; !reflect.DeepEqual(w.lastGood, want) {
		t.Fatalf("lastGood = %v after a refusal, want %v", w.lastGood, want)
	}
	if !strings.Contains(logs.String(), `revision "bad-combination" REFUSED, keeping revision "good"`) {
		t.Fatalf("no refusal line naming the revision kept:\n%s", logs)
	}
}

// TestAdmissionPolicyDeleteKeepsLastApplied: a delete, in either the plain or
// the tombstone form, changes nothing in force.
func TestAdmissionPolicyDeleteKeepsLastApplied(t *testing.T) {
	target, w, _ := directPolicyWatcher()
	cm := policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "good", "organization.maxActiveRequests": "3",
	})
	w.apply(cm)
	w.deleted(cm)
	w.deleted(cache.DeletedFinalStateUnknown{Key: policyNS + "/" + cm.Name, Obj: cm})
	if got := w.deleteCount(); got != 2 {
		t.Fatalf("deleteCount() = %d, want 2", got)
	}
	if target.replaceCount() != 1 || w.Revision() != "good" || target.orgActive() != 3 {
		t.Fatalf("a delete changed the settings in force: replaces %d, revision %q, org cap %d", target.replaceCount(), w.Revision(), target.orgActive())
	}
	if want := map[string]int64{"organization.maxActiveRequests": 3}; !reflect.DeepEqual(w.lastGood, want) {
		t.Fatalf("lastGood = %v after a delete, want %v", w.lastGood, want)
	}
}

// TestAdmissionPolicyWatcherKeepsTheLastGoodValueAcrossARefusedCombination:
// after a refused revision, a malformed entry falls back to the value that
// was last applied (3), never to the refused value (2).
func TestAdmissionPolicyWatcherKeepsTheLastGoodValueAcrossARefusedCombination(t *testing.T) {
	target, w, _ := directPolicyWatcher()
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "good", "organization.maxActiveRequests": "3",
	}))
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "bad-combination", "organization.maxActiveRequests": "2",
		"organization.maxConcurrentPrefills": "5",
	}))
	if w.Revision() != "good" || target.orgActive() != 3 {
		t.Fatalf("refused revision applied: revision %q, org cap %d", w.Revision(), target.orgActive())
	}
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "malformed", "organization.maxActiveRequests": "bad",
	}))
	if w.Revision() != "malformed" || target.orgActive() != 3 {
		t.Fatalf("malformed entry fell back to revision %q, org cap %d; want the last applied cap 3", w.Revision(), target.orgActive())
	}
}

// TestAdmissionPolicyWatcherRefusesAFailedCombination is the informer-level
// form: it waits until the refused revision was processed before asserting.
func TestAdmissionPolicyWatcherRefusesAFailedCombination(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	cms := client.CoreV1().ConfigMaps(policyNS)

	cm := policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "good", "organization.maxActiveRequests": "3",
	})
	if _, err := cms.Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision good", func() bool { return w.Revision() == "good" && target.orgActive() == 3 })

	cm.Data = map[string]string{
		"revision": "bad-combination", "organization.maxActiveRequests": "2",
		"organization.maxConcurrentPrefills": "5",
	}
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the refused revision to be processed", func() bool { return w.seenRevision() == "bad-combination" })
	if w.Revision() != "good" || target.orgActive() != 3 {
		t.Fatalf("a refused combination changed the settings in force: revision %q, org cap %d", w.Revision(), target.orgActive())
	}
}

// TestAdmissionPolicyWatcherKeepsSettingsOnDelete waits for the delete event
// itself before asserting.
func TestAdmissionPolicyWatcherKeepsSettingsOnDelete(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	cms := client.CoreV1().ConfigMaps(policyNS)

	cm := policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "good", "organization.maxActiveRequests": "3",
	})
	if _, err := cms.Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision good", func() bool { return w.Revision() == "good" && target.orgActive() == 3 })
	if err := cms.Delete(ctx, AdmissionPolicyConfigMap, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the delete to be processed", func() bool { return w.deleteCount() == 1 })
	if w.Revision() != "good" || target.orgActive() != 3 {
		t.Fatalf("a delete changed the settings in force: revision %q, org cap %d", w.Revision(), target.orgActive())
	}
}

// TestAdmissionPolicyWatcherIgnoresOtherConfigMaps has a positive control: the
// real policy created after the other ConfigMap is applied, and the informer
// delivers events in order, so the other ConfigMap's event was processed and
// ignored.
func TestAdmissionPolicyWatcherIgnoresOtherConfigMaps(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	cms := client.CoreV1().ConfigMaps(policyNS)
	other := policyConfigMap("someone-else", map[string]string{"revision": "x", "organization.maxActiveRequests": "1"})
	if _, err := cms.Create(ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	sentinelCM := policyConfigMap(AdmissionPolicyConfigMap, map[string]string{"revision": "sentinel", "organization.maxActiveRequests": "7"})
	if _, err := cms.Create(ctx, sentinelCM, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision sentinel", func() bool { return w.Revision() == "sentinel" })
	if got := target.orgActive(); got != 7 {
		t.Fatalf("org cap %d after the sentinel revision, want 7 (1 means the other ConfigMap was applied)", got)
	}

	direct, dw, _ := directPolicyWatcher()
	dw.apply(other)
	if direct.replaceCount() != 0 || dw.Revision() != "" {
		t.Fatalf("a ConfigMap with another name reached the target: replaces %d, revision %q", direct.replaceCount(), dw.Revision())
	}
}

// TestAdmissionPolicyWithoutARevisionIsReportedAsAPolicyNotTheHelmSettings:
// a policy applied with no revision key must not read as "Helm settings" in
// the logs, because its overrides are in force.
func TestAdmissionPolicyWithoutARevisionIsReportedAsAPolicyNotTheHelmSettings(t *testing.T) {
	target, w, logs := directPolicyWatcher()
	if got := w.InForce(); got != "helm settings (no policy applied)" {
		t.Fatalf("InForce() before any apply = %q", got)
	}
	cm := policyConfigMap(AdmissionPolicyConfigMap, map[string]string{"organization.maxActiveRequests": "4"})
	w.apply(cm)
	if target.orgActive() != 4 {
		t.Fatalf("org cap %d, want the policy value 4", target.orgActive())
	}
	if got := w.InForce(); got != "policy with no revision" {
		t.Fatalf("InForce() = %q, want %q", got, "policy with no revision")
	}
	w.deleted(cm)
	out := logs.String()
	if !strings.Contains(out, `has no "revision" key; applying it without a revision`) {
		t.Fatalf("no warning for the missing revision key:\n%s", out)
	}
	if !strings.Contains(out, "deleted; keeping policy with no revision") || strings.Contains(out, "helm settings") {
		t.Fatalf("delete line does not name the revisionless policy:\n%s", out)
	}
}

// TestAdmissionPolicyMalformedEntryWithNoHistoryKeepsTheHelmValue pins the
// documented limitation: last good values are per process, so a fresh watcher
// whose first read holds a malformed entry enforces the Helm value for that
// key (here maxActiveRequests stays 10 and maxColdHolds stays unset) while the
// valid entries apply.
func TestAdmissionPolicyMalformedEntryWithNoHistoryKeepsTheHelmValue(t *testing.T) {
	target, w, logs := directPolicyWatcher()
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "r1", "organization.maxActiveRequests": "abc",
		"organization.maxColdHolds": "0", "graph.maxActiveRequests": "5",
	}))
	if target.orgActive() != 10 || target.orgColdHolds() != -1 {
		t.Fatalf("org maxActiveRequests=%d maxColdHolds=%d, want the Helm values 10 and unset", target.orgActive(), target.orgColdHolds())
	}
	if g := target.Settings().Graph.MaxActiveRequests; g == nil || *g != 5 {
		t.Fatalf("graph maxActiveRequests=%v, want the valid entry 5 applied", g)
	}
	if !strings.Contains(logs.String(), "no last good value in this process, the Helm value stays") {
		t.Fatalf("no per-process fallback line:\n%s", logs)
	}
}

// TestAdmissionPolicyKeptValueDoesNotBlockAValidTighteningInTheSameRevision: a
// kept prefill cap (8) above the new active cap (4) is lowered so the
// tightening applies, instead of the whole revision being refused at 10.
func TestAdmissionPolicyKeptValueDoesNotBlockAValidTighteningInTheSameRevision(t *testing.T) {
	target, w, logs := directPolicyWatcher()
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "r1", "organization.maxActiveRequests": "10", "organization.maxConcurrentPrefills": "8",
	}))
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "r2", "organization.maxActiveRequests": "4", "organization.maxConcurrentPrefills": "bad",
	}))
	if w.Revision() != "r2" || target.orgActive() != 4 {
		t.Fatalf("revision %q org cap %d, want r2 applied with 4", w.Revision(), target.orgActive())
	}
	if p := target.orgPrefills(); p < 1 || p > 4 {
		t.Fatalf("org maxConcurrentPrefills=%d, want the kept value lowered to <= 4", p)
	}
	if w.lastGood["organization.maxConcurrentPrefills"] != target.orgPrefills() {
		t.Fatalf("lastGood %v does not hold the value applied", w.lastGood)
	}
	if !strings.Contains(logs.String(), `kept value(s) for organization.maxConcurrentPrefills were lowered so revision "r2" could apply`) {
		t.Fatalf("no log line for the lowered kept value:\n%s", logs)
	}
}

// TestAdmissionPolicyLogsAPartlyAppliedRevisionAsPartial: a revision with a
// malformed entry is logged PARTIALLY, naming the key; a clean one is logged
// as plainly applied.
func TestAdmissionPolicyLogsAPartlyAppliedRevisionAsPartial(t *testing.T) {
	_, w, logs := directPolicyWatcher()
	w.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "r42", "organization.maxActiveRequests": "4x", "organization.maxColdHolds": "2",
	}))
	out := logs.String()
	if !strings.Contains(out, `applied revision "r42" PARTIALLY`) || !strings.Contains(out, "organization.maxActiveRequests") {
		t.Fatalf("no PARTIALLY line naming the malformed key:\n%s", out)
	}
	if got := w.Degraded(); !reflect.DeepEqual(got, []string{"organization.maxActiveRequests"}) {
		t.Fatalf("Degraded() = %v", got)
	}

	_, clean, cleanLogs := directPolicyWatcher()
	clean.apply(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "r43", "organization.maxActiveRequests": "4",
	}))
	out = cleanLogs.String()
	if !strings.Contains(out, `applied revision "r43" from`) || strings.Contains(out, "PARTIALLY") {
		t.Fatalf("a clean revision was not logged as plainly applied:\n%s", out)
	}
	if got := clean.Degraded(); len(got) != 0 {
		t.Fatalf("Degraded() = %v for a clean revision", got)
	}
}

// TestAdmissionPolicyWaitSyncedMeansTheExistingPolicyIsApplied guards the
// startup contract: once WaitSynced returns true, a ConfigMap that already
// existed has been applied to the target, so a restarted replica serves the
// admin caps rather than the Helm values. No polling after the wait.
func TestAdmissionPolicyWaitSyncedMeansTheExistingPolicyIsApplied(t *testing.T) {
	target := &fakePolicyTarget{cfg: config.AdmissionSettings{
		Enabled:           true,
		Organization:      config.AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](10), WindowStr: "1m", Window: time.Minute},
		Platform:          config.AdmissionLimits{WindowStr: "1m", Window: time.Minute},
		Graph:             config.AdmissionLimits{WindowStr: "1m", Window: time.Minute},
		OrganizationModel: config.AdmissionLimits{WindowStr: "1m", Window: time.Minute},
	}}
	client := fake.NewSimpleClientset(policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "r7", "organization.maxActiveRequests": "4",
	}))
	w := NewAdmissionPolicyWatcher(client, policyNS, target, logging.New(logging.ERROR))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go w.Run(ctx)

	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	if !w.WaitSynced(wctx) {
		t.Fatal("WaitSynced returned false with the ConfigMap present")
	}
	if got := target.orgActive(); got != 4 || w.Revision() != "r7" {
		t.Fatalf("after WaitSynced: org maxActiveRequests=%d revision=%q, want 4 and r7", got, w.Revision())
	}
}
