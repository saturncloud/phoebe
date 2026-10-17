package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/testutil"
)

const policyNS = "main-namespace"

type fakePolicyTarget struct {
	mu  sync.Mutex
	cfg config.AdmissionSettings
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

func startPolicyWatcher(t *testing.T) (*fake.Clientset, *fakePolicyTarget, *AdmissionPolicyWatcher) {
	t.Helper()
	target := &fakePolicyTarget{cfg: config.AdmissionSettings{
		Enabled:      true,
		Organization: config.AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](10), WindowStr: "1m", Window: time.Minute},
		Platform:     config.AdmissionLimits{WindowStr: "1m", Window: time.Minute},
		Graph:        config.AdmissionLimits{WindowStr: "1m", Window: time.Minute},
		OrganizationModel: config.AdmissionLimits{
			WindowStr: "1m", Window: time.Minute,
		},
	}}
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
}

func TestAdmissionPolicyWatcherKeepsTheLastAppliedSettingsOnAFailedCombinationOrDelete(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	cms := client.CoreV1().ConfigMaps(policyNS)

	cm := policyConfigMap(AdmissionPolicyConfigMap, map[string]string{
		"revision": "good", "organization.maxActiveRequests": "3",
	})
	if _, err := cms.Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision good", func() bool { return w.Revision() == "good" })

	// Each entry is valid alone; together the prefill cap exceeds the active cap.
	cm.Data = map[string]string{
		"revision": "bad-combination", "organization.maxActiveRequests": "2",
		"organization.maxConcurrentPrefills": "5",
	}
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := cms.Delete(ctx, AdmissionPolicyConfigMap, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // let every event reach the handler
	if w.Revision() != "good" || target.orgActive() != 3 {
		t.Fatalf("a refused combination or a delete changed the settings in force: revision %q, org cap %d", w.Revision(), target.orgActive())
	}
}

func TestAdmissionPolicyWatcherIgnoresOtherConfigMaps(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	other := policyConfigMap("someone-else", map[string]string{"revision": "x", "organization.maxActiveRequests": "1"})
	if _, err := client.CoreV1().ConfigMaps(policyNS).Create(ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if w.Revision() != "" || target.orgActive() != 10 {
		t.Fatalf("a ConfigMap with another name was applied: revision %q, org cap %d", w.Revision(), target.orgActive())
	}
}
