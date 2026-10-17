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

func policyConfigMap(name, doc string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: policyNS,
			Labels: map[string]string{AdmissionPolicyLabel: "true"},
		},
		Data: map[string]string{AdmissionPolicyKey: doc},
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

func TestAdmissionPolicyWatcherAppliesUpdatesLiveFromTheHelmBaseline(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	cms := client.CoreV1().ConfigMaps(policyNS)

	cm := policyConfigMap(AdmissionPolicyConfigMap, "version: 1\nrevision: r1\norganization:\n  maxActiveRequests: 4\n")
	if _, err := cms.Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision r1", func() bool { return w.Revision() == "r1" && target.orgActive() == 4 })

	// The next update is laid over the Helm baseline, not over r1: removing
	// the field returns it to the Helm value.
	cm.Data[AdmissionPolicyKey] = "version: 1\nrevision: r2\n"
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision r2", func() bool { return w.Revision() == "r2" && target.orgActive() == 10 })
}

func TestAdmissionPolicyWatcherKeepsTheLastGoodSettingsOnAMalformedUpdateOrDelete(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	cms := client.CoreV1().ConfigMaps(policyNS)

	cm := policyConfigMap(AdmissionPolicyConfigMap, "version: 1\nrevision: good\norganization:\n  maxActiveRequests: 3\n")
	if _, err := cms.Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision good", func() bool { return w.Revision() == "good" })

	for _, bad := range []string{
		"version: 1\nrevision: bad\norganization:\n  maxActiveRequests: null\n",
		"version: 9\nrevision: bad\n",
		"not: [yaml",
	} {
		cm.Data[AdmissionPolicyKey] = bad
		if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	delete(cm.Data, AdmissionPolicyKey)
	if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := cms.Delete(ctx, AdmissionPolicyConfigMap, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // let every event reach the handler
	if w.Revision() != "good" || target.orgActive() != 3 {
		t.Fatalf("a bad update or a delete changed the settings in force: revision %q, org cap %d", w.Revision(), target.orgActive())
	}
}

func TestAdmissionPolicyWatcherIgnoresOtherConfigMaps(t *testing.T) {
	client, target, w := startPolicyWatcher(t)
	ctx := context.Background()
	other := policyConfigMap("someone-else", "version: 1\nrevision: x\norganization:\n  maxActiveRequests: 1\n")
	if _, err := client.CoreV1().ConfigMaps(policyNS).Create(ctx, other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if w.Revision() != "" || target.orgActive() != 10 {
		t.Fatalf("a ConfigMap with another name was applied: revision %q, org cap %d", w.Revision(), target.orgActive())
	}
}
