package registry

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const testNS = "main-namespace"

// svc builds a Service with the given name, labels, and ports for the fake clientset.
func svc(name string, labels map[string]string, ports ...corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec:       corev1.ServiceSpec{Ports: ports},
	}
}

func port(name string, p int32) corev1.ServicePort {
	return corev1.ServicePort{Name: name, Port: p}
}

// internalLabels is the model Service's label set: resource-id + service-type=internal.
func internalLabels(id string) map[string]string {
	return map[string]string{labelResourceID: id, labelServiceType: serviceTypeInternal}
}

func newLookup(t *testing.T, objs ...*corev1.Service) LookupFunc {
	t.Helper()
	runtimeObjs := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		runtimeObjs = append(runtimeObjs, o)
	}
	client := fake.NewSimpleClientset(runtimeObjs...)
	lf, err := NewK8sLookup(K8sLookupConfig{Namespace: testNS, Client: client})
	if err != nil {
		t.Fatalf("NewK8sLookup: %v", err)
	}
	return lf
}

func TestK8sLookup_ResolvesInternalService(t *testing.T) {
	lf := newLookup(t,
		svc("pd-abcde-mymodel-r123", internalLabels("r123"), port("8000", 8000)),
	)
	u, err := lf(context.Background(), "r123")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	want := "http://pd-abcde-mymodel-r123.main-namespace.svc.cluster.local:8000"
	if u.String() != want {
		t.Errorf("url = %q, want %q", u.String(), want)
	}
}

// The deployment's ssh Service shares the resource-id label; the service-type
// selector MUST exclude it. If it didn't, this would be an ambiguous match.
func TestK8sLookup_IgnoresSSHService(t *testing.T) {
	lf := newLookup(t,
		svc("pd-abcde-mymodel-r123", internalLabels("r123"), port("8000", 8000)),
		svc("pd-abcde-mymodel-r123-ssh",
			map[string]string{labelResourceID: "r123", labelServiceType: "ssh"},
			port("ssh", 22)),
	)
	u, err := lf(context.Background(), "r123")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if u.Port() != "8000" {
		t.Errorf("resolved the ssh service (port %s), want the internal :8000", u.Port())
	}
}

func TestK8sLookup_UnknownIsErrNotFound(t *testing.T) {
	lf := newLookup(t,
		svc("pd-abcde-other-r999", internalLabels("r999"), port("8000", 8000)),
	)
	_, err := lf(context.Background(), "r123")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound (so the cache negative-caches it)", err)
	}
}

// Two internal Services with the same id is a can't-happen state; if it ever
// occurs we must error (transient, not cached), never silently pick one.
func TestK8sLookup_AmbiguousIsError(t *testing.T) {
	lf := newLookup(t,
		svc("svc-a", internalLabels("r123"), port("8000", 8000)),
		svc("svc-b", internalLabels("r123"), port("8000", 8000)),
	)
	_, err := lf(context.Background(), "r123")
	if err == nil {
		t.Fatal("want an error for ambiguous match, got nil")
	}
	if errors.Is(err, ErrNotFound) {
		t.Error("ambiguous must NOT be ErrNotFound (it must not be negative-cached)")
	}
}

// A single unnamed port is used as-is (defensive: not every Service names 8000).
func TestK8sLookup_SinglePortFallback(t *testing.T) {
	lf := newLookup(t,
		svc("pd-x-r123", internalLabels("r123"), port("", 8000)),
	)
	u, err := lf(context.Background(), "r123")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if u.Port() != "8000" {
		t.Errorf("port = %s, want 8000", u.Port())
	}
}

// Multiple ports without an 8000-named one is an error, not a silent wrong pick.
func TestK8sLookup_MultiPortNoServedIsError(t *testing.T) {
	lf := newLookup(t,
		svc("pd-x-r123", internalLabels("r123"), port("http", 80), port("metrics", 9090)),
	)
	_, err := lf(context.Background(), "r123")
	if err == nil {
		t.Fatal("want an error when no served port (8000) is identifiable, got nil")
	}
}

func TestK8sLookup_PrefersNamed8000OverOthers(t *testing.T) {
	lf := newLookup(t,
		svc("pd-x-r123", internalLabels("r123"), port("metrics", 9090), port("8000", 8000)),
	)
	u, err := lf(context.Background(), "r123")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if u.Port() != "8000" {
		t.Errorf("port = %s, want the named 8000 port", u.Port())
	}
}

func TestNewK8sLookup_RequiresNamespaceAndClient(t *testing.T) {
	if _, err := NewK8sLookup(K8sLookupConfig{Client: fake.NewSimpleClientset()}); err == nil {
		t.Error("want error for empty Namespace")
	}
	if _, err := NewK8sLookup(K8sLookupConfig{Namespace: testNS}); err == nil {
		t.Error("want error for nil Client")
	}
}
