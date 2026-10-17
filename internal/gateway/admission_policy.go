package gateway

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
)

const (
	// AdmissionPolicyConfigMap is the one ConfigMap Atlas writes the shared-tier
	// admin page's gateway limits to (shared-tier-admin Part B), in the
	// gateway namespace next to the served-model registry ConfigMaps.
	AdmissionPolicyConfigMap = "phoebe-admission-policy"
	// AdmissionPolicyLabel selects it server-side. The registry informer's
	// selector does not match it, so the registry never tries to parse it.
	AdmissionPolicyLabel = "saturncloud.io/phoebe-admission-policy"
	// AdmissionPolicyKey is its data key: a YAML document read by
	// config.AdmissionSettings.WithAdmissionPolicy.
	AdmissionPolicyKey = "policy.yaml"
)

// PolicyTarget is the admitter an admission policy is applied to.
type PolicyTarget interface {
	Settings() config.AdmissionSettings
	Replace(config.AdmissionSettings)
}

// AdmissionPolicyWatcher applies the Atlas-written admission-policy ConfigMap
// to the admitter, live and without a restart.
//
// Every update is laid over the Helm-rendered settings the admitter started
// with (never over the previous policy), so a field removed from the policy
// returns to its Helm value. It fails closed: a malformed document is refused
// whole and the admitter keeps the last good settings; a deleted ConfigMap
// also keeps the last good settings (Atlas resets by writing a document with
// no fields). Each phoebe replica runs its own watcher.
type AdmissionPolicyWatcher struct {
	log      *logging.Logger
	target   PolicyTarget
	base     config.AdmissionSettings
	informer cache.SharedIndexInformer

	mu       sync.Mutex
	revision string // revision in force; "" while the Helm settings apply
}

// NewAdmissionPolicyWatcher builds the watcher over client in namespace. The
// target's current settings are the Helm baseline. Call Run to start it.
func NewAdmissionPolicyWatcher(client kubernetes.Interface, namespace string, target PolicyTarget, log *logging.Logger) *AdmissionPolicyWatcher {
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = AdmissionPolicyLabel + "=true"
		}),
	)
	w := &AdmissionPolicyWatcher{
		log:      log,
		target:   target,
		base:     target.Settings(),
		informer: factory.Core().V1().ConfigMaps().Informer(),
	}
	_ = w.informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		w.log.Error.Printf("admission policy: watch failed (%v); keeping the settings in force (revision %q) until it reconnects", err, w.Revision())
	})
	_, _ = w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.apply(obj) },
		UpdateFunc: func(_, obj any) { w.apply(obj) },
		DeleteFunc: func(obj any) { w.deleted(obj) },
	})
	return w
}

// Run starts the informer; it returns when ctx is cancelled.
func (w *AdmissionPolicyWatcher) Run(ctx context.Context) { w.informer.Run(ctx.Done()) }

// Revision is the revision of the policy in force, "" while only the Helm
// settings apply.
func (w *AdmissionPolicyWatcher) Revision() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.revision
}

func (w *AdmissionPolicyWatcher) apply(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok || cm.Name != AdmissionPolicyConfigMap {
		if ok {
			w.log.Warn.Printf("admission policy: ignoring ConfigMap %s/%s (only %s is read)", cm.Namespace, cm.Name, AdmissionPolicyConfigMap)
		}
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	doc, present := cm.Data[AdmissionPolicyKey]
	if !present {
		w.log.Error.Printf("admission policy: %s has no %q key; REFUSED, keeping revision %q", cm.Name, AdmissionPolicyKey, w.revision)
		return
	}
	next, revision, err := w.base.WithAdmissionPolicy([]byte(doc))
	if err != nil {
		w.log.Error.Printf("admission policy: %s (resourceVersion %s) REFUSED, keeping revision %q: %v", cm.Name, cm.ResourceVersion, w.revision, err)
		return
	}
	w.target.Replace(next)
	w.revision = revision
	w.log.Info.Printf("admission policy: applied revision %q from %s (resourceVersion %s)", revision, cm.Name, cm.ResourceVersion)
}

func (w *AdmissionPolicyWatcher) deleted(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == AdmissionPolicyConfigMap {
		w.log.Warn.Printf("admission policy: %s deleted; keeping revision %q (write a document with no fields to return to the Helm values)", cm.Name, w.Revision())
	}
}
