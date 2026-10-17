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
	// admin page's gateway limits to (shared-tier-admin Part B contracts), in
	// the gateway namespace next to the served-model registry ConfigMaps.
	AdmissionPolicyConfigMap = "tf-admission-policy"
	// AdmissionPolicyLabel selects it server-side. The registry informer's
	// selector does not match it, so the registry never tries to parse it.
	AdmissionPolicyLabel = "saturncloud.io/tf-admission-policy"
	// AdmissionPolicyRevisionKey is the data key carrying Atlas's settings
	// revision; every other key is "<tier>.<field>" with a decimal value
	// (config.ParseAdmissionPolicyEntry).
	AdmissionPolicyRevisionKey = "revision"
)

// PolicyTarget is the admitter an admission policy is applied to.
type PolicyTarget interface {
	Settings() config.AdmissionSettings
	Replace(config.AdmissionSettings)
}

// AdmissionPolicyWatcher applies the Atlas-written admission-policy ConfigMap
// to the admitter, live and without a restart.
//
// Each data key "<tier>.<field>" replaces that Helm value; an absent key keeps
// the Helm value. Every update is laid over the Helm settings the admitter
// started with (never over the previous update). It fails closed, never open:
//   - a malformed entry (unknown key, value not an integer >= 1) keeps the last
//     good value for that key, or the Helm value if there never was one, and
//     the other entries still apply;
//   - if the entries are each valid but the combination fails phoebe's
//     cross-field validation (per-tier rules, lane shares), the whole update is
//     refused and the last applied settings stay;
//   - a deleted ConfigMap keeps the last applied settings (Atlas re-creates it).
//
// Each apply, kept value and refusal is logged with the revision. Each phoebe
// replica runs its own watcher.
type AdmissionPolicyWatcher struct {
	log      *logging.Logger
	target   PolicyTarget
	base     config.AdmissionSettings
	informer cache.SharedIndexInformer
	reg      cache.ResourceEventHandlerRegistration

	mu       sync.Mutex
	revision string           // revision in force; "" while the Helm settings apply
	lastGood map[string]int64 // entries applied by the last good update
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
	reg, err := w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.apply(obj) },
		UpdateFunc: func(_, obj any) { w.apply(obj) },
		DeleteFunc: func(obj any) { w.deleted(obj) },
	})
	if err != nil {
		w.log.Error.Printf("admission policy: registering the policy handler failed (%v); the Helm admission values stay in force", err)
	} else {
		w.reg = reg
	}
	return w
}

// Run starts the informer; it returns when ctx is cancelled.
func (w *AdmissionPolicyWatcher) Run(ctx context.Context) { w.informer.Run(ctx.Done()) }

// WaitSynced blocks until the event handler has applied the informer's initial
// list or ctx is done, and reports whether it synced. It waits on the handler
// registration, not the informer: the informer reports synced once its store
// is filled, possibly before apply has run for the existing ConfigMap.
func (w *AdmissionPolicyWatcher) WaitSynced(ctx context.Context) bool {
	if w.reg == nil {
		return false
	}
	return cache.WaitForCacheSync(ctx.Done(), w.reg.HasSynced)
}

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
	revision := cm.Data[AdmissionPolicyRevisionKey]
	values := map[string]int64{}
	for key, raw := range cm.Data {
		if key == AdmissionPolicyRevisionKey {
			continue
		}
		path, value, err := config.ParseAdmissionPolicyEntry(key, raw)
		if err != nil {
			if last, kept := w.lastGood[key]; kept {
				values[key] = last
				w.log.Error.Printf("admission policy: revision %q: %v; keeping the last good value %d", revision, err, last)
			} else {
				w.log.Error.Printf("admission policy: revision %q: %v; the Helm value stays", revision, err)
			}
			continue
		}
		values[path] = value
	}
	next, err := w.base.WithLimitOverrides(values)
	if err != nil {
		w.log.Error.Printf("admission policy: revision %q REFUSED, keeping revision %q: %v", revision, w.revision, err)
		return
	}
	w.target.Replace(next)
	w.revision = revision
	w.lastGood = values
	w.log.Info.Printf("admission policy: applied revision %q from %s (%d limits, resourceVersion %s)", revision, cm.Name, len(values), cm.ResourceVersion)
}

func (w *AdmissionPolicyWatcher) deleted(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == AdmissionPolicyConfigMap {
		w.log.Warn.Printf("admission policy: %s deleted; keeping revision %q until Atlas writes it again", cm.Name, w.Revision())
	}
}
