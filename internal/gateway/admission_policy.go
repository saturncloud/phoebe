package gateway

import (
	"context"
	"sort"
	"strconv"
	"strings"
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
//     the other entries still apply. The last good values are held in this
//     replica's memory only. A replica that starts while a malformed entry is
//     live has no history, so it enforces the Helm value for that key, and it
//     can differ from replicas that kept a last good value until Atlas writes a
//     valid revision;
//   - a kept last good value never blocks a valid entry of the same revision:
//     if the kept value breaks a per-tier rule against a new valid value (for
//     example a kept maxConcurrentPrefills above a lowered maxActiveRequests),
//     the kept value is lowered to the cap it must not exceed and the revision
//     applies. A kept value is only ever lowered, never raised;
//   - if the entries are each valid but the combination still fails phoebe's
//     cross-field validation (per-tier rules, lane shares), the whole update is
//     refused and the last applied settings stay;
//   - a deleted ConfigMap keeps the last applied settings (Atlas re-creates it).
//
// Each apply, kept value and refusal is logged with the revision. A revision
// with malformed entries is logged as "applied revision ... PARTIALLY", naming
// the malformed keys, and Degraded reports them. Each phoebe replica runs its
// own watcher.
type AdmissionPolicyWatcher struct {
	log      *logging.Logger
	target   PolicyTarget
	base     config.AdmissionSettings
	informer cache.SharedIndexInformer
	reg      cache.ResourceEventHandlerRegistration

	mu        sync.Mutex
	applied   bool             // a policy ConfigMap has been applied; false while the Helm settings apply
	revision  string           // revision of the applied policy; "" also when that policy had no revision
	malformed []string         // sorted keys of the applied revision that were malformed
	lastGood  map[string]int64 // entries applied by the last good update
	seen      string           // revision of the last policy ConfigMap processed, applied or refused
	deletes   int              // policy ConfigMap delete events seen
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
		w.log.Error.Printf("admission policy: watch failed (%v); keeping the settings in force (%s) until it reconnects", err, w.InForce())
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

// Revision is the revision of the policy in force. "" means either that only
// the Helm settings apply or that the applied policy carried no revision;
// InForce tells the two apart.
func (w *AdmissionPolicyWatcher) Revision() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.revision
}

// InForce describes the settings in force for a log line: the quoted revision,
// "policy with no revision", or "helm settings (no policy applied)". A
// partly applied revision is marked "(partial)".
func (w *AdmissionPolicyWatcher) InForce() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.describeLocked()
}

// Degraded returns the sorted keys of the revision in force that were
// malformed and kept their last good or Helm value; empty when the revision
// applied in full.
func (w *AdmissionPolicyWatcher) Degraded() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.malformed...)
}

// describeLocked is InForce with w.mu held.
func (w *AdmissionPolicyWatcher) describeLocked() string {
	var s string
	switch {
	case !w.applied:
		return "helm settings (no policy applied)"
	case w.revision == "":
		s = "policy with no revision"
	default:
		s = "revision " + strconv.Quote(w.revision)
	}
	if len(w.malformed) > 0 {
		s += " (partial)"
	}
	return s
}

func (w *AdmissionPolicyWatcher) seenRevision() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seen
}

func (w *AdmissionPolicyWatcher) deleteCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deletes
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
	w.seen = revision
	if revision == "" {
		w.log.Warn.Printf("admission policy: %s has no %q key; applying it without a revision", cm.Name, AdmissionPolicyRevisionKey)
	}
	values := map[string]int64{}
	var malformed, kept []string
	for key, raw := range cm.Data {
		if key == AdmissionPolicyRevisionKey {
			continue
		}
		path, value, err := config.ParseAdmissionPolicyEntry(key, raw)
		if err != nil {
			malformed = append(malformed, key)
			if last, ok := w.lastGood[key]; ok {
				values[key] = last
				kept = append(kept, key)
				w.log.Error.Printf("admission policy: revision %q: %v; keeping the last good value %d", revision, err, last)
			} else {
				w.log.Error.Printf("admission policy: revision %q: %v; no last good value in this process, the Helm value stays (replicas that kept a last good value may enforce a different cap)", revision, err)
			}
			continue
		}
		values[path] = value
	}
	sort.Strings(malformed)
	next, err := w.base.WithLimitOverrides(values)
	if err != nil && len(kept) > 0 {
		// A kept (older) value may break a per-tier rule against a valid new
		// value of this revision. Lower the kept values to the caps they must
		// not exceed and retry, so one malformed entry cannot block a valid
		// tightening.
		clamped, adjusted := clampKeptValues(w.base, values, kept)
		if len(adjusted) > 0 {
			if retry, rerr := w.base.WithLimitOverrides(clamped); rerr == nil {
				w.log.Error.Printf("admission policy: kept value(s) for %s were lowered so revision %q could apply", strings.Join(adjusted, ", "), revision)
				next, err, values = retry, nil, clamped
			}
		}
	}
	if err != nil {
		w.log.Error.Printf("admission policy: revision %q REFUSED, keeping %s: %v", revision, w.describeLocked(), err)
		return
	}
	w.target.Replace(next)
	w.applied = true
	w.revision = revision
	w.malformed = malformed
	w.lastGood = values
	if len(malformed) == 0 {
		w.log.Info.Printf("admission policy: applied revision %q from %s (%d limits, resourceVersion %s)", revision, cm.Name, len(values), cm.ResourceVersion)
		return
	}
	w.log.Warn.Printf("admission policy: applied revision %q PARTIALLY from %s (%d limits; %d malformed entries kept their last good or Helm value: %s; resourceVersion %s)",
		revision, cm.Name, len(values), len(malformed), strings.Join(malformed, ", "), cm.ResourceVersion)
}

// tierRules are the per-tier rules a kept value may break against a new
// value: the first field cannot exceed the second (AdmissionLimits.parse).
var tierRules = [][2]string{
	{"maxConcurrentPrefills", "maxActiveRequests"},
	{"maxReservedDecodeSlots", "maxActiveRequests"},
	{"uncachedPromptTokensPerWindow", "totalPromptTokensPerWindow"},
}

// clampKeptValues returns a copy of values in which each kept key that is the
// lower side of a per-tier rule is lowered to that tier's effective upper cap
// (the value in values, else the Helm value), and the keys it lowered. It only
// ever lowers a kept value, so the result is never looser than values.
func clampKeptValues(base config.AdmissionSettings, values map[string]int64, kept []string) (map[string]int64, []string) {
	out := make(map[string]int64, len(values))
	for k, v := range values {
		out[k] = v
	}
	var adjusted []string
	for _, key := range kept {
		tier, field, _ := strings.Cut(key, ".")
		for _, rule := range tierRules {
			if field != rule[0] {
				continue
			}
			upper, ok := values[tier+"."+rule[1]]
			if !ok {
				helm := helmLimit(base, tier, rule[1])
				if helm == nil {
					continue
				}
				upper = *helm
			}
			if out[key] > upper {
				out[key] = upper
				adjusted = append(adjusted, key)
			}
		}
	}
	return out, adjusted
}

// helmLimit returns the Helm value of tier.field in base, nil when unset.
func helmLimit(base config.AdmissionSettings, tier, field string) *int64 {
	var limits config.AdmissionLimits
	switch tier {
	case "platform":
		limits = base.Platform
	case "graph":
		limits = base.Graph
	case "organization":
		limits = base.Organization
	case "organizationModel":
		limits = base.OrganizationModel
	default:
		return nil
	}
	for _, f := range config.LimitFields(limits) {
		if f.Name == field {
			return f.Value
		}
	}
	return nil
}

func (w *AdmissionPolicyWatcher) deleted(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == AdmissionPolicyConfigMap {
		w.mu.Lock()
		w.deletes++
		inForce := w.describeLocked()
		w.mu.Unlock()
		w.log.Warn.Printf("admission policy: %s deleted; keeping %s until Atlas writes it again", cm.Name, inForce)
	}
}
