package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/saturncloud/phoebe/internal/identity"
)

// DEDICATED ENDPOINT ROWS (issue #71, tenant-domains design 4.5; contract of
// record: saturn-architecture-docs agent-wip/tf-tenant-domains-build.md,
// "Registry rows for dedicated endpoints"). Atlas writes one ConfigMap per
// deployed dedicated Token Factory endpoint, in the same workload namespace
// and with the same informer label as the shared rows, so the one registry
// informer sees both tiers:
//
//	Name:      tf-endpoint-<deployment_id>
//	Labels:    saturncloud.io/tf-served-model: "true" (the informer selector)
//	           saturncloud.io/tf-endpoint: "true"   (marks a dedicated row)
//	           saturncloud.io/resource-id: <deployment_id>
//	Data keys: serving_mode          — exactly "dedicated" (required)
//	           org_id                — the endpoint's org (required)
//	           served_model_name     — the endpoint name, byte-exact (required)
//	           served_model_aliases  — comma list (legacy <name>-base); may be empty
//	           resource_id           — the deployment id (required)
//	           base_model            — the price key (required)
//	           adapter               — checkpoint artifact id; may be empty
//	           upstream              — host:port; set on the serving cluster's row
//	           forward_url           — https://<label>-inference.<serving cluster
//	                                   domain>; set only on a copy written to
//	                                   another cluster. Exactly one of upstream
//	                                   and forward_url is set.
//	           serving_profile       — as X-Saturn-Serving-Profile
//	           video_limits          — compact JSON, or empty (chat endpoint)
//	           state                 — "running" | "stopped" (required)
//	           cluster               — the serving cluster's name (informational)
//	           org_host_url          — https://<label>-inference.<install domain>/v1,
//	                                   named in the shared host's wrong_endpoint_host
//
// A dedicated row is NEVER returned by Resolve (the shared-tier lookup) and a
// shared row never by ResolveDedicated: the host decides the tier, and the
// tier decides which rows can serve.

// DedicatedEndpointLabel marks a registry ConfigMap as a dedicated endpoint row.
const DedicatedEndpointLabel = "saturncloud.io/tf-endpoint"

// Endpoint states a dedicated row may carry. Scaled to zero is "running".
const (
	EndpointStateRunning = "running"
	EndpointStateStopped = "stopped"
)

// DedicatedEndpoint is one dedicated endpoint's registry row.
type DedicatedEndpoint struct {
	OrgID          string
	Name           string   // served_model_name: the endpoint name
	Aliases        []string // served_model_aliases
	ResourceID     string
	BaseModel      string
	Adapter        string
	Upstream       string // host:port; empty on a forwarding copy
	ForwardURL     string // https://host; empty on the serving cluster's row
	ServingProfile string
	VideoLimits    string
	State          string
	Cluster        string
	OrgHostURL     string
}

// Forwards reports whether this row is a copy on an entry cluster that must
// forward to the serving cluster.
func (e DedicatedEndpoint) Forwards() bool { return e.ForwardURL != "" }

// DedicatedResolver resolves org-host requests. The RegistryResolver
// implements it; the deprecated Postgres resolver does not, and an
// interceptor whose resolver does not implement it refuses org-host
// requests (fail closed).
type DedicatedResolver interface {
	// ResolveDedicated returns the org's dedicated endpoint whose name (or,
	// failing that, an alias) is exactly model; ErrNotFound when there is
	// none; ErrNotSynced before the first registry sync.
	ResolveDedicated(ctx context.Context, orgID, model string) (DedicatedEndpoint, error)
	// ListDedicated returns every dedicated endpoint row of the org, sorted by
	// name. ErrNotSynced before the first sync.
	ListDedicated(ctx context.Context, orgID string) ([]DedicatedEndpoint, error)
}

var _ DedicatedResolver = (*RegistryResolver)(nil)

// isDedicatedEndpointConfigMap reports whether cm is a dedicated endpoint row.
func isDedicatedEndpointConfigMap(cm *corev1.ConfigMap) bool {
	return cm.Labels[DedicatedEndpointLabel] == "true"
}

// parseDedicatedConfigMap validates a dedicated endpoint row. Every refusal is
// fail closed: the row is not indexed, so its endpoint answers
// model_not_found on the org host until Atlas writes a valid row.
func parseDedicatedConfigMap(cm *corev1.ConfigMap) (DedicatedEndpoint, error) {
	d := cm.Data
	for _, req := range []string{"org_id", "served_model_name", "resource_id", "base_model", "state"} {
		if d[req] == "" {
			return DedicatedEndpoint{}, fmt.Errorf("missing required data key %q", req)
		}
	}
	if d["serving_mode"] != identity.ServingModeDedicated {
		return DedicatedEndpoint{}, fmt.Errorf("dedicated endpoint row with serving_mode %q (must be %q)",
			d["serving_mode"], identity.ServingModeDedicated)
	}
	switch d["state"] {
	case EndpointStateRunning, EndpointStateStopped:
	default:
		return DedicatedEndpoint{}, fmt.Errorf("invalid state %q (must be %q or %q)",
			d["state"], EndpointStateRunning, EndpointStateStopped)
	}
	upstream := strings.TrimSpace(d["upstream"])
	forward := strings.TrimSpace(d["forward_url"])
	switch {
	case upstream != "" && forward != "":
		return DedicatedEndpoint{}, errors.New("row sets both upstream and forward_url (exactly one is allowed)")
	case upstream == "" && forward == "":
		return DedicatedEndpoint{}, errors.New("row sets neither upstream nor forward_url")
	}
	if forward != "" {
		if err := ValidForwardURL(forward); err != nil {
			return DedicatedEndpoint{}, err
		}
	}
	ep := DedicatedEndpoint{
		OrgID:          d["org_id"],
		Name:           d["served_model_name"],
		ResourceID:     d["resource_id"],
		BaseModel:      d["base_model"],
		Adapter:        d["adapter"],
		Upstream:       upstream,
		ForwardURL:     strings.TrimRight(forward, "/"),
		ServingProfile: d["serving_profile"],
		VideoLimits:    d["video_limits"],
		State:          d["state"],
		Cluster:        d["cluster"],
		OrgHostURL:     d["org_host_url"],
	}
	seen := map[string]bool{ep.Name: true}
	for _, a := range strings.Split(d["served_model_aliases"], ",") {
		a = strings.TrimSpace(a)
		if a == "" || seen[a] {
			continue
		}
		seen[a] = true
		ep.Aliases = append(ep.Aliases, a)
	}
	return ep, nil
}

// ValidForwardURL checks a forward_url: https, a host, and nothing else (no
// userinfo, path, query or fragment). The entry cluster only ever forwards to
// the base it names plus the request's own path.
func ValidForwardURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("unparseable forward_url: %w", err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("forward_url scheme %q (must be https)", u.Scheme)
	}
	if u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("forward_url %q must be https://host[:port] only", raw)
	}
	return nil
}

// dedicatedIndex holds the dedicated rows. byName and byAlias are keyed by
// (org, served name); a ConfigMap claims its name key and each alias key.
type dedicatedIndex struct {
	byName  map[cacheKey]map[string]DedicatedEndpoint
	byAlias map[cacheKey]map[string]DedicatedEndpoint
	byOrg   map[string]map[string]DedicatedEndpoint
	rows    map[string]DedicatedEndpoint // ConfigMap name -> row
}

func newDedicatedIndex() dedicatedIndex {
	return dedicatedIndex{
		byName:  map[cacheKey]map[string]DedicatedEndpoint{},
		byAlias: map[cacheKey]map[string]DedicatedEndpoint{},
		byOrg:   map[string]map[string]DedicatedEndpoint{},
		rows:    map[string]DedicatedEndpoint{},
	}
}

func addClaim(m map[cacheKey]map[string]DedicatedEndpoint, k cacheKey, cm string, ep DedicatedEndpoint) {
	if m[k] == nil {
		m[k] = map[string]DedicatedEndpoint{}
	}
	m[k][cm] = ep
}

func dropClaim(m map[cacheKey]map[string]DedicatedEndpoint, k cacheKey, cm string) {
	if claims := m[k]; claims != nil {
		delete(claims, cm)
		if len(claims) == 0 {
			delete(m, k)
		}
	}
}

func (x *dedicatedIndex) put(cm string, ep DedicatedEndpoint) {
	x.drop(cm)
	x.rows[cm] = ep
	addClaim(x.byName, cacheKey{org: ep.OrgID, model: ep.Name}, cm, ep)
	for _, a := range ep.Aliases {
		addClaim(x.byAlias, cacheKey{org: ep.OrgID, model: a}, cm, ep)
	}
	if x.byOrg[ep.OrgID] == nil {
		x.byOrg[ep.OrgID] = map[string]DedicatedEndpoint{}
	}
	x.byOrg[ep.OrgID][cm] = ep
}

func (x *dedicatedIndex) drop(cm string) bool {
	ep, ok := x.rows[cm]
	if !ok {
		return false
	}
	delete(x.rows, cm)
	dropClaim(x.byName, cacheKey{org: ep.OrgID, model: ep.Name}, cm)
	for _, a := range ep.Aliases {
		dropClaim(x.byAlias, cacheKey{org: ep.OrgID, model: a}, cm)
	}
	if rows := x.byOrg[ep.OrgID]; rows != nil {
		delete(rows, cm)
		if len(rows) == 0 {
			delete(x.byOrg, ep.OrgID)
		}
	}
	return true
}

// ErrAmbiguousDedicated reports that two different endpoints of one org claim
// the same served name. Atlas guarantees names are unique per org, so this
// only happens when a deleted endpoint's row outlived its delete and the
// name was reused before the registry sweep removed the stale row. Serving
// either could send a request to the wrong (possibly stale) endpoint, so the
// proxy refuses with 503 (it treats every resolver error that way) until the
// sweep heals the registry.
var ErrAmbiguousDedicated = errors.New("gateway: several dedicated endpoints claim this name")

// pickClaim returns the one endpoint behind claims. Several ConfigMaps of
// the SAME endpoint (resource id) are one claimant and resolve to the row
// with the lowest ConfigMap name, so every replica agrees; claims by two
// different endpoints are ambiguous.
func pickClaim(claims map[string]DedicatedEndpoint) (DedicatedEndpoint, int, bool) {
	names := make([]string, 0, len(claims))
	for n := range claims {
		names = append(names, n)
	}
	sort.Strings(names)
	first := claims[names[0]]
	for _, n := range names[1:] {
		if claims[n].ResourceID != first.ResourceID {
			return DedicatedEndpoint{}, len(names), false
		}
	}
	return first, len(names), true
}

// ResolveDedicated implements DedicatedResolver from memory. An exact
// endpoint name wins over an alias (decision 6), so a legacy endpoint's
// <name>-base alias never shadows another endpoint actually named that.
func (r *RegistryResolver) ResolveDedicated(_ context.Context, orgID, model string) (DedicatedEndpoint, error) {
	if !r.synced.Load() {
		return DedicatedEndpoint{}, ErrNotSynced
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	k := cacheKey{org: orgID, model: model}
	for _, m := range []map[cacheKey]map[string]DedicatedEndpoint{r.dedicated.byName, r.dedicated.byAlias} {
		if claims := m[k]; len(claims) > 0 {
			ep, n, ok := pickClaim(claims)
			if !ok {
				r.log.Error.Printf("gateway: %d dedicated registry rows of different endpoints claim (org=%s, model=%q) — Atlas uniqueness violated (a stale row?); refusing until the registry sweep removes it",
					n, orgID, model)
				return DedicatedEndpoint{}, ErrAmbiguousDedicated
			}
			return ep, nil
		}
	}
	return DedicatedEndpoint{}, ErrNotFound
}

// ListDedicated implements DedicatedResolver: the org's rows sorted by name
// (then resource id). Pure memory read.
func (r *RegistryResolver) ListDedicated(_ context.Context, orgID string) ([]DedicatedEndpoint, error) {
	if !r.synced.Load() {
		return nil, ErrNotSynced
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	rows := r.dedicated.byOrg[orgID]
	out := make([]DedicatedEndpoint, 0, len(rows))
	for _, ep := range rows {
		out = append(out, ep)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].ResourceID < out[j].ResourceID
	})
	return out, nil
}

// upsertDedicatedLocked indexes (or, when invalid, de-indexes) a dedicated
// row. Caller holds mu.
func (r *RegistryResolver) upsertDedicatedLocked(cm *corev1.ConfigMap) {
	ep, err := parseDedicatedConfigMap(cm)
	if err != nil {
		r.dedicated.drop(cm.Name)
		r.log.Error.Printf("gateway: ignoring dedicated registry ConfigMap %s/%s: %v", cm.Namespace, cm.Name, err)
		return
	}
	r.dedicated.put(cm.Name, ep)
}
