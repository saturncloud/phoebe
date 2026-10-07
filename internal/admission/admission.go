// Package admission implements Saturn-owned, distributed HTTP admission for
// shared Token Factory inference. It protects physical serving capacity; it is
// deliberately independent from contractual product limits and billing usage.
package admission

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
)

var ErrUnavailable = errors.New("distributed admission state unavailable")

// valkeyOOMPrefix starts the error Valkey returns for a write refused past
// maxmemory under noeviction ("OOM command not allowed when used memory >
// 'maxmemory'.").
const valkeyOOMPrefix = "OOM "

// IsStoreOutOfMemory reports whether an admission error was caused by the
// store refusing writes because it is at its memory cap. The store error is
// carried as text inside ErrUnavailable, so this checks for the Valkey OOM
// reply at the start of the error or of any wrapped segment.
func IsStoreOutOfMemory(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.HasPrefix(msg, valkeyOOMPrefix) || strings.Contains(msg, ": "+valkeyOOMPrefix)
}

// ErrInvalidIdentity marks a broken trusted identity contract (a missing
// organization, model, graph, or required owner). It is deliberately distinct
// from ErrUnavailable: an unavailable fairness store bypasses soft limits for
// otherwise valid traffic, while broken trusted identity must fail closed.
var ErrInvalidIdentity = errors.New("trusted admission identity invalid")

const (
	// Admission is deliberately a soft, fail-open gate. Keep its network budget
	// far below an inference request's latency budget so a blackholed Valkey
	// cannot turn loss of fairness state into loss of inference availability.
	valkeyIOTimeout      = 100 * time.Millisecond
	admitOperationBudget = 750 * time.Millisecond
)

// Rejected is a capacity/policy rejection, as distinct from state failure.
type Rejected struct {
	Scope       string
	Dimension   string
	RetryAfter  time.Duration
	Contractual bool
	// Unsatisfiable marks a contract rejection that no amount of waiting can
	// clear: the single request alone reserves more than the scope's whole
	// per-window limit (for example max_tokens above the generated-token
	// limit). RetryAfter is zero; the proxy answers 400 without Retry-After.
	Unsatisfiable bool
}

func (r *Rejected) Error() string {
	return fmt.Sprintf("admission limit %s exceeded at %s scope", r.Dimension, r.Scope)
}

type Request struct {
	Graph                string
	Organization         string
	Owner                string
	Model                string
	PromptBytes          int64
	EstimatedInputTokens int64
	ReservedOutputTokens int64
	Adapter              bool
	OrganizationLimits   RateLimits
	OwnerLimits          RateLimits
	// GroupScopes is the parsed membership-aware group quota envelope
	// (X-Saturn-Group-Scopes, ruled 2026-10-07): one entry per group the
	// caller belongs to that carries limits. Each entry's Limits enforce the
	// four per-minute rate windows as a contract scope (the same Lua/counter
	// machinery as the contract_owner scope, settling through the same
	// lease/CompleteUsage path); SpendCap, when set, enforces the group's
	// monthly spend cap against the rater's group_usage rollup. Empty when the
	// envelope carried no group (the common case).
	GroupScopes []GroupScope
}

// GroupScope is one group's quota contract within a Request: the group's id,
// its four per-minute rate limits (R4 sentinels: nil unlimited, 0 zero cap),
// and its monthly spend cap as a plain decimal NUMERIC(20,9) string straight
// from the trusted envelope ("" = no cap). The cap is compared in Postgres,
// never as a Go number.
type GroupScope struct {
	GroupID  string
	Limits   RateLimits
	SpendCap string
}

// RateLimits is the authenticated customer contract. R4 sentinel semantics
// (the Saturn UsageLimit pattern): a nil field is unlimited; an explicit 0 is
// a zero cap that blocks every request (fail-closed). There is no 0-sentinel
// for unlimited. Every value is per minute; total prompt contains the
// uncached subset.
type RateLimits struct {
	Requests             *int64
	TotalPromptTokens    *int64
	UncachedPromptTokens *int64
	GeneratedTokens      *int64
}

type scope struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	Active              int64  `json:"active"`
	Prefills            int64  `json:"prefills"`
	ReservedDecodeSlots int64  `json:"reserved_decode_slots"`
	Prompt              int64  `json:"prompt"`
	Output              int64  `json:"output"`
	Adapters            int64  `json:"adapters"`
	Requests            int64  `json:"requests"`
	TotalPrompt         int64  `json:"total_prompt"`
	UncachedPrompt      int64  `json:"uncached_prompt"`
	Generated           int64  `json:"generated"`
	Cold                int64  `json:"cold"`
	Wakes               int64  `json:"wakes"`
	WindowMs            int64  `json:"window_ms"`
	Contractual         bool   `json:"contractual"`
}

type wireRequest struct {
	ID             string  `json:"id"`
	LeaseMs        int64   `json:"lease_ms"`
	Prompt         int64   `json:"prompt"`
	EstimatedInput int64   `json:"estimated_input"`
	Output         int64   `json:"output"`
	Adapter        int64   `json:"adapter"`
	Scopes         []scope `json:"scopes"`
}

// Admitter is the proxy-facing seam, allowing deterministic lifecycle tests.
type Admitter interface {
	Admit(context.Context, Request) (*Lease, error)
}

type RedisAdmitter struct {
	client                     redis.Cmdable
	cfg                        config.AdmissionSettings
	counters, leases, expiries string
	windowExpiries             string
	// Group scope enforcement (membership-aware group quotas, ruled
	// 2026-10-07). spendStore is nil on installs without a Postgres handle
	// (serving-only spokes): group RATE limits still enforce from the Valkey
	// store, spend caps are then unchecked — logged loudly at startup by
	// cmd/interceptor, never silently assumed.
	spendStore  GroupSpendStore
	spendCache  *spendVerdictCache
	spendFlight *spendFlight
	spendLog    *logging.Logger
	spendFails  *spendFailureLog
}

// WithGroupSpend enables the monthly spend cap check against a GroupSpendStore
// and returns the admitter for chaining. log carries the loud-failure lines:
// the spend check FAILS OPEN (a quota may never take inference down), so a
// store error is always an ERROR line, throttled by spendFailureLog so a
// Postgres outage cannot emit one line per admitted request.
func (a *RedisAdmitter) WithGroupSpend(store GroupSpendStore, log *logging.Logger) *RedisAdmitter {
	a.spendStore = store
	a.spendLog = log
	a.spendCache = newSpendVerdictCache()
	a.spendFlight = newSpendFlight()
	a.spendFails = &spendFailureLog{}
	return a
}

func New(client redis.Cmdable, cfg config.AdmissionSettings) *RedisAdmitter {
	prefix := cfg.KeyPrefix
	// One cluster hash slot makes each multi-key Lua operation valid on Redis
	// Cluster as well as single-node Valkey.
	tag := "{" + prefix + "}"
	return &RedisAdmitter{
		client: client, cfg: cfg, counters: tag + ":counters", leases: tag + ":leases",
		expiries: tag + ":expiries", windowExpiries: tag + ":window-expiries",
	}
}

// NewValkeyClient builds the admission-only Valkey client. Metering owns a
// separate durable client/WAL path and must not inherit these fail-open
// timeouts.
func NewValkeyClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:                  addr,
		DialTimeout:           valkeyIOTimeout,
		ReadTimeout:           valkeyIOTimeout,
		WriteTimeout:          valkeyIOTimeout,
		PoolTimeout:           valkeyIOTimeout,
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	})
}

// FromSettings builds the distributed admitter the interceptor runs with: the
// settings from Settings.EffectiveAdmission (operator tiers cleared under
// admission.enabled=false) against the admission store (admission.valkeyAddr,
// else emit.valkeyAddr). It returns false, and a nil admitter and client,
// when no store is configured. The caller owns the client and closes it.
func FromSettings(s *config.Settings) (*RedisAdmitter, *redis.Client, bool) {
	cfg, ok := s.EffectiveAdmission()
	if !ok {
		return nil, nil, false
	}
	client := NewValkeyClient(cfg.ValkeyAddr)
	return New(client, cfg), client, true
}

// Settings returns the admission settings this admitter enforces.
func (a *RedisAdmitter) Settings() config.AdmissionSettings { return a.cfg }

func (a *RedisAdmitter) keys() []string {
	return []string{a.counters, a.leases, a.expiries, a.windowExpiries}
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (a *RedisAdmitter) Admit(ctx context.Context, req Request) (*Lease, error) {
	if req.Organization == "" || req.Model == "" || req.Graph == "" {
		return nil, fmt.Errorf("%w: missing trusted organization/model/graph identity", ErrInvalidIdentity)
	}
	if req.OwnerLimits.Any() && req.Owner == "" {
		return nil, fmt.Errorf("%w: missing trusted owner identity", ErrInvalidIdentity)
	}
	for _, gs := range req.GroupScopes {
		// A group scope carrying any limit or spend cap must name its group:
		// an anonymous quota is not a quota, it is a broken envelope (fail
		// closed, like every other structural violation).
		if gs.GroupID == "" && (gs.Limits.Any() || gs.SpendCap != "") {
			return nil, fmt.Errorf("%w: group scope carries limits without a group id", ErrInvalidIdentity)
		}
	}
	if req.PromptBytes < 0 || req.EstimatedInputTokens <= 0 || req.ReservedOutputTokens <= 0 {
		return nil, &Rejected{Scope: "request", Dimension: "work estimate", RetryAfter: time.Second}
	}
	// The monthly spend cap is a Go-side pre-check against Postgres (cached
	// per (group, cap), compared in SQL), ahead of the Lua reservation: a group
	// at its cap is rejected without reserving anything. It fails OPEN — a
	// store error logs loud and admits, the 2026-09-24 posture (quotas are
	// permissible; they never take inference down).
	if err := a.checkGroupSpend(ctx, req.GroupScopes); err != nil {
		return nil, err
	}
	id, err := randomID()
	if err != nil {
		return nil, fmt.Errorf("%w: lease id: %v", ErrUnavailable, err)
	}
	w := wireRequest{ID: id, LeaseMs: a.cfg.LeaseTTL.Milliseconds(), Prompt: req.PromptBytes,
		EstimatedInput: req.EstimatedInputTokens, Output: req.ReservedOutputTokens, Scopes: a.scopes(req)}
	if req.Adapter {
		w.Adapter = 1
	}
	b, _ := json.Marshal(w)
	deadline := time.Now().Add(admitOperationBudget)
	operationCtx, cancelOperation := context.WithDeadline(ctx, deadline)
	defer cancelOperation()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		res, runErr := admitScript.Run(operationCtx, a.client, a.keys(), string(b)).Result()
		if runErr != nil {
			lastErr = runErr
			continue
		}
		parts, ok := res.([]interface{})
		if !ok || len(parts) == 0 {
			lastErr = errors.New("malformed response")
			continue
		}
		if number(parts[0]) != 1 {
			scopeName, dimension := "unknown", "capacity"
			if len(parts) > 2 {
				scopeName = fmt.Sprint(parts[1])
				dimension = fmt.Sprint(parts[2])
			}
			contractual := len(parts) > 3 && number(parts[3]) == 1
			if strings.HasSuffix(dimension, unsatisfiableSuffix) {
				return nil, &Rejected{Scope: scopeName, Dimension: dimension, Contractual: contractual, Unsatisfiable: true}
			}
			retryAfter := a.retryAfter(w.Scopes)
			if len(parts) > 4 && number(parts[4]) > 0 {
				retryAfter = time.Duration(number(parts[4])) * time.Millisecond
			}
			return nil, &Rejected{Scope: scopeName, Dimension: dimension, RetryAfter: retryAfter, Contractual: contractual}
		}
		return &Lease{
			owner: a,
			id:    id,
			unknownUsage: Usage{
				TotalPromptTokens: req.EstimatedInputTokens,
				GeneratedTokens:   req.ReservedOutputTokens,
			},
		}, nil
	}

	// Both replies were indeterminate. Compensate with the same request id:
	// this releases a committed lease and is a no-op if neither attempt ran.
	// The cleanup gets its own budget, independent of the attempt deadline:
	// the attempts may have exhausted it — a slow store is exactly when a
	// committed lease needs compensating — and a cancelled client context
	// must not strand the reservation either.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), admitOperationBudget)
	defer cancel()
	if _, cleanupErr := abandonScript.Run(cleanupCtx, a.client, a.keys(), id).Result(); cleanupErr != nil {
		return nil, fmt.Errorf("%w: admit: %v; cleanup: %v", ErrUnavailable, lastErr, cleanupErr)
	}
	return nil, fmt.Errorf("%w: admit: %v", ErrUnavailable, lastErr)
}

func number(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case string:
		var x int64
		_, _ = fmt.Sscan(n, &x)
		return x
	}
	return 0
}

func (a *RedisAdmitter) retryAfter(scopes []scope) time.Duration {
	minimum := time.Minute
	for _, s := range scopes {
		if d := time.Duration(s.WindowMs) * time.Millisecond; d > 0 && d < minimum {
			minimum = d
		}
	}
	return minimum
}

func scaled(l config.AdmissionLimits, weight int64) config.AdmissionLimits {
	if weight <= 1 {
		return l
	}
	mul := func(v *int64) *int64 {
		if v == nil || *v == 0 || *v > math.MaxInt64/weight {
			return v
		}
		m := *v * weight
		return &m
	}
	l.MaxActiveRequests = mul(l.MaxActiveRequests)
	l.MaxConcurrentPrefills = mul(l.MaxConcurrentPrefills)
	l.MaxReservedDecodeSlots = mul(l.MaxReservedDecodeSlots)
	l.MaxPromptBytes = mul(l.MaxPromptBytes)
	l.MaxReservedOutputTokens = mul(l.MaxReservedOutputTokens)
	l.MaxActiveAdapters = mul(l.MaxActiveAdapters)
	l.RequestsPerWindow = mul(l.RequestsPerWindow)
	l.TotalPromptTokensPerWindow = mul(l.TotalPromptTokensPerWindow)
	l.UncachedPromptTokensPerWindow = mul(l.UncachedPromptTokensPerWindow)
	l.GeneratedTokensPerWindow = mul(l.GeneratedTokensPerWindow)
	l.MaxColdHolds = mul(l.MaxColdHolds)
	l.WakesPerWindow = mul(l.WakesPerWindow)
	return l
}

// wireLimit maps a nullable limit onto the Lua wire sentinel: nil (unlimited)
// is 0, an explicit zero cap is -1, and a positive cap passes through. Every
// Lua limit check is `limit ~= 0 and usage > limit`, so -1 rejects any usage
// (usage is never negative) while 0 skips the check entirely.
func wireLimit(v *int64) int64 {
	if v == nil {
		return 0
	}
	if *v == 0 {
		return -1
	}
	return *v
}

func makeScope(name, id string, l config.AdmissionLimits) scope {
	windowMs := l.Window.Milliseconds()
	if windowMs <= 0 {
		windowMs = time.Minute.Milliseconds()
	}
	digest := sha256.Sum256([]byte(id))
	return scope{ID: name + ":" + hex.EncodeToString(digest[:]), Name: name, Active: wireLimit(l.MaxActiveRequests), Prefills: wireLimit(l.MaxConcurrentPrefills),
		ReservedDecodeSlots: wireLimit(l.MaxReservedDecodeSlots),
		Prompt:              wireLimit(l.MaxPromptBytes), Output: wireLimit(l.MaxReservedOutputTokens), Adapters: wireLimit(l.MaxActiveAdapters),
		Requests: wireLimit(l.RequestsPerWindow), TotalPrompt: wireLimit(l.TotalPromptTokensPerWindow),
		UncachedPrompt: wireLimit(l.UncachedPromptTokensPerWindow), Generated: wireLimit(l.GeneratedTokensPerWindow), Cold: wireLimit(l.MaxColdHolds),
		Wakes: wireLimit(l.WakesPerWindow), WindowMs: windowMs}
}

func makeContractScope(name, id string, limits RateLimits) scope {
	digest := sha256.Sum256([]byte(id))
	return scope{
		ID: name + ":" + hex.EncodeToString(digest[:]), Name: name,
		Requests: wireLimit(limits.Requests), TotalPrompt: wireLimit(limits.TotalPromptTokens),
		UncachedPrompt: wireLimit(limits.UncachedPromptTokens), Generated: wireLimit(limits.GeneratedTokens),
		WindowMs: time.Minute.Milliseconds(), Contractual: true,
	}
}

// unsatisfiableSuffix ends the dimension name admitScript returns when one
// request alone exceeds a contract scope's whole per-window limit.
const unsatisfiableSuffix = "_exceeds_limit"

// Any reports whether the contract carries at least one limit. A contract with
// no limit is unlimited in every dimension and admits nothing to check.
func (l RateLimits) Any() bool {
	return l.Requests != nil || l.TotalPromptTokens != nil || l.UncachedPromptTokens != nil || l.GeneratedTokens != nil
}

func (a *RedisAdmitter) scopes(r Request) []scope {
	out := []scope{makeScope("platform", "all", a.cfg.Platform), makeScope("graph", r.Graph, a.cfg.Graph),
		makeScope("organization", r.Organization, a.cfg.Organization), makeScope("organization_model", r.Organization+"\x1f"+r.Model, a.cfg.OrganizationModel)}
	// Every operator scope, the lane included, precedes the contract scopes:
	// admitScript checks scopes in order, so at an exact tie the operator scope
	// answers (503) before the contract scope (429).
	laneName := a.cfg.OrganizationLanes[r.Organization]
	if laneName == "" {
		laneName = "default"
	}
	if _, ok := a.cfg.Lanes[laneName]; !ok {
		laneName = "default"
	}
	if t, ok := a.cfg.Lanes[laneName]; ok {
		out = append(out, makeScope("lane", laneName, scaled(t.Limits, t.Weight)))
	}
	if r.OrganizationLimits.Any() {
		out = append(out, makeContractScope("contract_organization", r.Organization, r.OrganizationLimits))
	}
	if r.OwnerLimits.Any() {
		out = append(out, makeContractScope("contract_owner", r.Owner, r.OwnerLimits))
	}
	// Group rate scopes: one contract scope per group that carries any rate
	// limit, same Lua/counter machinery and wireLimit sentinels as the org/
	// owner contract scopes, settling through the same lease/CompleteUsage
	// path (the lease record holds this scope like any other, and finishScript
	// charges its windows). The scope NAME carries the group id so a rejection
	// names the team; the scope id digests it, so groups never collide.
	for _, gs := range r.GroupScopes {
		if gs.Limits.Any() {
			out = append(out, makeContractScope("group:"+gs.GroupID, gs.GroupID, gs.Limits))
		}
	}
	return out
}

// checkGroupSpend enforces the monthly spend cap for every group scope that
// carries one: reject when the group's month-to-date attribution spend (the
// rater's group_usage rollup) has reached the cap. Reaching is the boundary —
// spend >= cap rejects, so a cap of 0 rejects all paid work for the group.
// A 429+Retry-After denial (Contractual); the retry hint is one minute, the
// cache TTL: the verdict can refresh no sooner anyway, and spend only relaxes
// when the month turns or a re-rate supersedes the rollup.
//
// FAIL OPEN, LOUD: a missing store (install without Postgres) or a store
// error admits the request and logs. Quotas are permissible; they never take
// inference down, and they are never silently enforced either — every bypass
// is an ERROR line (throttled so an outage cannot flood).
func (a *RedisAdmitter) checkGroupSpend(ctx context.Context, scopes []GroupScope) error {
	if a.spendStore == nil {
		for _, gs := range scopes {
			if gs.SpendCap != "" {
				a.spendBypass(gs.GroupID, errors.New("no spend store configured (group_usage is unreachable — is DATABASE_URL set?)"))
			}
		}
		return nil
	}
	// One query budget covers the whole check, not one per group: a request
	// carrying many uncached capped groups against a slow store waits at most
	// one groupSpendQueryBudget in total. A group whose read cannot finish
	// before the shared deadline fails open like any other store error. The
	// budget starts at the first cache miss, so all-cached checks pay nothing.
	var budgetCtx context.Context
	for _, gs := range scopes {
		if gs.SpendCap == "" {
			continue
		}
		if verdict, ok := a.spendCache.lookup(gs.GroupID, gs.SpendCap); ok {
			if verdict.failed {
				// A recent store error already logged its bypass once
				// (throttled): admit without re-querying and without waiting
				// on the store.
				continue
			}
			if verdict.exhausted {
				return &Rejected{Scope: "group:" + gs.GroupID, Dimension: "monthly_spend", RetryAfter: groupSpendCacheTTL, Contractual: true}
			}
			continue
		}
		if budgetCtx == nil {
			var cancelBudget context.CancelFunc
			budgetCtx, cancelBudget = context.WithTimeout(ctx, groupSpendQueryBudget)
			defer cancelBudget()
		}
		deadline, _ := budgetCtx.Deadline()
		// Collapse concurrent misses on one (group, cap) into a single store
		// read: waiters take the leader's result (verdict, or fail-open on
		// error) instead of each running their own query against a slow store.
		result, leader := a.spendFlight.do(budgetCtx, gs.GroupID+"\x00"+gs.SpendCap, func() (bool, error) {
			// Bounded: a hung store read fails open (deadline exceeded is just
			// another store error) instead of holding the request. Detached
			// from the leader's cancellation: other requests are waiting on
			// this read, and one client disconnecting must not end it and
			// hand every waiter a free admit. The shared deadline still
			// bounds it.
			queryCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
			defer cancel()
			return a.spendStore.GroupSpendExhausted(queryCtx, gs.GroupID, gs.SpendCap)
		})
		if result.err != nil {
			// A canceled request context (client gone) makes the check moot:
			// it is not evidence the store is down and must not consume the
			// throttled bypass slot that a genuine outage needs (KeepAlive
			// guards the same race at renew time). The 250ms deadline
			// expiring with a live client IS logged: the store is too slow.
			// Only the leader records and logs — waiters share its outcome.
			if leader && ctx.Err() == nil {
				a.spendCache.storeFailure(gs.GroupID, gs.SpendCap)
				a.spendBypass(gs.GroupID, result.err)
			}
			continue
		}
		if leader {
			a.spendCache.store(gs.GroupID, gs.SpendCap, result.exhausted)
		}
		if result.exhausted {
			return &Rejected{Scope: "group:" + gs.GroupID, Dimension: "monthly_spend", RetryAfter: groupSpendCacheTTL, Contractual: true}
		}
	}
	return nil
}

// spendBypass logs a fail-open spend-check bypass at ERROR, throttled through
// spendFailureLog so a sustained store outage stays visible at its onset
// without one line per admitted request. Nil-safe for admitters built without
// WithGroupSpend (a spendStore-less admitter can still see spend caps if a
// caller hands it group scopes directly).
func (a *RedisAdmitter) spendBypass(groupID string, err error) {
	if a.spendLog == nil || a.spendFails == nil {
		return
	}
	a.spendFails.logf(a.spendLog, "admission: group %s: monthly spend cap NOT enforced (fail open): %v", groupID, err)
}

// Lease owns all reservations for one accepted request. Its transition methods
// are idempotent locally and atomically idempotent in Valkey.
type Lease struct {
	owner *RedisAdmitter
	id    string

	mu      sync.Mutex
	settled bool
	pending *Usage

	// unknownUsage is the conservative contractual charge when the engine's
	// authoritative usage block never arrives. The estimate was already
	// reserved as wholly uncached input, and generated capacity was reserved at
	// the request's maximum output, so retaining both prevents an abort-before-
	// usage client from turning consumed model work into a zero-token request.
	unknownUsage Usage
}

func (l *Lease) PrefillDone(ctx context.Context) error {
	return l.run(ctx, transitionScript, "prefill", 0)
}
func (l *Lease) BeginColdHold(ctx context.Context) error { return l.run(ctx, coldScript, "begin", 0) }
func (l *Lease) EndColdHold(ctx context.Context) error   { return l.run(ctx, coldScript, "end", 0) }

func (l *Lease) Complete(ctx context.Context, generatedTokens int64) error {
	return l.CompleteUsage(ctx, Usage{GeneratedTokens: generatedTokens})
}

// CompleteUnknownUsage releases physical capacity while retaining the
// conservative input and output token charges reserved before dispatch. Use it
// whenever a dispatched request's usage is indeterminate (including a client
// abort before response headers); definite pre-dispatch/transport failures
// continue to use Complete(0).
func (l *Lease) CompleteUnknownUsage(ctx context.Context) error {
	return l.CompleteUsage(ctx, l.unknownUsage)
}

// Usage is the engine-authoritative token result used to settle contractual
// throughput windows. CachedPromptTokens is a subset of TotalPromptTokens.
// Prompt-byte reservations remain a separate physical guard and are never
// converted into contractual token usage.
type Usage struct {
	TotalPromptTokens  int64
	CachedPromptTokens int64
	GeneratedTokens    int64
}

// CompleteUsage releases physical reservations and atomically reconciles the
// conservative input-token reservation with exact engine-reported usage.
// Estimated input is reserved as uncached before dispatch; settlement refunds
// overestimates or records underestimates as debt in the current window.
// Generated capacity remains strictly reserved from max_tokens before dispatch.
func (l *Lease) CompleteUsage(ctx context.Context, usage Usage) error {
	// The Lua transition is idempotent (a missing lease is success), so retrying
	// after a transient Valkey failure is both safe and preferable to waiting for
	// TTL reaping.
	totalPrompt := max(usage.TotalPromptTokens, 0)
	cachedPrompt := max(usage.CachedPromptTokens, 0)
	if cachedPrompt > totalPrompt {
		cachedPrompt = totalPrompt
	}
	normalized := Usage{TotalPromptTokens: totalPrompt, CachedPromptTokens: cachedPrompt, GeneratedTokens: max(usage.GeneratedTokens, 0)}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.settled {
		return nil
	}
	// First completion data wins. In the normal response path this is the
	// engine-authoritative usage. If its transaction fails transiently, the
	// deferred Complete(0) retries these retained counts instead of deleting the
	// lease with zero usage.
	if l.pending == nil {
		l.pending = &normalized
	}
	pending := *l.pending
	uncachedPrompt := pending.TotalPromptTokens - pending.CachedPromptTokens
	if err := l.run(ctx, finishScript, "finish", pending.GeneratedTokens, pending.TotalPromptTokens, uncachedPrompt); err != nil {
		return err
	}
	l.settled = true
	l.pending = nil
	return nil
}

// KeepAlive renews a live request's expiry until ctx is cancelled. A healthy
// replica therefore retains reservations for arbitrarily long streams; a dead
// replica stops renewing and is reaped after leaseTtl. onError is invoked once
// if renewal fails. The proxy treats distributed-state failure as a temporary
// bypass of the fairness gate; authorization and independent metering remain.
func (l *Lease) KeepAlive(ctx context.Context, onError func(error)) {
	interval := l.owner.cfg.LeaseTTL / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	renew := func() bool {
		// A request can sit in a scheduler queue long enough that waiting for the
		// first ticker edge would put a short lease needlessly close to expiry.
		// Renew once synchronously, then maintain it on the regular cadence.
		if ctx.Err() != nil {
			return false
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.settled {
			return false
		}
		res, err := renewScript.Run(ctx, l.owner.client, l.owner.keys(), l.id, l.owner.cfg.LeaseTTL.Milliseconds()).Int64()
		if err != nil {
			// Request completion normally cancels this context. If cancellation
			// races the Redis call, it is not evidence that the admission store
			// failed and must not be reported as an availability incident.
			if ctx.Err() != nil {
				return false
			}
			if onError != nil {
				onError(fmt.Errorf("%w: renew lease: %v", ErrUnavailable, err))
			}
			return false
		}
		if res == 0 {
			if onError != nil {
				onError(fmt.Errorf("%w: lease expired or was reaped", ErrUnavailable))
			}
			return false
		}
		return true
	}
	if !renew() {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !renew() {
				return
			}
		}
	}
}

func (l *Lease) run(ctx context.Context, script *redis.Script, action string, values ...int64) error {
	args := []interface{}{l.id, action}
	for _, value := range values {
		args = append(args, value)
	}
	res, err := script.Run(ctx, l.owner.client, l.owner.keys(), args...).Result()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	parts, ok := res.([]interface{})
	if ok && len(parts) > 0 && number(parts[0]) == 0 {
		sn, dim := "unknown", "capacity"
		if len(parts) > 2 {
			sn = fmt.Sprint(parts[1])
			dim = fmt.Sprint(parts[2])
		}
		contractual := len(parts) > 3 && number(parts[3]) == 1
		retryAfter := time.Minute
		if len(parts) > 4 && number(parts[4]) > 0 {
			retryAfter = time.Duration(number(parts[4])) * time.Millisecond
		}
		return &Rejected{Scope: sn, Dimension: dim, RetryAfter: retryAfter, Contractual: contractual}
	}
	return nil
}
