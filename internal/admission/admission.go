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
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
)

var ErrUnavailable = errors.New("distributed admission state unavailable")

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
}

// RateLimits is the authenticated customer contract. Zero means unlimited.
// Every value is per minute; total prompt contains the uncached subset.
type RateLimits struct {
	Requests             int64
	TotalPromptTokens    int64
	UncachedPromptTokens int64
	GeneratedTokens      int64
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
		return nil, fmt.Errorf("%w: missing trusted organization/model/graph identity", ErrUnavailable)
	}
	if req.OwnerLimits.any() && req.Owner == "" {
		return nil, fmt.Errorf("%w: missing trusted owner identity", ErrUnavailable)
	}
	if req.PromptBytes < 0 || req.EstimatedInputTokens <= 0 || req.ReservedOutputTokens <= 0 {
		return nil, &Rejected{Scope: "request", Dimension: "work estimate", RetryAfter: time.Second}
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
	cleanupCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
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
	mul := func(v int64) int64 {
		if v == 0 || v > math.MaxInt64/weight {
			return v
		}
		return v * weight
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

func makeScope(name, id string, l config.AdmissionLimits) scope {
	windowMs := l.Window.Milliseconds()
	if windowMs <= 0 {
		windowMs = time.Minute.Milliseconds()
	}
	digest := sha256.Sum256([]byte(id))
	return scope{ID: name + ":" + hex.EncodeToString(digest[:]), Name: name, Active: l.MaxActiveRequests, Prefills: l.MaxConcurrentPrefills,
		ReservedDecodeSlots: l.MaxReservedDecodeSlots,
		Prompt:              l.MaxPromptBytes, Output: l.MaxReservedOutputTokens, Adapters: l.MaxActiveAdapters,
		Requests: l.RequestsPerWindow, TotalPrompt: l.TotalPromptTokensPerWindow,
		UncachedPrompt: l.UncachedPromptTokensPerWindow, Generated: l.GeneratedTokensPerWindow, Cold: l.MaxColdHolds,
		Wakes: l.WakesPerWindow, WindowMs: windowMs}
}

func makeContractScope(name, id string, limits RateLimits) scope {
	digest := sha256.Sum256([]byte(id))
	return scope{
		ID: name + ":" + hex.EncodeToString(digest[:]), Name: name,
		Requests: limits.Requests, TotalPrompt: limits.TotalPromptTokens,
		UncachedPrompt: limits.UncachedPromptTokens, Generated: limits.GeneratedTokens,
		WindowMs: time.Minute.Milliseconds(), Contractual: true,
	}
}

func (l RateLimits) any() bool {
	return l.Requests > 0 || l.TotalPromptTokens > 0 || l.UncachedPromptTokens > 0 || l.GeneratedTokens > 0
}

func (a *RedisAdmitter) scopes(r Request) []scope {
	out := []scope{makeScope("platform", "all", a.cfg.Platform), makeScope("graph", r.Graph, a.cfg.Graph),
		makeScope("organization", r.Organization, a.cfg.Organization), makeScope("organization_model", r.Organization+"\x1f"+r.Model, a.cfg.OrganizationModel)}
	if r.OrganizationLimits.any() {
		out = append(out, makeContractScope("contract_organization", r.Organization, r.OrganizationLimits))
	}
	if r.OwnerLimits.any() {
		out = append(out, makeContractScope("contract_owner", r.Owner, r.OwnerLimits))
	}
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
	return out
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
// only after upstream response work began but no authoritative usage block was
// captured; early transport failures continue to use Complete(0).
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
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.mu.Lock()
			if l.settled {
				l.mu.Unlock()
				return
			}
			res, err := renewScript.Run(ctx, l.owner.client, l.owner.keys(), l.id, l.owner.cfg.LeaseTTL.Milliseconds()).Int64()
			if err != nil {
				l.mu.Unlock()
				if onError != nil {
					onError(fmt.Errorf("%w: renew lease: %v", ErrUnavailable, err))
				}
				return
			}
			if res == 0 {
				l.mu.Unlock()
				if onError != nil {
					onError(fmt.Errorf("%w: lease expired or was reaped", ErrUnavailable))
				}
				return
			}
			l.mu.Unlock()
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
