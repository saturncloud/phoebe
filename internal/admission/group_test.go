package admission

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
)

// fakeGroupSpendStore is the spend-check seam test double: verdicts and errors
// are scripted per call so a test can observe exactly how often Postgres would
// be read (the cache-behavior tests pin the once-per-TTL contract).
type fakeGroupSpendStore struct {
	mu        sync.Mutex
	exhausted bool
	err       error
	calls     int
	gotGID    []string
	gotCap    []string
}

func (f *fakeGroupSpendStore) GroupSpendExhausted(_ context.Context, groupID, cap string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.gotGID = append(f.gotGID, groupID)
	f.gotCap = append(f.gotCap, cap)
	return f.exhausted, f.err
}

// spendTestAdmitter builds an admitter over miniredis with a fake spend store
// and a silent logger.
func spendTestAdmitter(t *testing.T) (*RedisAdmitter, *fakeGroupSpendStore) {
	t.Helper()
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	fake := &fakeGroupSpendStore{}
	a.WithGroupSpend(fake, logging.New(logging.ERROR))
	return a, fake
}

const testGID = "a1b2c3d4e5f60718293a4b5c6d7e8f90"

// groupScope builds one GroupScope. rate of nil leaves every rate unset
// (unlimited); a non-nil rate installs all four dimensions to the same value.
func groupScope(gid string, rate *int64, spend string) GroupScope {
	gs := GroupScope{GroupID: gid, SpendCap: spend}
	if rate != nil {
		gs.Limits = RateLimits{Requests: rate, TotalPromptTokens: rate,
			UncachedPromptTokens: rate, GeneratedTokens: rate}
	}
	return gs
}

func groupRequest(org, model string, scopes ...GroupScope) Request {
	r := request(org, model)
	r.GroupScopes = scopes
	return r
}

// TestGroupRateLimitsEnforcedLikeContractScope: a group's per-minute windows
// are the same Lua/counter machinery as the contract scopes — a burst up to
// the cap admits, the next request is a contractual rejection (the proxy maps
// that to 429 + Retry-After).
func TestGroupRateLimitsEnforcedLikeContractScope(t *testing.T) {
	a, _ := spendTestAdmitter(t)
	// Only the requests window is capped, so the request's own estimates
	// (the helper's 10-in/20-out shape) can never trip the unsatisfiable
	// check: the third request is rejected by the window counter itself.
	cap := int64(2)
	scope := GroupScope{GroupID: testGID, Limits: RateLimits{Requests: &cap}}

	for i := 0; i < 2; i++ {
		lease, err := a.Admit(context.Background(), groupRequest("org-g", "m", scope))
		if err != nil {
			t.Fatalf("admit %d: %v, want admitted", i+1, err)
		}
		_ = lease.Complete(context.Background(), 0)
	}
	_, err := a.Admit(context.Background(), groupRequest("org-g", "m", scope))
	var rejected *Rejected
	if !errors.As(err, &rejected) {
		t.Fatalf("third admit: %v, want a contract rejection", err)
	}
	if !rejected.Contractual || rejected.RetryAfter <= 0 {
		t.Fatalf("rejection = %+v, want Contractual with a Retry-After (429 mapping)", rejected)
	}
	if rejected.Scope != "group:"+testGID {
		t.Fatalf("rejection scope = %q, want the group id named", rejected.Scope)
	}
}

// TestGroupRateZeroCapBlocksEveryRequest: an explicit 0 is a zero cap, not
// unlimited — the same R4 sentinel as every other contract scope.
func TestGroupRateZeroCapBlocksEveryRequest(t *testing.T) {
	a, _ := spendTestAdmitter(t)
	zero := int64(0)
	_, err := a.Admit(context.Background(), groupRequest("org-z", "m",
		groupScope(testGID, &zero, "")))
	var rejected *Rejected
	if !errors.As(err, &rejected) || !rejected.Contractual {
		t.Fatalf("zero-capped group admit: %v, want a contractual rejection", err)
	}
}

// TestGroupRateNilLimitIsUnlimited: a group scope whose rate fields are all
// nil contributes no contract scope (unlimited), so requests flow freely.
func TestGroupRateNilLimitIsUnlimited(t *testing.T) {
	a, _ := spendTestAdmitter(t)
	for i := 0; i < 3; i++ {
		lease, err := a.Admit(context.Background(), groupRequest("org-u", "m",
			groupScope(testGID, nil, "")))
		if err != nil {
			t.Fatalf("admit %d: %v, want admitted (nil limits are unlimited)", i+1, err)
		}
		_ = lease.Complete(context.Background(), 0)
	}
}

// TestGroupSpendCapDeniesAtCap: spend >= cap rejects (reaching the cap IS the
// denial boundary), the denial is contractual (429 + Retry-After at the
// proxy), and the cap string reaches the store verbatim.
func TestGroupSpendCapDeniesAtCap(t *testing.T) {
	a, store := spendTestAdmitter(t)
	store.exhausted = true

	_, err := a.Admit(context.Background(), groupRequest("org-s", "m",
		groupScope(testGID, nil, "100.000000000")))
	var rejected *Rejected
	if !errors.As(err, &rejected) {
		t.Fatalf("capped group admit: %v, want a rejection", err)
	}
	if !rejected.Contractual || rejected.Dimension != "monthly_spend" || rejected.RetryAfter <= 0 {
		t.Fatalf("rejection = %+v, want Contractual monthly_spend with Retry-After (429 mapping)", rejected)
	}
	if store.gotGID[0] != testGID || store.gotCap[0] != "100.000000000" {
		t.Fatalf("store bound (gid, cap) = (%q, %q), want the envelope values verbatim", store.gotGID[0], store.gotCap[0])
	}
}

// TestGroupSpendZeroCapDeniesAllPaidWork: the rater-side predicate is
// spend >= cap, and a zero cap is reached by every request — zero spend is
// still >= 0.
func TestGroupSpendZeroCapDeniesAllPaidWork(t *testing.T) {
	a, store := spendTestAdmitter(t)
	store.exhausted = true // the SQL verdict at a zero cap: COALESCE(SUM,0) >= 0
	_, err := a.Admit(context.Background(), groupRequest("org-s", "m",
		groupScope(testGID, nil, "0")))
	var rejected *Rejected
	if !errors.As(err, &rejected) || !rejected.Contractual {
		t.Fatalf("zero-cap group admit: %v, want a contractual rejection", err)
	}
}

// TestGroupSpendCapAdmitsBelowCap: under the cap the request admits, and the
// verdict — not the spend — is what the store returns.
func TestGroupSpendCapAdmitsBelowCap(t *testing.T) {
	a, store := spendTestAdmitter(t)
	lease, err := a.Admit(context.Background(), groupRequest("org-b", "m",
		groupScope(testGID, nil, "100")))
	if err != nil {
		t.Fatalf("under-cap admit: %v, want admitted", err)
	}
	_ = lease.Complete(context.Background(), 0)
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}

	// For contrast: a scope without a spend cap never reads the store.
	lease, err = a.Admit(context.Background(), groupRequest("org-b", "m",
		groupScope("ffffffffffffffffffffffffffffffff", nil, "")))
	if err != nil {
		t.Fatalf("uncapped admit: %v, want admitted", err)
	}
	_ = lease.Complete(context.Background(), 0)
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want still 1 (an empty SpendCap skips the store)", store.calls)
	}
}

// TestGroupSpendVerdictCachedPerGroupAndCap: the verdict is cached ~60s per
// (group, cap) — a second request must not re-read Postgres, and a different
// cap for the same group is a different key.
func TestGroupSpendVerdictCachedPerGroupAndCap(t *testing.T) {
	a, store := spendTestAdmitter(t)
	scope := groupScope(testGID, nil, "100")
	ctx := context.Background()

	if _, err := a.Admit(ctx, groupRequest("org-c", "m", scope)); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	if _, err := a.Admit(ctx, groupRequest("org-c", "m", scope)); err != nil {
		t.Fatalf("second admit: %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (the verdict is cached per (group, cap))", store.calls)
	}

	// A different cap is a different cache key: one more read, then cached.
	scope.SpendCap = "50"
	if _, err := a.Admit(ctx, groupRequest("org-c", "m", scope)); err != nil {
		t.Fatalf("different-cap admit: %v", err)
	}
	if _, err := a.Admit(ctx, groupRequest("org-c", "m", scope)); err != nil {
		t.Fatalf("different-cap admit 2: %v", err)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2 (a changed cap re-checks once)", store.calls)
	}
}

// TestGroupSpendCacheExpiry: past the TTL the verdict re-reads Postgres, so a
// group that hit its cap is not pinned to a stale verdict forever, and a cap
// RAISE takes effect within one TTL.
func TestGroupSpendCacheExpiry(t *testing.T) {
	a, store := spendTestAdmitter(t)
	now := time.Now()
	a.spendCache.now = func() time.Time { return now }
	scope := groupScope(testGID, nil, "100")
	ctx := context.Background()

	if _, err := a.Admit(ctx, groupRequest("org-e", "m", scope)); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	if _, err := a.Admit(ctx, groupRequest("org-e", "m", scope)); err != nil {
		t.Fatalf("cached admit: %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1 inside the TTL", store.calls)
	}

	// Past the TTL: a fresh read.
	now = now.Add(groupSpendCacheTTL + time.Second)
	if _, err := a.Admit(ctx, groupRequest("org-e", "m", scope)); err != nil {
		t.Fatalf("post-TTL admit: %v", err)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2 after the TTL expires", store.calls)
	}
}

// TestGroupSpendStoreErrorFailsOpen: a Postgres/store error must never deny —
// the check fails open with a loud log (the 2026-09-24 posture: quotas are
// permissible; they never take inference down). The error is negative-cached
// briefly so an outage is not re-queried per request, and the check re-probes
// once the short TTL lapses, so recovery is seen quickly.
func TestGroupSpendStoreErrorFailsOpen(t *testing.T) {
	a, store := spendTestAdmitter(t)
	now := time.Now()
	a.spendCache.now = func() time.Time { return now }
	store.err = errors.New("postgres unreachable")

	lease, err := a.Admit(context.Background(), groupRequest("org-f", "m",
		groupScope(testGID, nil, "100")))
	if err != nil {
		t.Fatalf("store-error admit: %v, want fail-open admission", err)
	}
	_ = lease.Complete(context.Background(), 0)

	// Inside the negative TTL the second request admits without re-querying.
	lease2, err := a.Admit(context.Background(), groupRequest("org-f", "m",
		groupScope(testGID, nil, "100")))
	if err != nil {
		t.Fatalf("second store-error admit: %v, want fail-open admission", err)
	}
	_ = lease2.Complete(context.Background(), 0)
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (the error is negative-cached)", store.calls)
	}

	// Past the negative TTL the check re-probes the store — and still admits
	// while the error persists.
	now = now.Add(groupSpendNegativeCacheTTL + time.Second)
	lease3, err := a.Admit(context.Background(), groupRequest("org-f", "m",
		groupScope(testGID, nil, "100")))
	if err != nil {
		t.Fatalf("post-negative-TTL admit: %v, want fail-open admission", err)
	}
	_ = lease3.Complete(context.Background(), 0)
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2 (recovery re-probes after the negative TTL)", store.calls)
	}
}

// TestGroupSpendStoreErrorNegativeCache: after one failing Admit, a second
// Admit within the negative-cache TTL makes no store call, does not wait on
// the store, and admits.
func TestGroupSpendStoreErrorNegativeCache(t *testing.T) {
	a, store := spendTestAdmitter(t)
	store.err = errors.New("postgres unreachable")
	scope := groupScope(testGID, nil, "100")
	ctx := context.Background()

	lease, err := a.Admit(ctx, groupRequest("org-nc", "m", scope))
	if err != nil {
		t.Fatalf("first admit: %v", err)
	}
	_ = lease.Complete(ctx, 0)
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}

	start := time.Now()
	lease2, err := a.Admit(ctx, groupRequest("org-nc", "m", scope))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("negative-cached admit: %v, want fail-open admission", err)
	}
	_ = lease2.Complete(ctx, 0)
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (no re-query inside the negative TTL)", store.calls)
	}
	if elapsed >= groupSpendQueryBudget {
		t.Fatalf("negative-cached admit took %v, want no wait on the store (budget %v)", elapsed, groupSpendQueryBudget)
	}
}

// blockingGroupSpendStore models a blackholed Postgres: the read never
// answers and returns only when its context is done.
type blockingGroupSpendStore struct{}

func (blockingGroupSpendStore) GroupSpendExhausted(ctx context.Context, _, _ string) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

// TestGroupSpendHungStoreFailsOpenWithinBudget: a store read that never
// returns must not hold the request. The check abandons it after
// groupSpendQueryBudget, admits (fail open), and logs the bypass loudly.
func TestGroupSpendHungStoreFailsOpenWithinBudget(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	log := logging.New(logging.ERROR)
	var buf bytes.Buffer
	log.Error.SetOutput(&buf)
	a.WithGroupSpend(blockingGroupSpendStore{}, log)

	start := time.Now()
	lease, err := a.Admit(context.Background(), groupRequest("org-h", "m",
		groupScope(testGID, nil, "100")))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("hung-store admit: %v, want fail-open admission", err)
	}
	_ = lease.Complete(context.Background(), 0)
	if elapsed > admitOperationBudget {
		t.Fatalf("hung-store admit took %v, want it bounded by the spend query budget (%v)", elapsed, groupSpendQueryBudget)
	}
	out := buf.String()
	if !strings.Contains(out, "monthly spend cap NOT enforced (fail open)") || !strings.Contains(out, context.DeadlineExceeded.Error()) {
		t.Fatalf("bypass log = %q, want a fail-open ERROR naming the deadline", out)
	}
}

// countedBlockingSpendStore models a blackholed Postgres that concurrent
// requests pile onto: the read never answers until its context is done, and
// every read is counted.
type countedBlockingSpendStore struct {
	mu    sync.Mutex
	calls int
}

func (s *countedBlockingSpendStore) GroupSpendExhausted(ctx context.Context, _, _ string) (bool, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	<-ctx.Done()
	return false, ctx.Err()
}

func (s *countedBlockingSpendStore) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestGroupSpendConcurrentMissSingleflight: N concurrent Admits on one capped
// group with a blocking store collapse to a single store call — concurrent
// cache misses share one in-flight read — and every request still admits.
func TestGroupSpendConcurrentMissSingleflight(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	store := &countedBlockingSpendStore{}
	a.WithGroupSpend(store, logging.New(logging.ERROR))

	const n = 20
	start := make(chan struct{})
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			<-start
			lease, err := a.Admit(context.Background(), groupRequest("org-sf", "m",
				groupScope(testGID, nil, "100")))
			if err == nil {
				err = lease.Complete(context.Background(), 0)
			}
			errs <- err
		}()
	}
	close(start)
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent admit %d: %v, want fail-open admission", i, err)
		}
	}
	if calls := store.Calls(); calls != 1 {
		t.Fatalf("store calls = %d, want 1 (concurrent misses share one in-flight read)", calls)
	}
}

// TestGroupSpendSharedBudgetAcrossGroups: many uncached capped groups against
// a store that never answers share ONE query budget — the request is held
// about one groupSpendQueryBudget in total, not one per group — and admits.
func TestGroupSpendSharedBudgetAcrossGroups(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	a.WithGroupSpend(blockingGroupSpendStore{}, logging.New(logging.ERROR))

	scopes := make([]GroupScope, 16)
	for i := range scopes {
		scopes[i] = groupScope(fmt.Sprintf("%032x", i+1), nil, "100")
	}
	start := time.Now()
	lease, err := a.Admit(context.Background(), groupRequest("org-sb", "m", scopes...))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("16-group hung-store admit: %v, want fail-open admission", err)
	}
	_ = lease.Complete(context.Background(), 0)
	if elapsed >= 2*groupSpendQueryBudget {
		t.Fatalf("16-group admit took %v, want under %v (one shared query budget)", elapsed, 2*groupSpendQueryBudget)
	}
}

// releasedSpendStore answers exhausted=true once released, or the context's
// error if the context ends first; entered signals the first read began.
type releasedSpendStore struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	calls   int
}

func (s *releasedSpendStore) GroupSpendExhausted(ctx context.Context, _, _ string) (bool, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// TestGroupSpendLeaderCancelDoesNotCancelCoalescedQuery: the leader's client
// disconnecting mid-read must not end the coalesced read. A waiter with a live
// context still gets the real verdict (the over-cap 429), from that one read.
func TestGroupSpendLeaderCancelDoesNotCancelCoalescedQuery(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	store := &releasedSpendStore{entered: make(chan struct{}), release: make(chan struct{})}
	a.WithGroupSpend(store, logging.New(logging.ERROR))

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		if lease, err := a.Admit(leaderCtx, groupRequest("org-lc", "m",
			groupScope(testGID, nil, "100"))); err == nil {
			_ = lease.Complete(context.Background(), 0)
		}
	}()
	<-store.entered
	cancelLeader()

	waiterErr := make(chan error, 1)
	go func() {
		lease, err := a.Admit(context.Background(), groupRequest("org-lc", "m",
			groupScope(testGID, nil, "100")))
		if err == nil {
			_ = lease.Complete(context.Background(), 0)
		}
		waiterErr <- err
	}()
	// Let the waiter join the in-flight read, then let the read answer.
	time.Sleep(20 * time.Millisecond)
	close(store.release)

	err := <-waiterErr
	<-leaderDone
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Dimension != "monthly_spend" || !rejected.Contractual {
		t.Fatalf("waiter admit: %v, want the 429 monthly_spend rejection from the coalesced read", err)
	}
	store.mu.Lock()
	calls := store.calls
	store.mu.Unlock()
	if calls != 1 {
		t.Fatalf("store calls = %d, want 1 (the waiter shares the leader's read)", calls)
	}
}

// TestGroupSpendParentCancelNotLoggedAsBypass: a canceled request context
// (client gone) makes the spend check moot — it must NOT log the throttled
// 'monthly spend cap NOT enforced' ERROR, which a genuine outage needs.
func TestGroupSpendParentCancelNotLoggedAsBypass(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	log := logging.New(logging.ERROR)
	var buf bytes.Buffer
	log.Error.SetOutput(&buf)
	a.WithGroupSpend(blockingGroupSpendStore{}, log)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The Admit itself fails on the canceled context (the Valkey reservation
	// cannot run); only the absence of the bypass line is asserted.
	_, _ = a.Admit(ctx, groupRequest("org-pc", "m",
		groupScope(testGID, nil, "100")))
	if out := buf.String(); strings.Contains(out, "monthly spend cap NOT enforced") {
		t.Fatalf("canceled-request bypass logged: %q, want no bypass ERROR (the check is moot)", out)
	}
}

// TestGroupSpendMonthBoundaryIsUTC: the month predicate is computed in UTC,
// matching the rater's UTC hour buckets, not in the session TimeZone.
func TestGroupSpendMonthBoundaryIsUTC(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(
		"window_start >= date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'")).
		WithArgs(testGID, "100").
		WillReturnRows(sqlmock.NewRows([]string{"exhausted"}).AddRow(true))

	exhausted, err := NewPostgresSpendStore(db).GroupSpendExhausted(context.Background(), testGID, "100")
	if err != nil {
		t.Fatalf("GroupSpendExhausted: %v", err)
	}
	if !exhausted {
		t.Fatalf("exhausted = false, want the row's verdict")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// TestGroupSpendCacheExpiresAtUTCMonthBoundary: an exhausted verdict cached
// just before the UTC month turns must not deny requests in the new month —
// the rollover invalidates it even though the TTL has not elapsed.
func TestGroupSpendCacheExpiresAtUTCMonthBoundary(t *testing.T) {
	a, store := spendTestAdmitter(t)
	store.exhausted = true
	now := time.Date(2026, time.October, 31, 23, 59, 50, 0, time.UTC)
	a.spendCache.now = func() time.Time { return now }
	scope := groupScope(testGID, nil, "100")
	ctx := context.Background()

	var rejected *Rejected
	if _, err := a.Admit(ctx, groupRequest("org-m", "m", scope)); !errors.As(err, &rejected) {
		t.Fatalf("pre-boundary admit: %v, want a monthly_spend rejection", err)
	}
	if _, err := a.Admit(ctx, groupRequest("org-m", "m", scope)); !errors.As(err, &rejected) {
		t.Fatalf("cached pre-boundary admit: %v, want a monthly_spend rejection", err)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (verdict cached inside the old month)", store.calls)
	}

	// Twenty seconds later, inside the TTL but in November: the new month's
	// spend is zero, so the cached verdict must be dropped and re-read.
	now = now.Add(20 * time.Second)
	store.exhausted = false
	lease, err := a.Admit(ctx, groupRequest("org-m", "m", scope))
	if err != nil {
		t.Fatalf("post-boundary admit: %v, want admission (new month)", err)
	}
	_ = lease.Complete(ctx, 0)
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2 (month rollover re-reads the store)", store.calls)
	}
}

// TestGroupSpendNoStoreFailsOpen: an admitter without a spend store (a
// serving-only install) admits requests carrying a spend cap — the bypass is
// the loud startup log's contract, not a per-request error.
func TestGroupSpendNoStoreFailsOpen(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	lease, err := a.Admit(context.Background(), groupRequest("org-n", "m",
		groupScope(testGID, nil, "100")))
	if err != nil {
		t.Fatalf("no-store admit: %v, want admission (spend check bypassed)", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

// TestGroupScopeMissingGroupIDFailsClosed: a scope carrying limits without a
// group id is a broken envelope, and broken trusted identity fails closed —
// distinct from the store-outage bypass.
func TestGroupScopeMissingGroupIDFailsClosed(t *testing.T) {
	a, _ := spendTestAdmitter(t)
	cap := int64(10)
	_, err := a.Admit(context.Background(), groupRequest("org-i", "m",
		GroupScope{Limits: RateLimits{Requests: &cap}}))
	if !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("anonymous group scope: %v, want ErrInvalidIdentity (fail closed)", err)
	}

	// A scope with nothing to enforce and no id is inert, not an error.
	lease, err := a.Admit(context.Background(), groupRequest("org-i", "m", GroupScope{}))
	if err != nil {
		t.Fatalf("inert anonymous scope: %v, want admission", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

// TestGroupScopesSettleThroughCompleteUsage: the group contract scope rides
// the lease like any other scope — CompleteUsage settles it through the same
// finishScript path, charging the window the engine-reported tokens (not the
// reservation). Generated cap 50: settle 30, then 20 — both admit; a third
// reservation that would push the window past 50 is rejected.
func TestGroupScopesSettleThroughCompleteUsage(t *testing.T) {
	a, _ := spendTestAdmitter(t)
	cap := int64(50)
	r := groupRequest("org-l", "m", GroupScope{GroupID: testGID,
		Limits: RateLimits{GeneratedTokens: &cap}})

	lease, err := a.Admit(context.Background(), r)
	if err != nil {
		t.Fatalf("first admit: %v", err)
	}
	if err := lease.Complete(context.Background(), 30); err != nil {
		t.Fatalf("first Complete: %v", err)
	}

	lease2, err := a.Admit(context.Background(), r)
	if err != nil {
		t.Fatalf("second admit (window at 30/50): %v, want admitted", err)
	}
	if err := lease2.Complete(context.Background(), 20); err != nil {
		t.Fatalf("second Complete: %v", err)
	}

	_, err = a.Admit(context.Background(), r)
	var rejected *Rejected
	if !errors.As(err, &rejected) || !rejected.Contractual {
		t.Fatalf("third admit (window at 50/50): %v, want a contractual rejection", err)
	}
}

// TestGroupScopesSettleZeroUsageRefundsReservation: the settlement test above
// cannot tell settlement from a reservation left charged (each request
// reserves 20 output tokens, so an unsettled window would also read 20, 40,
// 60). Here every request completes with zero generated tokens: against a cap
// of 50, five requests admit only if each settlement refunds its 20-token
// reservation — without settlement the third would be rejected at 60/50.
func TestGroupScopesSettleZeroUsageRefundsReservation(t *testing.T) {
	a, _ := spendTestAdmitter(t)
	cap := int64(50)
	r := groupRequest("org-z0", "m", GroupScope{GroupID: testGID,
		Limits: RateLimits{GeneratedTokens: &cap}})

	for i := 0; i < 5; i++ {
		lease, err := a.Admit(context.Background(), r)
		if err != nil {
			t.Fatalf("admit %d: %v, want admitted (each zero-usage Complete refunds the reservation)", i+1, err)
		}
		if err := lease.Complete(context.Background(), 0); err != nil {
			t.Fatalf("Complete %d: %v", i+1, err)
		}
	}
}

// TestGroupScopesSettleChargesActualUsage: settling 45 generated tokens leaves
// the window at 45/50, so the next 20-token reservation is rejected — even
// though an unsettled window (holding only the first 20-token reservation)
// would have fit it at 40/50.
func TestGroupScopesSettleChargesActualUsage(t *testing.T) {
	a, _ := spendTestAdmitter(t)
	cap := int64(50)
	r := groupRequest("org-l45", "m", GroupScope{GroupID: testGID,
		Limits: RateLimits{GeneratedTokens: &cap}})

	lease, err := a.Admit(context.Background(), r)
	if err != nil {
		t.Fatalf("first admit: %v", err)
	}
	if err := lease.Complete(context.Background(), 45); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	_, err = a.Admit(context.Background(), r)
	var rejected *Rejected
	if !errors.As(err, &rejected) || !rejected.Contractual || rejected.Scope != "group:"+testGID {
		t.Fatalf("admit after settling 45/50: %v, want a contractual group rejection", err)
	}
}

// TestSpendFailureLogThrottle: the first fail-open line of an incident writes
// immediately; repeats inside spendFailureLogInterval are counted and silent;
// the first call after the interval writes and carries the suppressed count.
func TestSpendFailureLogThrottle(t *testing.T) {
	log := logging.New(logging.ERROR)
	var buf bytes.Buffer
	log.Error.SetOutput(&buf)
	now := time.Now()
	l := &spendFailureLog{now: func() time.Time { return now }}

	l.logf(log, "bypass %d", 1)
	if got := strings.Count(buf.String(), "bypass"); got != 1 {
		t.Fatalf("lines after first logf = %d (%q), want 1", got, buf.String())
	}
	if strings.Contains(buf.String(), "suppressed") {
		t.Fatalf("first line = %q, want no suppressed count", buf.String())
	}

	for i := 0; i < 3; i++ {
		now = now.Add(spendFailureLogInterval / 4)
		l.logf(log, "bypass %d", i+2)
	}
	if got := strings.Count(buf.String(), "bypass"); got != 1 {
		t.Fatalf("lines inside the interval = %d (%q), want still 1 (repeats are silent)", got, buf.String())
	}

	now = now.Add(spendFailureLogInterval)
	l.logf(log, "bypass %d", 5)
	out := buf.String()
	if got := strings.Count(out, "bypass"); got != 2 {
		t.Fatalf("lines after the interval = %d (%q), want 2", got, out)
	}
	if !strings.Contains(out, "bypass 5 (+3 similar suppressed)") {
		t.Fatalf("post-interval line = %q, want it to carry the 3 suppressed repeats", out)
	}
}

// TestGroupSpendStoreErrorLogsOnce: a plain (non-timeout) store error writes
// exactly one fail-open ERROR line for the incident; the negative-cached
// repeats do not add more.
func TestGroupSpendStoreErrorLogsOnce(t *testing.T) {
	a, store := spendTestAdmitter(t)
	log := logging.New(logging.ERROR)
	var buf bytes.Buffer
	log.Error.SetOutput(&buf)
	a.WithGroupSpend(store, log)
	store.err = errors.New("postgres unreachable")

	for i := 0; i < 3; i++ {
		lease, err := a.Admit(context.Background(), groupRequest("org-lo", "m",
			groupScope(testGID, nil, "100")))
		if err != nil {
			t.Fatalf("store-error admit %d: %v, want fail-open admission", i+1, err)
		}
		_ = lease.Complete(context.Background(), 0)
	}
	out := buf.String()
	if got := strings.Count(out, "monthly spend cap NOT enforced (fail open)"); got != 1 {
		t.Fatalf("fail-open ERROR lines = %d (%q), want exactly 1", got, out)
	}
	if !strings.Contains(out, "postgres unreachable") {
		t.Fatalf("fail-open line = %q, want it to name the store error", out)
	}
}
