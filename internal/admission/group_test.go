package admission

import (
	"context"
	"errors"
	"testing"

	"github.com/saturncloud/phoebe/internal/config"
)

const testGID = "a1b2c3d4e5f60718293a4b5c6d7e8f90"

// groupScope builds one GroupScope. rate of nil leaves every rate unset
// (unlimited); a non-nil rate installs all four dimensions to the same value.
func groupScope(gid string, rate *int64) GroupScope {
	gs := GroupScope{GroupID: gid}
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
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	// Only the requests window is capped, so the request's own estimates
	// (the helper's 10-in/20-out shape) can never trip the unsatisfiable
	// check: the third request is rejected by the window counter itself.
	reqCap := int64(2)
	scope := GroupScope{GroupID: testGID, Limits: RateLimits{Requests: &reqCap}}

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

// TestGroupTotalPromptTokensWindowEnforced: a group scope may cap only the
// total-prompt window, leaving the other rate fields nil (unlimited). Unlike
// the request-count window, token windows charge actual usage at settlement,
// so each request settles its 10-token input estimate: two requests fill a
// 20-token cap and the next reservation is a contractual rejection naming
// the group scope.
func TestGroupTotalPromptTokensWindowEnforced(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	promptCap := int64(20)
	r := groupRequest("org-p", "m", GroupScope{GroupID: testGID,
		Limits: RateLimits{TotalPromptTokens: &promptCap}})

	for i := 0; i < 2; i++ {
		lease, err := a.Admit(context.Background(), r)
		if err != nil {
			t.Fatalf("admit %d: %v, want admitted", i+1, err)
		}
		if err := lease.CompleteUsage(context.Background(), Usage{TotalPromptTokens: 10}); err != nil {
			t.Fatalf("CompleteUsage %d: %v", i+1, err)
		}
	}

	_, err := a.Admit(context.Background(), r)
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
	if rejected.Dimension != "total_prompt_tokens" {
		t.Fatalf("rejection dimension = %q, want the total-prompt window", rejected.Dimension)
	}
}

// TestGroupRateZeroCapBlocksEveryRequest: an explicit 0 is a zero cap, not
// unlimited — the same R4 sentinel as every other contract scope.
func TestGroupRateZeroCapBlocksEveryRequest(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	zero := int64(0)
	_, err := a.Admit(context.Background(), groupRequest("org-z", "m",
		groupScope(testGID, &zero)))
	var rejected *Rejected
	if !errors.As(err, &rejected) || !rejected.Contractual {
		t.Fatalf("zero-capped group admit: %v, want a contractual rejection", err)
	}
}

// TestGroupRateNilLimitIsUnlimited: a group scope whose rate fields are all
// nil contributes no contract scope (unlimited), so requests flow freely.
func TestGroupRateNilLimitIsUnlimited(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	for i := 0; i < 3; i++ {
		lease, err := a.Admit(context.Background(), groupRequest("org-u", "m",
			groupScope(testGID, nil)))
		if err != nil {
			t.Fatalf("admit %d: %v, want admitted (nil limits are unlimited)", i+1, err)
		}
		_ = lease.Complete(context.Background(), 0)
	}
}

// TestGroupScopeMissingGroupIDFailsClosed: a scope carrying limits without a
// group id is a broken envelope, and broken trusted identity fails closed —
// distinct from the store-outage bypass.
func TestGroupScopeMissingGroupIDFailsClosed(t *testing.T) {
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	reqCap := int64(10)
	_, err := a.Admit(context.Background(), groupRequest("org-i", "m",
		GroupScope{Limits: RateLimits{Requests: &reqCap}}))
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
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	genCap := int64(50)
	r := groupRequest("org-l", "m", GroupScope{GroupID: testGID,
		Limits: RateLimits{GeneratedTokens: &genCap}})

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
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	genCap := int64(50)
	r := groupRequest("org-z0", "m", GroupScope{GroupID: testGID,
		Limits: RateLimits{GeneratedTokens: &genCap}})

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
	a, _ := testAdmitter(t, config.AdmissionSettings{})
	genCap := int64(50)
	r := groupRequest("org-l45", "m", GroupScope{GroupID: testGID,
		Limits: RateLimits{GeneratedTokens: &genCap}})

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
