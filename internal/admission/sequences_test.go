package admission

import (
	"context"
	"errors"
	"testing"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/testutil"
)

// n / best_of make the engine generate up to max_tokens for each sequence, so
// the contract generated-token pre-check is against max_tokens x sequences: a
// request that can generate more than the whole window is refused outright.
func TestContractGeneratedPreCheckChargesEveryOutputSequence(t *testing.T) {
	ctx := context.Background()
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: limits(10)})
	req := request("a", "m")
	req.ReservedOutputTokens, req.OutputSequences = 100, 2
	req.OwnerLimits = RateLimits{GeneratedTokens: testutil.Ptr[int64](100)}
	_, err := a.Admit(ctx, req)
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Scope != "contract_owner" ||
		rejected.Dimension != "generated_tokens_exceeds_limit" || !rejected.Unsatisfiable || !rejected.Contractual {
		t.Fatalf("err=%v, want contract_owner generated_tokens_exceeds_limit for 100 x 2 against a 100 window", err)
	}
	req.OutputSequences = 1
	lease, err := a.Admit(ctx, req)
	if err != nil {
		t.Fatalf("100 x 1 fits the 100 window: %v", err)
	}
	_ = lease.Complete(ctx, 0)
}

// The reserved-output counter and the generated-token reservation are both
// output x sequences, and release returns exactly that, leaving zero.
func TestReleaseReturnsOutputSequenceReservationsToZero(t *testing.T) {
	ctx := context.Background()
	a, mr := testAdmitter(t, config.AdmissionSettings{Platform: limits(10)})
	req := request("a", "m") // 20 output
	req.OutputSequences = 3
	req.OwnerLimits = RateLimits{GeneratedTokens: testutil.Ptr[int64](1000)}
	for _, finish := range []string{"complete", "abandon via unknown usage"} {
		lease, err := a.Admit(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		for _, sc := range a.scopes(req) {
			for _, dim := range []string{"output", "genreserved"} {
				if got := mr.HGet(a.counters, sc.ID+"|"+dim); got != "60" {
					t.Fatalf("%s %s=%q, want 20 x 3 = 60", sc.Name, dim, got)
				}
			}
		}
		if finish == "complete" {
			err = lease.Complete(ctx, 0)
		} else {
			err = lease.CompleteUnknownUsage(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, sc := range a.scopes(req) {
			for _, dim := range []string{"output", "genreserved"} {
				if got := mr.HGet(a.counters, sc.ID+"|"+dim); got != "" {
					t.Fatalf("%s: %s %s=%q after release, want zero (field removed)", finish, sc.Name, dim, got)
				}
			}
		}
	}
}

// An operator maxReservedOutputTokens cap bounds decode work, which is
// max_tokens x sequences; a request whose product passes what is left (but
// fits the whole cap) waits.
func TestReservedOutputCapChargesEveryOutputSequence(t *testing.T) {
	ctx := context.Background()
	l := limits(10)
	l.MaxReservedOutputTokens = testutil.Ptr[int64](50)
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
	held := request("held", "m") // 20 output x 1
	holder, err := a.Admit(ctx, held)
	if err != nil {
		t.Fatal(err)
	}
	req := request("a", "m") // 20 output
	req.OutputSequences = 2
	_, err = a.Admit(ctx, req)
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Scope != "platform" || rejected.Dimension != "output" || rejected.Unsatisfiable {
		t.Fatalf("err=%v, want a retryable platform output rejection for 20 + 20 x 2 against a cap of 50", err)
	}
	_ = holder.Complete(ctx, 0)
	lease, err := a.Admit(ctx, req)
	if err != nil {
		t.Fatalf("20 x 2 fits a cap of 50 once the holder is released: %v", err)
	}
	_ = lease.Complete(ctx, 0)
}

// Q24: a reserved-output cap is a load limit. Output x sequences above the
// whole cap is admitted alone and refused as retryable while the counter is
// in use.
func TestRequestOutputAboveReservedOutputCapIsAdmittedAlone(t *testing.T) {
	ctx := context.Background()
	l := limits(10)
	l.MaxReservedOutputTokens = testutil.Ptr[int64](64)
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
	req := request("a", "m")
	req.ReservedOutputTokens, req.OutputSequences = 1, 128
	alone, err := a.Admit(ctx, req)
	if err != nil {
		t.Fatalf("1 x 128 against a cap of 64, alone: %v", err)
	}
	_, err = a.Admit(ctx, req)
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Scope != "platform" || rejected.Dimension != "output" || rejected.Unsatisfiable {
		t.Fatalf("err=%+v, want a retryable platform output rejection while the counter is in use", rejected)
	}
	_ = alone.Complete(ctx, 0)
}

// A request that ends without a usage block is charged its whole reservation,
// max_tokens x sequences, against the contract generated-token window.
func TestUnknownUsageChargesEveryOutputSequence(t *testing.T) {
	ctx := context.Background()
	a, _ := testAdmitter(t, config.AdmissionSettings{Platform: limits(10)})
	contract := RateLimits{GeneratedTokens: testutil.Ptr[int64](100)}
	req := request("a", "m") // 20 output
	req.OutputSequences = 3
	req.OwnerLimits = contract
	lease, err := a.Admit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.CompleteUnknownUsage(ctx); err != nil {
		t.Fatal(err)
	}
	// 60 is now charged: 41 more passes the 100 window, 40 fits it.
	next := request("a", "m")
	next.OwnerLimits = contract
	next.ReservedOutputTokens = 41
	_, err = a.Admit(ctx, next)
	var rejected *Rejected
	if !errors.As(err, &rejected) || rejected.Scope != "contract_owner" || rejected.Dimension != "generated_tokens" {
		t.Fatalf("err=%v, want contract_owner generated_tokens: unknown usage must charge 20 x 3 = 60", err)
	}
	next.ReservedOutputTokens = 40
	second, err := a.Admit(ctx, next)
	if err != nil {
		t.Fatalf("60 charged + 40 fits the 100 window: %v", err)
	}
	_ = second.Complete(ctx, 0)
}
