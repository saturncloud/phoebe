// Package gateway resolves TF shared-inference gateway requests to their
// tf_model row.
//
// THE GATEWAY CONTRACT (ratified with Atlas, built in parallel on both sides):
// the TF shared mode drops per-model subdomains for a SINGLE gateway host
// routed to phoebe. On that route Atlas's middleware injects
// `X-Saturn-Gateway: "true"` and `X-Saturn-Org-Id` (both anti-spoofed: only
// the gateway route sets them, per-resource routes strip them) and injects
// NONE of the per-resource routing headers (X-Saturn-Upstream /
// X-Saturn-Resource-Id / X-Saturn-Served-Model). The request-body `model=`
// selects the model, so phoebe — not a per-subdomain middleware — must map
// (org, model) to the served resource: its billing identity (resource id,
// base_model, adapter, serving_mode) and its Dynamo graph.
//
// The lookup source is Atlas's `tf_model` table (the same Atlas Postgres
// phoebe already writes billing_event / rated_usage to): one row per served
// model endpoint, carrying exactly the identity this package returns. The
// match on served_model_name is BYTE-EXACT — the binding contract is case-
// and byte-exact, mirroring the model-binding allow-list check on the
// subdomain path (the served-name is claimed verbatim and Dynamo routes on it
// verbatim).
//
// FAIL CLOSED, both ways: a lookup MISS is ErrNotFound (the proxy 404s with a
// generic body — no model echo, no existence oracle), and a DB failure is a
// plain error (the proxy 503s — phoebe never serves traffic it cannot
// attribute). Resolution here IS the tenancy boundary for gateway traffic:
// the org filter in the SQL is what stops org B resolving org A's model.
package gateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNotFound reports that no tf_model row matches (org, served model name) —
// including rows that exist but are not addressable (no serving graph yet).
// The proxy maps it to a generic 404: the caller learns nothing about which
// models exist.
var ErrNotFound = errors.New("gateway: model not found for org")

// Resolution is the resolved identity of a gateway request's model — exactly
// the fields the per-subdomain middleware would have injected as headers, so a
// gateway-routed billing event is indistinguishable from a header-injected one.
type Resolution struct {
	// ResourceID is the tf_model row id — the billing resource id
	// (billing_event.resource_id), exactly as X-Saturn-Resource-Id would carry.
	ResourceID string
	// BaseModel is the HF base id — the catalog price key (C4), as
	// X-Saturn-Base-Model would carry.
	BaseModel string
	// Adapter is the fine-tune checkpoint artifact id ("" for a base-model
	// endpoint), as X-Saturn-Adapter would carry. Its presence triggers the
	// fine-tune premium at rating.
	Adapter string
	// ServingMode is the serving-mode SKU axis ("shared" | "dedicated"), as
	// X-Saturn-Serving-Mode would carry.
	ServingMode string
	// GraphK8sName is the k8s name of the DynamoGraphDeployment serving this
	// model. The proxy composes the upstream from it:
	// <graph>-frontend.<namespace>.svc.cluster.local:<port>. Never empty on a
	// successful resolution (a row with no graph resolves as ErrNotFound).
	GraphK8sName string
}

// Resolver maps a (org id, request-body model=) pair to its Resolution.
type Resolver interface {
	// Resolve returns the Resolution for the org's model, ErrNotFound when the
	// org has no such addressable model, or another error on lookup failure
	// (the caller must fail closed — never serve unattributed).
	Resolve(ctx context.Context, orgID, model string) (Resolution, error)
}

// resolveQuery looks up the org's model by served name in Atlas's tf_model.
//
//   - org_id = $1 is the TENANCY boundary: org B's request can never resolve
//     org A's row, whatever model= it sends. For a shared model org_id is the
//     TENANT org (the adapter's owner), which is exactly the org the gateway
//     middleware stamps from the authorized API key's route.
//   - served_model_name = $2 is BYTE-EXACT (Postgres text equality under the
//     default deterministic collation is byte-wise) — the binding contract is
//     case- and byte-exact, like the subdomain path's allow-list check.
//   - served_model_name is NOT unique per org for BASE-MODEL shared endpoints
//     (every tenant base endpoint of one base serves the base id itself, and
//     one org may create several). Such duplicate rows are the same product —
//     same base, mode, and (per cluster) graph — differing only in which row
//     the usage attributes to, so the pick must simply be DETERMINISTIC:
//     ORDER BY created_at, id takes the oldest row, never a per-request
//     coin-flip between attribution targets.
//   - graph_k8s_name IS the addressability: a row without one (not yet
//     provisioned / being torn down) cannot be routed, and reporting it
//     differently from a miss would leak model existence — so the scanner
//     folds it into ErrNotFound (logged distinctly server-side by the caller's
//     resolver error, not distinguishable by the client).
//
// NOTE on placement: the row also carries a `cluster`, and dedicated rows'
// graphs live in org namespaces — but phoebe composes every gateway upstream
// inside the single configured gateway namespace. The operating assumption
// (documented on the gateway settings) is that every graph reachable through
// this phoebe's gateway host lives in that namespace; a row whose graph lives
// elsewhere fails at dial (502), never misroutes to a guessed namespace.
const resolveQuery = `
SELECT id, base_model, COALESCE(checkpoint_artifact_id, ''), serving_mode, COALESCE(graph_k8s_name, '')
FROM tf_model
WHERE org_id = $1 AND served_model_name = $2
ORDER BY created_at ASC, id ASC
LIMIT 1`

// PGResolver resolves against Atlas's tf_model table over database/sql (pgx
// stdlib), the same connection style as the drainer/rater stores.
type PGResolver struct {
	db *sql.DB
}

// NewPGResolver wraps an open *sql.DB (the caller owns opening/closing it).
func NewPGResolver(db *sql.DB) *PGResolver {
	return &PGResolver{db: db}
}

// Resolve implements Resolver against tf_model. sql.ErrNoRows and a
// graph-less row both map to ErrNotFound (see resolveQuery); any other error
// is returned wrapped for the caller's fail-closed 503.
func (p *PGResolver) Resolve(ctx context.Context, orgID, model string) (Resolution, error) {
	var r Resolution
	err := p.db.QueryRowContext(ctx, resolveQuery, orgID, model).Scan(
		&r.ResourceID, &r.BaseModel, &r.Adapter, &r.ServingMode, &r.GraphK8sName,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Resolution{}, ErrNotFound
	case err != nil:
		return Resolution{}, fmt.Errorf("gateway: resolve model for org %s: %w", orgID, err)
	}
	if r.GraphK8sName == "" {
		// Exists but not addressable (no serving graph). Folded into ErrNotFound
		// so a client cannot distinguish "no such model" from "model not yet
		// servable" (no existence oracle); the miss is still diagnosable
		// server-side from Atlas's own tf_model state.
		return Resolution{}, ErrNotFound
	}
	return r, nil
}
