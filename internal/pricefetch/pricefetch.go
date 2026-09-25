// Package pricefetch is the ONE client for the manager's token-price endpoint.
//
// The manager (saturn-aws-manager) is the authoritative source of BOTH live and
// historical prices: model_token_price is effective-dated (a tstzrange with a
// no-overlap constraint, append-only reprices), so "what did this model cost at
// time T" is a question it can always answer. phoebe keeps NO price history of its
// own — it asks.
//
// The one caller:
//   - cmd/rater asks for the prices EFFECTIVE DURING EACH HOUR it rates (non-zero
//     asOf), which is what makes re-rating an old hour idempotent: the hour always
//     resolves to the rates that were in force during it, however many times prices
//     have changed since.
//
// There is no local price file and no last-good fallback: the manager is the only
// price source, so a manager outage is a rating outage (the trailing-24h re-rate
// window catches up).
package pricefetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
	"time"
)

// TokenPricesPath is the endpoint path on the manager. The customer is identified
// by the auth token, so there is no per-customer path component.
const TokenPricesPath = "/customer/token-prices"

// VersionHeader carries the manager's deterministic content hash of the served
// price set. It rides OUT-OF-BAND (not inside the YAML) by design — the YAML's own
// `version:` field is the SCHEMA version, a different thing.
const VersionHeader = "X-Saturn-Price-Version"

// MaxBodyBytes caps the served price file. The file is small (KBs); 8 MiB is a
// generous ceiling that still bounds memory against a misbehaving endpoint.
const MaxBodyBytes = 8 << 20

// DefaultTimeout bounds one fetch (connect + read). The price file is small; a long
// stall means the manager is unhealthy.
const DefaultTimeout = 30 * time.Second

// Client fetches price files from one manager with one customer token.
type Client struct {
	ManagerURL string
	Token      string
	Timeout    time.Duration
	// HTTPClient is optional; tests inject one. Timeout applies when it is nil.
	HTTPClient *http.Client
}

// Fetch GETs the price file and returns the body plus its X-Saturn-Price-Version.
//
// A non-zero asOf requests the prices effective AT THAT INSTANT (?at=<RFC3339>)
// instead of the current ones.
//
// Fails closed in every ambiguous case: a non-200 (including the endpoint's own 503
// for no-plan / incomplete-card), an over-cap body (refused rather than silently
// truncated — a truncated valid-YAML prefix would be a partial price book), and a
// 200 missing the version header (the manager always sets it, so its absence means
// something is wrong: a proxy stripping it, or the wrong endpoint answering).
func (c Client) Fetch(ctx context.Context, asOf time.Time) ([]byte, string, error) {
	if c.ManagerURL == "" {
		return nil, "", fmt.Errorf("manager URL is empty")
	}
	if c.Token == "" {
		return nil, "", fmt.Errorf("customer auth token is empty (the manager will not serve prices without it)")
	}
	url := c.ManagerURL + TokenPricesPath
	if !asOf.IsZero() {
		// RFC3339 in UTC: an unambiguous instant. The manager reads a naive
		// timestamp as UTC, but we never rely on that — always send the offset.
		url += "?at=" + neturl.QueryEscape(asOf.UTC().Format(time.RFC3339Nano))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}
	// Same Authorization scheme as the rest of the install->manager direction.
	req.Header.Set("Authorization", "token "+c.Token)

	httpClient := c.HTTPClient
	if httpClient == nil {
		timeout := c.Timeout
		if timeout <= 0 {
			timeout = DefaultTimeout
		}
		httpClient = &http.Client{Timeout: timeout}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Read ONE byte past the cap so an exactly-at-cap body is distinguishable from
	// one that overran it.
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("read response from %s: %w", url, err)
	}
	if len(body) > MaxBodyBytes {
		return nil, "", fmt.Errorf("price file from %s exceeds %d bytes (refusing a possibly-truncated body)", url, MaxBodyBytes)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("GET %s: status %d (not serving prices)", url, resp.StatusCode)
	}
	version := strings.TrimSpace(resp.Header.Get(VersionHeader))
	if version == "" {
		return nil, "", fmt.Errorf("GET %s: 200 response is missing the %s header (the manager always sets it; refusing an unversioned price file)", url, VersionHeader)
	}
	return body, version, nil
}
