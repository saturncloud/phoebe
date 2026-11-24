package proxy

import (
	"bytes"
	"net/http"
	"sort"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
)

// RETRIEVING A VIDEO MADE ON ANOTHER CLUSTER (issue #71, tenant-domains design
// section 6: "A retrieval for a video made on another cluster is forwarded to
// that cluster's org host, as in section 5"; battery round 3, R3-1).
//
// A create for a video endpoint served elsewhere is forwarded, so its job is
// stored by that cluster's phoebe-videos. Read, download and delete of one
// video by id therefore first ask this cluster's phoebe-videos (under the
// host's org, as always); when it has no such video they are forwarded, with
// forwardOrgHost, to the org host of each other cluster that serves one of
// this org's video endpoints, and the first answer that is not a 404 is
// returned. When every cluster answers 404 the caller gets this cluster's own
// 404, byte for byte, so the answer does not depend on where a video might
// be. The serving cluster checks the key and the org again, so another org's
// id is a 404 there too. A request that was already forwarded here is never
// forwarded again. An endpoint-scoped key reaches only its own endpoint's
// cluster. The org-wide list stays this cluster's only (design section 6
// defers merging per-cluster lists).
//
// The other clusters are found from this org's registry rows: the distinct
// forward_url of its dedicated video endpoints (non-empty video_limits) that
// run elsewhere. A video whose endpoint was deleted and that lives on another
// cluster cannot be found this way (its row is gone); it is a 404.

// maxVideoBodyCapture bounds how much of a 404 answer is kept to replay it.
const maxVideoBodyCapture = 64 << 10

// missCapture lets a 404 answer be held back while other clusters are asked.
// Any other status is passed through to the real writer as it arrives
// (headers, body and flushes), so a download still streams.
type missCapture struct {
	w         http.ResponseWriter
	header    http.Header
	status    int
	passed    bool
	body      bytes.Buffer
	truncated bool
}

func newMissCapture(w http.ResponseWriter) *missCapture {
	return &missCapture{w: w, header: http.Header{}}
}

func (c *missCapture) Header() http.Header {
	if c.passed {
		return c.w.Header()
	}
	return c.header
}

func (c *missCapture) WriteHeader(status int) {
	if c.status != 0 {
		return
	}
	c.status = status
	if status == http.StatusNotFound {
		return
	}
	c.passed = true
	dst := c.w.Header()
	for k, v := range c.header {
		dst[k] = v
	}
	c.w.WriteHeader(status)
}

func (c *missCapture) Write(b []byte) (int, error) {
	if c.status == 0 {
		c.WriteHeader(http.StatusOK)
	}
	if c.passed {
		return c.w.Write(b)
	}
	room := maxVideoBodyCapture - c.body.Len()
	if room > 0 {
		if len(b) > room {
			c.body.Write(b[:room])
			c.truncated = true
		} else {
			c.body.Write(b)
		}
	} else if len(b) > 0 {
		c.truncated = true
	}
	return len(b), nil
}

// Flush passes through only once the answer is committed (not a 404).
func (c *missCapture) Flush() {
	if c.passed {
		if f, ok := c.w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// Unwrap lets http.ResponseController reach the real writer (deadlines).
// Flushing goes through Flush above, which holds a 404 back.
func (c *missCapture) Unwrap() http.ResponseWriter { return c.w }

// missed reports that the answer was a 404 that has not been written.
func (c *missCapture) missed() bool { return c.status == http.StatusNotFound && !c.passed }

// replay writes a held-back 404 to the real writer.
func (c *missCapture) replay() {
	dst := c.w.Header()
	for k, v := range c.header {
		dst[k] = v
	}
	if c.truncated {
		dst.Del("Content-Length")
	}
	c.w.WriteHeader(http.StatusNotFound)
	_, _ = c.w.Write(c.body.Bytes())
}

// videoForwardTargets returns the rows to forward a by-id video request to,
// one per distinct forward_url, in a fixed order. scope is the endpoint a
// narrowed key is limited to ("" for none).
func (s *Server) videoForwardTargets(r *http.Request, id identity.Identity, scope string) ([]gateway.DedicatedEndpoint, error) {
	rows, err := s.orgHosts.resolver.ListDedicated(r.Context(), id.OrgID)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []gateway.DedicatedEndpoint
	for _, row := range rows {
		if row.OrgID != id.OrgID || row.VideoLimits == "" || !row.Forwards() {
			continue
		}
		if scope != "" && row.ResourceID != scope {
			continue
		}
		if seen[row.ForwardURL] {
			continue
		}
		seen[row.ForwardURL] = true
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ForwardURL < out[j].ForwardURL })
	return out, nil
}

// serveOrgHostVideoByID answers GET /v1/videos/{id}, GET .../content and
// DELETE /v1/videos/{id}: this cluster first, then the other clusters that
// serve the org's video endpoints (see the file comment).
func (s *Server) serveOrgHostVideoByID(w http.ResponseWriter, r *http.Request, id identity.Identity, requestID, scope string, local func(http.ResponseWriter, *http.Request), alreadyForwarded bool) {
	// handOffVideo removes the caller's credential before phoebe-videos; a
	// forward needs it, so keep it for the retry.
	auth := r.Header.Values("Authorization")
	var held *missCapture
	if local != nil {
		held = newMissCapture(w)
		local(held, r)
		if !held.missed() {
			return
		}
	}
	notFound := func() {
		if held != nil {
			held.replay()
			return
		}
		writeClientError(w, requestID, http.StatusNotFound, orgHostVideoNotFound)
	}
	if alreadyForwarded {
		// Served or refused where it lands: never forwarded again.
		notFound()
		return
	}
	targets, err := s.videoForwardTargets(r, id, scope)
	if err != nil {
		s.log.Error.Printf("videos: org-host retrieval targets request_id=%s org_id=%s: %v", requestID, id.OrgID, err)
		notFound()
		return
	}
	if len(targets) == 0 {
		notFound()
		return
	}
	s.log.Info.Printf("videos: org-host retrieval request_id=%s org_id=%s not on this cluster; asking %d other cluster(s)", requestID, id.OrgID, len(targets))
	r.Header.Del("Authorization")
	for _, v := range auth {
		r.Header.Add("Authorization", v)
	}
	r.Body = http.NoBody
	r.ContentLength = 0
	for _, ep := range targets {
		attempt := newMissCapture(w)
		s.forwardOrgHost(attempt, r, id, ep, false)
		if !attempt.missed() {
			return
		}
	}
	notFound()
}
