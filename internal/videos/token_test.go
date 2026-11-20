package videos

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/saturncloud/phoebe/internal/atlastoken"
)

// staticToken is an atlastoken.Source that always has the same token.
type staticToken string

func (s staticToken) Token() (string, error) { return string(s), nil }

type noToken struct{}

func (noToken) Token() (string, error) { return "", atlastoken.ErrUnavailable }

// With no usable token (missing, expired, malformed), nothing is sent to
// Atlas, and the error is temporary (not an Atlas refusal), so the job is
// retried rather than failed.
func TestAtlasBlobWithNoUsableTokenSendsNothing(t *testing.T) {
	var calls atomic.Int32
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer atlas.Close()
	b, err := NewAtlasBlob(atlas.URL, noToken{}, "tf-videos")
	if err != nil {
		t.Fatal(err)
	}
	err = b.Register(context.Background(), testOrg, testJob, "dep1", "")
	if !errors.Is(err, atlastoken.ErrUnavailable) {
		t.Fatalf("Register = %v; want ErrUnavailable", err)
	}
	if _, permanent := storageRefusedPermanently(err); permanent {
		t.Fatal("a missing token was treated as a permanent refusal")
	}
	if calls.Load() != 0 {
		t.Fatalf("Atlas called %d times without a token", calls.Load())
	}
}
