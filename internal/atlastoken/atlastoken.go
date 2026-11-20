// Package atlastoken gives a phoebe service its Atlas service token: the
// standard Atlas token of its own service identity, scoped to the one family
// of /api/internal routes it calls (saturn pdc/service_tokens.py).
//
// Atlas is the issuer and the renewer. Its service-tokens job keeps the
// token in a Secret that the pod mounts as a file, and replaces it when half
// its 24-hour life has gone; the replaced token stays valid until its own
// expiry. So the client's refresh is re-reading the file: File reads it
// before every call, which picks up a new token with no restart, and costs one
// small read per call to Atlas.
//
// It fails closed. A token that is missing, malformed, expired or within
// expirySkew of expiry is never sent; the call is refused here and an error is
// logged. There is no fallback to any other credential.
package atlastoken

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// EnvFile names the environment variable that holds the token file's path.
// CONTRACT with the phoebe chart.
const EnvFile = "PHOEBE_ATLAS_TOKEN_FILE"

// expirySkew is how close to its expiry a token is no longer sent: enough for
// the call to reach Atlas, and for some clock difference.
const expirySkew = 60 * time.Second

// warnBelow is how little life left is worth a warning. Atlas replaces the
// token with 12 hours left, so less than 6 means its job has failed for at
// least 6 hours.
const warnBelow = 6 * time.Hour

// logEvery limits the repeated warning and error lines to one per interval.
const logEvery = time.Minute

// ErrUnavailable is the error Token returns when there is no usable token.
// The call it was for was not sent.
var ErrUnavailable = errors.New("no usable Atlas service token")

// Source returns the token to send on one call to Atlas.
type Source interface {
	Token() (string, error)
}

// File reads the token from a file Atlas keeps current.
type File struct {
	path string
	log  *logging.Logger
	now  func() time.Time

	mu       sync.Mutex
	lastWarn time.Time
	lastErr  time.Time
}

// NewFile returns a Source for path. The file need not exist yet: on a new
// install the pod can start before Atlas's job writes the Secret, and the
// kubelet fills the mounted file in when it does.
func NewFile(path string, log *logging.Logger) (*File, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("atlas token: %s is empty", EnvFile)
	}
	return &File{path: path, log: log, now: time.Now}, nil
}

// Token reads the file and returns the token if it may be sent now.
func (f *File) Token() (string, error) {
	raw, err := os.ReadFile(f.path)
	if err != nil {
		return "", f.refuse(fmt.Errorf("%w: read %s: %v", ErrUnavailable, f.path, err))
	}
	token := strings.TrimSpace(string(raw))
	exp, err := expiry(token)
	if err != nil {
		return "", f.refuse(fmt.Errorf("%w: %s: %v", ErrUnavailable, f.path, err))
	}
	left := exp.Sub(f.now())
	if left <= expirySkew {
		return "", f.refuse(fmt.Errorf("%w: the token in %s expired at %s; Atlas's service-tokens job has not replaced it",
			ErrUnavailable, f.path, exp.UTC().Format(time.RFC3339)))
	}
	if left < warnBelow {
		f.mu.Lock()
		if f.now().Sub(f.lastWarn) >= logEvery {
			f.lastWarn = f.now()
			f.log.Warn.Printf("atlas token: the token in %s expires in %s; Atlas's service-tokens job should have replaced it",
				f.path, left.Round(time.Minute))
		}
		f.mu.Unlock()
	}
	return token, nil
}

func (f *File) refuse(err error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.now().Sub(f.lastErr) >= logEvery {
		f.lastErr = f.now()
		f.log.Error.Printf("atlas token: REFUSING to call Atlas: %v", err)
	}
	return err
}

// expiry reads a JWT's exp claim. The signature is not checked here: Atlas
// checks it. This only decides whether the token is worth sending.
func expiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return time.Time{}, errors.New("not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, errors.New("not a JWT: payload is not base64url")
	}
	var claims struct {
		Exp *json.Number `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, errors.New("not a JWT: payload is not JSON")
	}
	if claims.Exp == nil {
		// Every Atlas service token expires; one that does not is not one.
		return time.Time{}, errors.New("the token has no expiry")
	}
	sec, err := claims.Exp.Int64()
	if err != nil {
		return time.Time{}, errors.New("the token's expiry is not a whole number")
	}
	return time.Unix(sec, 0), nil
}
