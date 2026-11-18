package videos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Blob is the object store the outputs live in: the install's Token Factory
// bucket (TF_ADAPTER_STORE_BUCKET) under the video prefix (design 6.1).
// Objects are only ever read back through the API, streamed by this service
// with the customer's key checked; no presigned URL is ever minted, because a
// presigned URL can be read by anyone who has it (design 3.3).
type Blob interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	Delete(ctx context.Context, key string) error
}

// ErrBlobNotFound is Get on a missing object.
var ErrBlobNotFound = errors.New("object not found")

// Keys lays out one job's objects: <prefix>/<org_id>/<video_id>/<name>. The
// org id in the key makes per-org deletion and auditing a prefix operation.
type Keys struct{ Prefix string }

func (k Keys) Input(orgID, jobID, ext string) string { return k.join(orgID, jobID, "input."+ext) }
func (k Keys) Video(orgID, jobID string) string      { return k.join(orgID, jobID, "video.mp4") }
func (k Keys) Thumbnail(orgID, jobID string) string  { return k.join(orgID, jobID, "thumbnail.webp") }
func (k Keys) join(orgID, jobID, name string) string {
	return strings.Trim(k.Prefix, "/") + "/" + orgID + "/" + jobID + "/" + name
}

// S3 is Blob on any S3-compatible store.
type S3 struct {
	client *minio.Client
	bucket string
}

// S3Config names the store. Credentials come from the environment the chart
// renders from the video store secret (never from the YAML).
type S3Config struct {
	// Endpoint is the store URL, e.g. https://storage.uk-south1.nebius.cloud.
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
}

func NewS3(c S3Config) (*S3, error) {
	if c.Endpoint == "" || c.Bucket == "" {
		return nil, fmt.Errorf("video store: endpoint and bucket are required")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return nil, fmt.Errorf("video store: credentials are required")
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("video store: endpoint %q must be an http(s) URL", c.Endpoint)
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: u.Scheme == "https",
		Region: c.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("video store: %w", err)
	}
	return &S3{client: client, bucket: c.Bucket}, nil
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: contentType})
	return err
}

// Get stats the object, then opens it. The size comes from a separate
// StatObject: calling Stat on the reader GetObject returns makes minio-go send
// every later read with If-Match on the stat's ETag, which S3-compatible stores
// that compute ETags differently refuse ("At least one of the pre-conditions
// you specified did not hold"; SeaweedFS on uk2, 2026-11-18). The first byte is
// read here, so a store that refuses the read fails before the API has written
// a 200.
func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, 0, ErrBlobNotFound
		}
		return nil, 0, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, err
	}
	first := make([]byte, 1)
	n, err := obj.Read(first)
	if err != nil && !(errors.Is(err, io.EOF) && info.Size == 0) {
		_ = obj.Close()
		return nil, 0, err
	}
	return readCloser{Reader: io.MultiReader(bytes.NewReader(first[:n]), obj), Closer: obj}, info.Size, nil
}

type readCloser struct {
	io.Reader
	io.Closer
}

func (s *S3) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil && minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil
	}
	return err
}
