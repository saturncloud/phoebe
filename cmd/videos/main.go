// Command videos is phoebe-videos: Token Factory's video generation job
// service. It serves /v1/videos for requests the phoebe interceptor hands it
// (with the trusted identity of the request), runs the dispatcher that calls
// each video endpoint's engine, and the reaper that enforces timeouts and
// retention. One Deployment; several replicas may run (claims are decided in
// Postgres).
//
// Config: a YAML file (-f) for settings, the environment for secrets:
//
//	DATABASE_URL                   phoebe's own Postgres (migration 0016)
//	PHOEBE_VIDEOS_TOKEN            shared with the interceptor; required
//	PHOEBE_ATLAS_TOKEN_FILE        the mounted file that holds phoebe-videos'
//	                               Atlas service token (tf-internal:video-store),
//	                               which Atlas renews; required
//	POD_NAME                       the queue consumer name (defaults to the host name)
//
// phoebe-videos holds no object-store credential. Atlas (atlasURL) signs a
// presigned URL for each object of a registered job (design 6.1).
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v2"

	"github.com/saturncloud/phoebe/internal/atlastoken"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/videos"
)

// Limits Atlas enforces when it signs an upload (saturn pdc/settings.py
// TF_VIDEO_UPLOAD_WINDOW_MINUTES and pdc/managers/tf_video_store.py
// _MAX_BYTES). The chart checks the same values.
const (
	// maxUploadWindow is the largest upload window Atlas allows.
	maxUploadWindow = 1440 * time.Minute
	// uploadMargin is the time load() keeps for storing the video after the
	// last run ends.
	uploadMargin = 30 * time.Minute
	// maxStoreObjectBytes is the largest video.mp4 Atlas signs (5 GiB).
	maxStoreObjectBytes = 5 << 30
)

// Settings is the YAML file.
type Settings struct {
	Debug      bool   `yaml:"debug"`
	ListenPort int    `yaml:"listenPort"`
	ValkeyAddr string `yaml:"valkeyAddr"`
	// AtlasURL is the in-cluster Atlas that signs the object store URLs.
	AtlasURL string `yaml:"atlasURL"`
	Store    struct {
		// Prefix is TF_VIDEO_STORE_PREFIX (default tf-videos). Atlas signs
		// only keys under its own TF_VIDEO_STORE_PREFIX, so the two must match.
		Prefix string `yaml:"prefix"`
	} `yaml:"store"`
	// RetentionHours is TF_VIDEO_RETENTION_HOURS: 1 to 168, default 24.
	RetentionHours int    `yaml:"retentionHours"`
	QueueTimeout   string `yaml:"queueTimeout"`
	RunTimeout     string `yaml:"runTimeout"`
	MaxOutputBytes int64  `yaml:"maxOutputBytes"`
	QueueCap       int    `yaml:"queueCap"`
	WorkDir        string `yaml:"workDir"`
	FFprobe        string `yaml:"ffprobe"`
	FFmpeg         string `yaml:"ffmpeg"`
}

type config struct {
	s            Settings
	retention    time.Duration
	queueTimeout time.Duration
	runTimeout   time.Duration
}

func load(path string, getenv func(string) string) (config, error) {
	var c config
	raw, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := yaml.UnmarshalStrict(raw, &c.s); err != nil {
		return c, fmt.Errorf("%s: %w", path, err)
	}
	s := &c.s
	if s.ListenPort == 0 {
		s.ListenPort = 8090
	}
	if s.Store.Prefix == "" {
		s.Store.Prefix = "tf-videos"
	}
	if s.RetentionHours == 0 {
		s.RetentionHours = 24
	}
	if s.RetentionHours < 1 || s.RetentionHours > 168 {
		return c, fmt.Errorf("retentionHours must be 1 to 168 (design 6.2), got %d", s.RetentionHours)
	}
	c.retention = time.Duration(s.RetentionHours) * time.Hour
	for _, d := range []struct {
		raw  string
		def  time.Duration
		dst  *time.Duration
		name string
	}{
		{s.QueueTimeout, 60 * time.Minute, &c.queueTimeout, "queueTimeout"},
		{s.RunTimeout, 30 * time.Minute, &c.runTimeout, "runTimeout"},
	} {
		*d.dst = d.def
		if d.raw != "" {
			v, err := time.ParseDuration(d.raw)
			if err != nil || v <= 0 {
				return c, fmt.Errorf("%s: %q is not a positive duration", d.name, d.raw)
			}
			*d.dst = v
		}
	}
	// Atlas lets an upload be signed for a job until at most 1440 minutes
	// after it registered (TF_VIDEO_UPLOAD_WINDOW_MINUTES' ceiling). A job can
	// wait queueTimeout, run twice for runTimeout, and needs time to store its
	// video, so timeouts longer than that would let a job finish generating
	// after its upload can no longer be signed.
	if need := c.queueTimeout + 2*c.runTimeout + uploadMargin; need > maxUploadWindow {
		return c, fmt.Errorf("queueTimeout + 2*runTimeout + %s is %s, more than the %s upload window Atlas allows at most",
			uploadMargin, need, maxUploadWindow)
	}
	if s.MaxOutputBytes > maxStoreObjectBytes {
		return c, fmt.Errorf("maxOutputBytes must be at most %d (the largest video Atlas signs an upload for), got %d",
			maxStoreObjectBytes, s.MaxOutputBytes)
	}
	if s.ValkeyAddr == "" {
		return c, errors.New("valkeyAddr is required")
	}
	if getenv("DATABASE_URL") == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if getenv("PHOEBE_VIDEOS_TOKEN") == "" {
		return c, errors.New("PHOEBE_VIDEOS_TOKEN is required")
	}
	if s.AtlasURL == "" {
		return c, errors.New("atlasURL is required: Atlas signs every object store request")
	}
	// Only the path is checked here: on a new install the pod may start
	// before Atlas has written the token, which is read before every call.
	if getenv(atlastoken.EnvFile) == "" {
		return c, errors.New(atlastoken.EnvFile + " is required")
	}
	return c, nil
}

func main() {
	path := flag.String("f", "/etc/saturn/config/videos.yaml", "settings YAML file")
	flag.Parse()
	log := logging.New(logging.INFO)
	c, err := load(*path, os.Getenv)
	if err != nil {
		log.Error.Fatalf("videos: config: %v", err)
	}
	if c.s.Debug {
		log.SetLevel(logging.DEBUG)
	}

	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Error.Fatalf("videos: database: %v", err)
	}
	db.SetMaxOpenConns(20)
	db.SetConnMaxLifetime(30 * time.Minute)
	defer db.Close()

	tokens, err := atlastoken.NewFile(os.Getenv(atlastoken.EnvFile), log)
	if err != nil {
		log.Error.Fatalf("videos: %v", err)
	}
	blob, err := videos.NewAtlasBlob(c.s.AtlasURL, tokens, c.s.Store.Prefix)
	if err != nil {
		log.Error.Fatalf("videos: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: c.s.ValkeyAddr})
	defer rdb.Close()
	consumer := os.Getenv("POD_NAME")
	if consumer == "" {
		consumer, _ = os.Hostname()
	}

	store := videos.NewStore(db)
	queue := videos.NewQueue(rdb, "", consumer)
	media := videos.Media{FFprobe: c.s.FFprobe, FFmpeg: c.s.FFmpeg}
	api := videos.NewAPI(videos.APIConfig{
		Token: os.Getenv("PHOEBE_VIDEOS_TOKEN"), QueueCap: c.s.QueueCap, Prefix: c.s.Store.Prefix,
	}, store, queue, blob, log)
	dispatcher := videos.NewDispatcher(videos.DispatchConfig{
		RunTimeout: c.runTimeout, Retention: c.retention, MaxOutputBytes: c.s.MaxOutputBytes,
		WorkDir: c.s.WorkDir, Prefix: c.s.Store.Prefix,
	}, store, queue, blob, videos.NewOmniEngine(), media, log)
	reaper := videos.NewReaper(videos.ReaperConfig{QueueTimeout: c.queueTimeout, Prefix: c.s.Store.Prefix}, store, queue, blob, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	done := make(chan struct{}, 2)
	go func() { dispatcher.Run(ctx); done <- struct{}{} }()
	go func() { reaper.Run(ctx); done <- struct{}{} }()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		hctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(hctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/v1/videos", api)
	mux.Handle("/v1/videos/", api)
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", c.s.ListenPort),
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info.Printf("videos: listening on %s (store prefix %s signed by %s, retention %s, queue timeout %s, run timeout %s)",
		srv.Addr, c.s.Store.Prefix, c.s.AtlasURL, c.retention, c.queueTimeout, c.runTimeout)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error.Printf("videos: server: %v", err)
	}
	stop()
	<-done
	<-done
}
