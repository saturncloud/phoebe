// Command videos is phoebe-videos: Token Factory's video generation job
// service. It serves /v1/videos for requests the phoebe interceptor hands it
// (with the trusted identity of the request), runs the dispatcher that calls
// each video endpoint's engine, and the reaper that enforces timeouts and
// retention. One Deployment; several replicas may run (claims are decided in
// Postgres).
//
// Config: a YAML file (-f) for settings, the environment for secrets:
//
//	DATABASE_URL            phoebe's own Postgres (migration 0013)
//	PHOEBE_VIDEOS_TOKEN     shared with the interceptor; required
//	VIDEO_STORE_ACCESS_KEY  object store credentials for the video prefix
//	VIDEO_STORE_SECRET_KEY
//	POD_NAME                the queue consumer name (defaults to the host name)
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

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/videos"
)

// Settings is the YAML file.
type Settings struct {
	Debug      bool   `yaml:"debug"`
	ListenPort int    `yaml:"listenPort"`
	ValkeyAddr string `yaml:"valkeyAddr"`
	Store      struct {
		Endpoint string `yaml:"endpoint"`
		Bucket   string `yaml:"bucket"`
		Region   string `yaml:"region"`
		// Prefix is TF_VIDEO_STORE_PREFIX (default tf-videos).
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
	if s.ValkeyAddr == "" {
		return c, errors.New("valkeyAddr is required")
	}
	if getenv("DATABASE_URL") == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if getenv("PHOEBE_VIDEOS_TOKEN") == "" {
		return c, errors.New("PHOEBE_VIDEOS_TOKEN is required")
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

	blob, err := videos.NewS3(videos.S3Config{
		Endpoint: c.s.Store.Endpoint, Bucket: c.s.Store.Bucket, Region: c.s.Store.Region,
		AccessKey: os.Getenv("VIDEO_STORE_ACCESS_KEY"), SecretKey: os.Getenv("VIDEO_STORE_SECRET_KEY"),
	})
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
	reaper := videos.NewReaper(videos.ReaperConfig{QueueTimeout: c.queueTimeout}, store, queue, blob, log)

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
	log.Info.Printf("videos: listening on %s (store %s/%s, retention %s, queue timeout %s, run timeout %s)",
		srv.Addr, c.s.Store.Bucket, c.s.Store.Prefix, c.retention, c.queueTimeout, c.runTimeout)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error.Printf("videos: server: %v", err)
	}
	stop()
	<-done
	<-done
}
