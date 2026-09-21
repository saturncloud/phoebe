// Command recover validates and replays Phoebe metering recovery artifacts.
// It is dry-run-only unless -apply with an exact -expected-count and
// -expected-digest binding the complete reviewed event set are supplied.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/recovery"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "phoebe-recover: %v\n", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("phoebe-recover", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "JSONL/log file or tidwall WAL directory to recover")
	valkeyAddr := flags.String("valkey-addr", "localhost:6379", "Valkey address")
	stream := flags.String("stream", "phoebe:metering", "target Valkey stream")
	timeout := flags.Duration("timeout", 2*time.Second, "timeout for each XADD")
	apply := flags.Bool("apply", false, "write validated events (otherwise dry-run only)")
	expected := flags.Int("expected-count", -1, "required with -apply; must equal validated unique count")
	expectedDigest := flags.String("expected-digest", "", "required with -apply; must equal the dry-run event_set_sha256")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *input == "" {
		return fmt.Errorf("-input is required")
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}

	evidence, err := recovery.Load(*input)
	if err != nil {
		return err
	}
	digest := evidence.Digest()
	if digest == "" {
		return fmt.Errorf("unable to compute the recovery event digest")
	}
	fmt.Fprintf(stdout, "validated records=%d unique=%d duplicates=%d event_set_sha256=%s\n",
		evidence.Records, len(evidence.Events), evidence.Duplicates, digest)
	if !*apply {
		fmt.Fprintf(stdout, "dry-run only; replay with -apply -expected-count %d -expected-digest %s after reviewing this result\n",
			len(evidence.Events), digest)
		return nil
	}
	// Bind the replay to the exact artifact reviewed during dry-run. Both
	// checks run before a Valkey client is constructed so a mismatched or
	// tampered set never reaches the network.
	if *expected < 0 {
		return fmt.Errorf("-expected-count is required with -apply")
	}
	if *expectedDigest == "" {
		return fmt.Errorf("-expected-digest is required with -apply")
	}
	if *expected != len(evidence.Events) {
		return fmt.Errorf("expected count %d does not match validated unique count %d", *expected, len(evidence.Events))
	}
	if !strings.EqualFold(*expectedDigest, digest) {
		return fmt.Errorf("expected digest %s does not match validated event digest %s", *expectedDigest, digest)
	}
	if len(evidence.Events) == 0 {
		return fmt.Errorf("refuse to apply an empty recovery set")
	}
	if *valkeyAddr == "" || *stream == "" {
		return fmt.Errorf("-valkey-addr and -stream must be non-empty")
	}

	ctx, stop := signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	rdb := redis.NewClient(&redis.Options{Addr: *valkeyAddr})
	defer func() { _ = rdb.Close() }()
	written, err := recovery.Replay(ctx, rdb, *stream, *timeout, evidence.Events)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "applied events=%d stream=%s\n", written, *stream)
	return nil
}
