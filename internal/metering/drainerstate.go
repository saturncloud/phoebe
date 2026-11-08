package metering

// The drainer publishes its state to a Valkey hash next to the metering stream,
// and the interceptor reads it for its metrics. The drainer has no metrics port
// of its own, so this hash is how its state reaches Prometheus. The names below
// are shared by both sides so they cannot drift apart.
//
// Fields:
//
//	<consumer>.seen          unix seconds of that consumer's last publish
//	<consumer>.paused_since  unix seconds its current pause began; 0 when not paused
//	dropped_poison           rows dropped as poison, all consumers, since the key was created
//	dropped_undecodable      entries dropped as undecodable, all consumers, since the key was created
const (
	// DrainerStateSuffix is appended to the stream name to form the hash key.
	DrainerStateSuffix = ":drainer"

	DrainerFieldSeen        = ".seen"
	DrainerFieldPausedSince = ".paused_since"

	DrainerFieldDroppedPoison      = "dropped_poison"
	DrainerFieldDroppedUndecodable = "dropped_undecodable"
)

// DrainerStateKey returns the hash key the drainer publishes its state under
// for the given metering stream, e.g. "phoebe:metering:drainer".
func DrainerStateKey(stream string) string { return stream + DrainerStateSuffix }
