package iolog

// OpenSearch is the DESIGNED-FOR-BUT-NOT-BUILT alternative store for I/O logs
// (per DESIGN.md §8). It is intentionally NOT implemented in M5.
//
// # Why Postgres-first, OpenSearch later
//
// M5 logging is SAMPLED and short-retention, so the body corpus stays small —
// Postgres' tsvector + GIN full-text index ("grep inside bodies") is more than
// enough and avoids standing up and operating a second datastore. OpenSearch
// earns its keep only at a much larger corpus / richer-query regime.
//
// # The tripwire — when to actually build this
//
// Build the OpenSearch sink when I/O logging changes from SAMPLED to
// LOG-EVERYTHING (always-on full capture). At that point the corpus outgrows
// what Postgres FTS serves comfortably — index bloat, GIN write amplification,
// and ranked/aggregated body queries push past Postgres' sweet spot — and a
// purpose-built search store wins. Until that tripwire trips, this stays a
// typed hole.
//
// The Sink interface IS the seam: an OpenSearchSink would implement Log/Close
// exactly like PostgresSink, and wiring it is a one-line change at the
// construction site (see cmd/interceptor/main.go). Do NOT implement it
// speculatively.
//
// Intentionally left as documentation only — no type, no constructor, no
// "not implemented" stub to maintain. The Sink interface already defines the
// contract a future OpenSearchSink must satisfy.
