package proxy

import (
	"encoding/json"
	"testing"
)

func TestRewriteIncludeUsage(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantChanged bool
		wantInclude bool // expected stream_options.include_usage after rewrite
	}{
		{
			name:        "streaming, no stream_options -> injected",
			body:        `{"model":"m","stream":true,"messages":[]}`,
			wantChanged: true,
			wantInclude: true,
		},
		{
			name:        "streaming, include_usage already true -> unchanged",
			body:        `{"stream":true,"stream_options":{"include_usage":true}}`,
			wantChanged: false,
			wantInclude: true,
		},
		{
			name:        "streaming, client set include_usage false -> overwritten to true",
			body:        `{"stream":true,"stream_options":{"include_usage":false}}`,
			wantChanged: true,
			wantInclude: true,
		},
		{
			name:        "non-streaming -> untouched",
			body:        `{"stream":false,"messages":[]}`,
			wantChanged: false,
		},
		{
			name:        "no stream field -> untouched",
			body:        `{"messages":[]}`,
			wantChanged: false,
		},
		{
			name:        "not JSON -> untouched",
			body:        `not json at all`,
			wantChanged: false,
		},
		{
			name:        "streaming with other stream_options preserved",
			body:        `{"stream":true,"stream_options":{"continuous_usage_stats":true}}`,
			wantChanged: true,
			wantInclude: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, changed := rewriteIncludeUsage([]byte(tt.body))
			if changed != tt.wantChanged {
				t.Fatalf("changed = %v, want %v (out=%s)", changed, tt.wantChanged, out)
			}
			if !tt.wantChanged {
				return
			}
			var m map[string]json.RawMessage
			if err := json.Unmarshal(out, &m); err != nil {
				t.Fatalf("rewritten body not valid JSON: %v", err)
			}
			var opts struct {
				IncludeUsage bool `json:"include_usage"`
			}
			if err := json.Unmarshal(m["stream_options"], &opts); err != nil {
				t.Fatalf("stream_options not an object: %v", err)
			}
			if opts.IncludeUsage != tt.wantInclude {
				t.Fatalf("include_usage = %v, want %v", opts.IncludeUsage, tt.wantInclude)
			}
		})
	}
}

func TestRewritePreservesOtherStreamOptions(t *testing.T) {
	body := `{"stream":true,"stream_options":{"continuous_usage_stats":true}}`
	out, _ := rewriteIncludeUsage([]byte(body))
	var m struct {
		StreamOptions struct {
			ContinuousUsageStats bool `json:"continuous_usage_stats"`
			IncludeUsage         bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if !m.StreamOptions.ContinuousUsageStats {
		t.Fatal("continuous_usage_stats was dropped during rewrite")
	}
	if !m.StreamOptions.IncludeUsage {
		t.Fatal("include_usage was not set")
	}
}
