package atlastoken

import "testing"

// TestAtlasURLMustBeHTTPSUnlessClusterLocal: the Atlas service token goes in
// a header on every call, so http is allowed only to a cluster-local host.
// The suffix match is dot-anchored: "evilsvc.com" and "a.svc.evil.com" are
// external hosts. A trailing root dot skips the pod's DNS search path, so
// http to any host ending in "." is refused; https to one is accepted.
func TestAtlasURLMustBeHTTPSUnlessClusterLocal(t *testing.T) {
	for _, tc := range []struct {
		url string
		ok  bool
	}{
		{"https://atlas.example.com", true},
		{"https://atlas.example.com:8443/prefix", true},
		{"http://atlas.example.com", false},
		{"http://atlas.example.com:8000", false},
		{"http://10.0.0.1:8000", false},
		{"http://atlas", false},
		{"http://atlas.saturn.svc", true},
		{"http://atlas.saturn.svc:8000", true},
		{"http://atlas.saturn.svc.cluster.local:8000", true},
		{"http://ATLAS.SATURN.SVC.CLUSTER.LOCAL", true},
		{"http://atlas.saturn.svc.cluster.local.:8000", false},
		{"http://atlas.saturn.svc.", false},
		{"http://atlas.saturn.svc.:8888", false},
		{"http://localhost.:8000", false},
		{"http://127.0.0.1.:8000", false},
		{"https://atlas.example.com.", true},
		{"https://atlas.saturn.svc.:8443", true},
		{"http://localhost:8000", true},
		{"http://127.0.0.1:41234", true},
		{"http://[::1]:9000", true},
		{"http://127.0.0.2", false},
		{"http://localhost.example.com", false},
		{"http://evilsvc.com", false},
		{"http://a.svc.evil.com", false},
		{"http://a.svc.cluster.local.evil.com", false},
		{"http://svc", false},
		{"ftp://atlas.saturn.svc", false},
		{"ftp://atlas.example.com", false},
		{"atlas.saturn.svc", false},
		{"https://", false},
	} {
		err := CheckAtlasURL(tc.url)
		if tc.ok && err != nil {
			t.Errorf("CheckAtlasURL(%q) = %v, want accepted", tc.url, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("CheckAtlasURL(%q) accepted, want refused", tc.url)
		}
	}
}
