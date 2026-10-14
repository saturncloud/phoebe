package gateway

import (
	"os"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

type rbacRule struct {
	APIGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
	Verbs     []string `yaml:"verbs"`
}

type rbacDoc struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Rules []rbacRule `yaml:"rules"`
}

// gatewayRole parses the phoebe-gateway-registry Role out of the RBAC
// deliverable deploy/rbac-gateway.yaml.
func gatewayRole(t *testing.T) rbacDoc {
	t.Helper()
	raw, err := os.ReadFile("../../deploy/rbac-gateway.yaml")
	if err != nil {
		t.Fatalf("read RBAC deliverable: %v", err)
	}
	for _, part := range strings.Split(string(raw), "\n---\n") {
		var d rbacDoc
		if err := yaml.Unmarshal([]byte(part), &d); err != nil {
			t.Fatalf("parse RBAC deliverable: %v", err)
		}
		if d.Kind == "Role" && d.Metadata.Name == "phoebe-gateway-registry" {
			return d
		}
		if d.Kind == "ClusterRole" {
			t.Fatalf("RBAC deliverable must not contain a ClusterRole (%s)", d.Metadata.Name)
		}
	}
	t.Fatal("RBAC deliverable has no Role phoebe-gateway-registry")
	return rbacDoc{}
}

// The Role must grant exactly get/list/watch on every resource the gateway's
// informers watch: the registry ConfigMaps (registry.go) and the
// DynamoGraphDeployments read for ready workers (workers.go, D54). Without
// the DGD grant the informer never syncs and every graph is held to one
// worker's worth of admission, with no startup failure.
func TestGatewayRBACGrantsReadOnlyAccessToEveryResourceTheGatewayInformersWatch(t *testing.T) {
	role := gatewayRole(t)
	want := map[string]bool{
		"/configmaps": false,
		dynamoGraphResource.Group + "/" + dynamoGraphResource.Resource: false,
	}
	for _, r := range role.Rules {
		verbs := append([]string(nil), r.Verbs...)
		sort.Strings(verbs)
		if strings.Join(verbs, ",") != "get,list,watch" {
			t.Errorf("rule %+v: verbs must be exactly get/list/watch (read-only), got %v", r, r.Verbs)
		}
		for _, g := range r.APIGroups {
			for _, res := range r.Resources {
				key := g + "/" + res
				if _, ok := want[key]; !ok {
					t.Errorf("rule grants %q, which no gateway informer watches", key)
					continue
				}
				want[key] = true
			}
		}
	}
	for key, granted := range want {
		if !granted {
			t.Errorf("Role phoebe-gateway-registry does not grant get/list/watch on %q", key)
		}
	}
}
