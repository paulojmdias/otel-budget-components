package examples

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
	"gopkg.in/yaml.v3"
)

// Every YAML snippet in the READMEs must be a valid configuration once the
// parts it leaves out on purpose are filled in: undefined receivers and
// exporters get defaults, and a budget extension missing its required fields
// gets a minimal valid base. Keys the snippet sets always win, so an invalid
// or removed option in the docs fails this test.
var readmes = []string{
	"../README.md",
	"../extension/budgetextension/README.md",
	"../processor/budgetprocessor/README.md",
}

type snippet struct {
	file string
	line int
	body string
}

func snippets(t *testing.T) []snippet {
	re := regexp.MustCompile("(?s)```yaml\n(.*?)```")
	var out []snippet
	for _, f := range readmes {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		s := string(b)
		for _, m := range re.FindAllStringSubmatchIndex(s, -1) {
			out = append(out, snippet{file: f, line: strings.Count(s[:m[0]], "\n") + 1, body: s[m[2]:m[3]]})
		}
	}
	require.NotEmpty(t, out)
	return out
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// child returns m[k] as a map, creating it.
func child(m map[string]any, k string) map[string]any {
	c := asMap(m[k])
	if c == nil {
		c = map[string]any{}
		m[k] = c
	}
	return c
}

// defaultComponent returns a minimal config for a component ID by type.
func defaultComponent(kind, id string) any {
	typ, _, _ := strings.Cut(id, "/")
	switch {
	case kind == "receivers" && typ == "otlp":
		return map[string]any{"protocols": map[string]any{"grpc": map[string]any{"endpoint": "127.0.0.1:4317"}}}
	case kind == "exporters" && typ == "otlp_http":
		return map[string]any{"endpoint": "http://127.0.0.1:4318"}
	case kind == "exporters" && typ == "file":
		return map[string]any{"path": "/tmp/otelcol-budget-docs-test.jsonl"}
	case kind == "extensions" && typ == "redis_storage":
		return map[string]any{"endpoint": "127.0.0.1:6379", "tls": map[string]any{"insecure": true}}
	}
	return map[string]any{}
}

// complete fills in what a snippet leaves out on purpose.
func complete(doc map[string]any) map[string]any {
	// fragments of the budget extension config
	if _, ok := doc["budgets"]; ok {
		doc = map[string]any{"extensions": map[string]any{"budget": doc}}
	} else if _, ok := doc["tiers"]; ok {
		doc = map[string]any{"extensions": map[string]any{"budget": doc}}
	}
	exts := child(doc, "extensions")
	procs := child(doc, "processors")
	recvs := child(doc, "receivers")
	expts := child(doc, "exporters")
	service := child(doc, "service")

	// a budget extension gets the required fields it does not set
	if budget, ok := exts["budget"]; ok || len(procs) > 0 {
		b := asMap(budget)
		if b == nil {
			b = map[string]any{}
			exts["budget"] = b
		}
		if _, ok := b["budgets"]; !ok {
			b["budgets"] = []any{map[string]any{"name": "default", "match": "default", "budget": 100}}
		}
		if _, ok := b["tiers"]; !ok {
			b["tiers"] = []any{map[string]any{"threshold": 1.0}}
		}
		if _, ok := b["prices"]; !ok {
			b["prices"] = map[string]any{"hot": map[string]any{"logs_gb": 0.4}}
		}
	}

	pipelines := child(service, "pipelines")
	if len(pipelines) == 0 {
		p := []any{}
		for id := range procs {
			p = append(p, id)
		}
		pipelines["logs"] = map[string]any{"receivers": []any{"otlp"}, "processors": p, "exporters": []any{"debug"}}
	}
	// define every component a pipeline references
	for _, pl := range pipelines {
		pm := asMap(pl)
		for kind, set := range map[string]map[string]any{"receivers": recvs, "processors": procs, "exporters": expts} {
			list, _ := pm[kind].([]any)
			for _, id := range list {
				s := fmt.Sprint(id)
				if _, connector := asMap(doc["connectors"])[s]; connector {
					continue
				}
				if _, ok := set[s]; !ok {
					set[s] = defaultComponent(kind, s)
				}
			}
		}
	}
	// every defined extension is enabled
	if _, ok := service["extensions"]; !ok {
		list := []any{}
		for id := range exts {
			list = append(list, id)
		}
		service["extensions"] = list
	}
	for _, id := range asList(service["extensions"]) {
		if _, ok := exts[id]; !ok {
			exts[id] = defaultComponent("extensions", id)
		}
	}
	if len(procs) == 0 {
		delete(doc, "processors")
	}
	if len(recvs) == 0 {
		delete(doc, "receivers")
	}
	return doc
}

func asList(v any) []string {
	l, _ := v.([]any)
	out := make([]string, 0, len(l))
	for _, x := range l {
		out = append(out, fmt.Sprint(x))
	}
	return out
}

func TestDocSnippets(t *testing.T) {
	t.Setenv("POD_NAME", "otelcol-gw-0")
	t.Setenv("GATEWAY_REPLICAS", "3")
	for _, sn := range snippets(t) {
		name := fmt.Sprintf("%s:%d", filepath.Base(filepath.Dir(sn.file))+"/"+filepath.Base(sn.file), sn.line)
		t.Run(name, func(t *testing.T) {
			var doc map[string]any
			require.NoError(t, yaml.Unmarshal([]byte(sn.body), &doc), "not valid YAML")

			switch {
			case strings.Contains(sn.body, "gomod:"):
				// an OCB manifest fragment: the referenced local modules must exist
				for _, l := range asMapList(doc) {
					if p, ok := l["gomod"].(string); ok && strings.HasPrefix(p, "github.com/paulojmdias/otel-budget-components/") {
						dir := strings.Fields(strings.TrimPrefix(p, "github.com/paulojmdias/otel-budget-components/"))[0]
						_, err := os.Stat(filepath.Join("..", dir, "go.mod"))
						require.NoError(t, err, "module %s does not exist", dir)
					}
				}
				return
			case doc["key_attributes"] != nil && doc["budgets"] == nil:
				return // the defaults block, compared with createDefaultConfig in the extension's tests
			}

			out, err := yaml.Marshal(complete(doc))
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, out, 0o600))
			_, err = otelcoltest.LoadConfigAndValidate(path, factories(t))
			require.NoError(t, err, "snippet:\n%s\ncompleted:\n%s", sn.body, out)
		})
	}
}

// asMapList flattens a manifest fragment into its component entries.
func asMapList(doc map[string]any) []map[string]any {
	var out []map[string]any
	for _, v := range doc {
		l, _ := v.([]any)
		for _, x := range l {
			if m := asMap(x); m != nil {
				out = append(out, m)
			}
		}
	}
	return out
}
