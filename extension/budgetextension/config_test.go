package budgetextension

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/decision"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/ledger"
	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/metadata"
)

func loadConfig(t *testing.T, name string) (*Config, error) {
	t.Helper()
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	cfg := createDefaultConfig().(*Config)
	sub, err := cm.Sub(component.MustNewIDWithName(metadata.Type.String(), name).String())
	require.NoError(t, err)
	return cfg, sub.Unmarshal(cfg)
}

func TestLoadConfig(t *testing.T) {
	cfg, err := loadConfig(t, "")
	require.NoError(t, err)
	require.NoError(t, confmap.Validate(cfg))
	assert.Equal(t, []string{"tenant.id", "service.name"}, cfg.KeyAttributes)
	assert.True(t, cfg.Budgets[2].Match.Default)
	assert.Equal(t, map[string]string{"k8s.pod.label.team": "payments"}, cfg.Budgets[1].Match.Attribute)
	assert.Equal(t, "group", cfg.Budgets[1].Scope)
	assert.True(t, cfg.Prices.Cold.HasValue())
	assert.Equal(t, []string{"checkout", "group:payments-team"}, cfg.Exempt)
	assert.Equal(t, "gw-0", cfg.Sync.Storage.NodeID)
	assert.Equal(t, "redis_storage/budget", cfg.Sync.Storage.Extension.String())
	assert.Equal(t, 30*time.Second, cfg.Sync.Storage.ReadInterval, "defaults kept")
	tiers, err := cfg.tiers()
	require.NoError(t, err)
	assert.True(t, tiers[0].Actions.DropsMetric("go_gc"))
	assert.InDelta(t, 1.0, tiers[1].Actions.SampleRatio, 0, "unset sample ratio keeps all")
	hc, err := cfg.hardCap()
	require.NoError(t, err)
	assert.Equal(t, decision.HardCapDivert, hc)
	m := cfg.costModel()
	assert.Equal(t, int64(20_000), m.Prices[1].LogsGB)
	assert.Equal(t, int64(500_000), m.CompressionPPM[0])

	_, err = loadConfig(t, "badmatch")
	require.ErrorContains(t, err, "match must be a map")
}

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.Error(t, confmap.Validate(cfg), "budgets and tiers are required")
	cfg.Budgets = []BudgetRule{{Name: "default", Match: Match{Default: true}, Budget: 1}}
	cfg.Tiers = []TierConfig{{Threshold: 1}}
	require.NoError(t, confmap.Validate(cfg))
	p, err := cfg.period()
	require.NoError(t, err)
	assert.Equal(t, ledger.Period{Monthly: true}, p)
}

func validBase() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Budgets = []BudgetRule{
		{Name: "checkout", Match: Match{Key: "checkout*"}, Budget: 10},
		{Name: "default", Match: Match{Default: true}, Budget: 1},
	}
	cfg.Tiers = []TierConfig{{Threshold: 1}, {Threshold: 2, Actions: ActionsConfig{Divert: true}}}
	cfg.Prices.Cold = configoptional.Some(PriceList{LogsGB: 0.01})
	return cfg
}

func TestValidate(t *testing.T) {
	ext := component.MustNewIDWithName("redis_storage", "x")
	cases := []struct {
		name   string
		mutate func(*Config)
		errMsg string
	}{
		{"thresholds increasing", func(c *Config) { c.Tiers[1].Threshold = 1 }, "strictly increasing"},
		{"thresholds positive", func(c *Config) { c.Tiers[0].Threshold = 0 }, "threshold must be > 0"},
		{"divert only last", func(c *Config) {
			c.Tiers = []TierConfig{{Threshold: 1, Actions: ActionsConfig{Divert: true}}, {Threshold: 2}}
		}, "only the last tier may divert"},
		{"at most one divert", func(c *Config) {
			c.Tiers = []TierConfig{{Threshold: 1, Actions: ActionsConfig{Divert: true}}, {Threshold: 2, Actions: ActionsConfig{Divert: true}}}
		}, "at most one tier may divert"},
		{"hard cap divert needs divert tier", func(c *Config) { c.Tiers = c.Tiers[:1]; c.HardCap = "divert" }, "requires a tier with divert"},
		{"divert needs cold prices", func(c *Config) { c.Prices.Cold = configoptional.None[PriceList]() }, "prices.cold is required"},
		{"bad hard cap", func(c *Config) { c.HardCap = "sometimes" }, "hard_cap must be"},
		{"too many tiers", func(c *Config) {
			c.Tiers = []TierConfig{{Threshold: 1}, {Threshold: 2}, {Threshold: 3}, {Threshold: 4}, {Threshold: 5}, {Threshold: 6}}
		}, "1 to 5 entries"},
		{"no tiers", func(c *Config) { c.Tiers = nil }, "1 to 5 entries"},
		{"sample ratio range", func(c *Config) { c.Tiers[0].Actions.SampleRatio = 1.5 }, "sample_ratio"},
		{"bad severity", func(c *Config) { c.Tiers[0].Actions.DropBelowSeverity = "LOUD" }, "invalid severity"},
		{"bad metric glob", func(c *Config) { c.Tiers[0].Actions.DropMetrics = []string{"[a"} }, "invalid metric glob"},
		{"short < long", func(c *Config) { c.Windows.Short = 6 * time.Hour }, "windows must satisfy"},
		{"long <= period", func(c *Config) { c.Period = "1h" }, "windows must satisfy"},
		{"long <= 28 days for monthly", func(c *Config) { c.Windows.Long = 29 * 24 * time.Hour }, "windows must satisfy"},
		{"bad period", func(c *Config) { c.Period = "weekly" }, "period must be"},
		{"stale < fail open", func(c *Config) { c.StaleAfter = c.FailOpenAfter }, "stale_after"},
		{"hysteresis ratio", func(c *Config) { c.Hysteresis.Ratio = 1 }, "hysteresis.ratio"},
		{"hysteresis dwell", func(c *Config) { c.Hysteresis.MinDwell = -1 }, "min_dwell"},
		{"one default", func(c *Config) { c.Budgets[0].Match = Match{Default: true} }, "the default rule must be last"},
		{"default present", func(c *Config) { c.Budgets = c.Budgets[:1] }, "exactly one default rule"},
		{"default scope key", func(c *Config) { c.Budgets[1].Scope = "group" }, "must have scope key"},
		{"unique names", func(c *Config) { c.Budgets[0].Name = "default" }, "duplicate rule name"},
		{"name required", func(c *Config) { c.Budgets[0].Name = "" }, "name is required"},
		{"budget positive", func(c *Config) { c.Budgets[0].Budget = 0 }, "budget must be > 0"},
		{"empty match", func(c *Config) { c.Budgets[0].Match = Match{} }, "match needs"},
		{"bad scope", func(c *Config) { c.Budgets[0].Scope = "team" }, "scope must be"},
		{"bad key glob", func(c *Config) { c.Budgets[0].Match.Key = "[a" }, "invalid key glob"},
		{"no budgets", func(c *Config) { c.Budgets = nil }, "budgets must not be empty"},
		{"storage needs extension", func(c *Config) { c.Sync.Mode = "storage" }, "requires sync.storage.extension"},
		{"storage needs node names in auto mode", func(c *Config) {
			c.Sync.Mode, c.Sync.Storage.Extension = "storage", &ext
		}, "requires sync.storage.node_id and sync.storage.node_prefix"},
		{"storage needs node prefix in increment mode", func(c *Config) {
			c.Sync.Mode, c.Sync.Storage.Extension, c.Sync.Storage.Mode, c.Sync.Storage.NodeID = "storage", &ext, "increment", "gw-0"
		}, "sync.storage.node_prefix"},
		{"bad storage mode", func(c *Config) { c.Sync.Mode, c.Sync.Storage.Extension, c.Sync.Storage.Mode = "storage", &ext, "x" }, "sync.storage.mode"},
		{"storage intervals", func(c *Config) {
			c.Sync.Mode, c.Sync.Storage.Extension, c.Sync.Storage.FlushInterval = "storage", &ext, 0
		}, "must be positive"},
		{"share needs replicas", func(c *Config) { c.Sync.Mode = "share" }, "requires sync.share.replicas"},
		{"bad sync mode", func(c *Config) { c.Sync.Mode = "gossip" }, "sync.mode must be"},
		{"empty exempt", func(c *Config) { c.Exempt = []string{""} }, "exempt[0] must not be empty"},
		{"exempt unknown group", func(c *Config) { c.Exempt = []string{"group:nope"} }, "does not name a group rule"},
		{"exempt key rule is not a group", func(c *Config) { c.Exempt = []string{"group:checkout"} }, "does not name a group rule"},
		{"key attributes", func(c *Config) { c.KeyAttributes = nil }, "key_attributes"},
		{"max keys", func(c *Config) { c.MaxKeys = 0 }, "max_keys"},
		{"negative price", func(c *Config) { c.Prices.Hot.LogsGB = -1 }, "prices.hot.logs_gb"},
		{"negative compression", func(c *Config) { c.CompressionRatio.Logs = -1 }, "compression_ratio"},
		{"decision interval", func(c *Config) { c.DecisionInterval = 0 }, "decision_interval"},
		{"warmup", func(c *Config) { c.WarmupCoverage = 2 }, "warmup_coverage"},
		{"always pass", func(c *Config) { c.AlwaysPass.LogSeverity = "x" }, "always_pass.log_severity"},
		{"telemetry keys", func(c *Config) { c.Telemetry.MaxKeys = 0 }, "telemetry.max_keys"},
	}
	require.NoError(t, validBase().Validate())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validBase()
			tc.mutate(cfg)
			require.ErrorContains(t, cfg.Validate(), tc.errMsg)
		})
	}
}

func TestShareReplicas(t *testing.T) {
	cfg := validBase()
	cfg.Sync.Share.Replicas = 3
	n, err := cfg.shareReplicas()
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	cfg.Sync.Share = ShareSyncConfig{ReplicasEnv: "BUDGET_TEST_REPLICAS"}
	t.Setenv("BUDGET_TEST_REPLICAS", " 4 ")
	n, err = cfg.shareReplicas()
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)
	t.Setenv("BUDGET_TEST_REPLICAS", "zero")
	_, err = cfg.shareReplicas()
	require.Error(t, err)
	cfg.Sync.Share = ShareSyncConfig{}
	_, err = cfg.shareReplicas()
	require.Error(t, err)
}
