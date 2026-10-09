package budgetextension // import "github.com/paulojmdias/otel-budget-components/extension/budgetextension"

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/budgetapi"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/costmodel"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/decision"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/keying"
	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/ledger"
	spendsync "github.com/paulojmdias/otel-budget-components/extension/budgetextension/internal/sync"
)

// Config is the budget extension configuration.
type Config struct {
	// KeyAttributes are the resource attributes joined with "|" to form the
	// budget key. A missing attribute becomes "unknown"; when all are missing
	// the key is "default". Default: [service.name].
	KeyAttributes []string `mapstructure:"key_attributes"`
	// MaxKeys is the number of distinct keys per period. New keys beyond it
	// fold into "__other__" for the rest of the period. Default: 10000.
	MaxKeys int `mapstructure:"max_keys"`
	// Period is the budget window: "monthly" (calendar month, UTC) or a Go
	// duration such as "1h". Default: monthly.
	Period string `mapstructure:"period"`
	// Prices per signal, for the hot and the cold price class.
	Prices PricesConfig `mapstructure:"prices"`
	// CompressionRatio multiplies bytes before pricing, per signal.
	// Default: 1.0 for every signal.
	CompressionRatio CompressionConfig `mapstructure:"compression_ratio"`
	// Budgets are the ordered budget rules. Required; the last one must be
	// the default rule.
	Budgets []BudgetRule `mapstructure:"budgets"`
	// Windows are the short and long burn rate windows.
	// Default: short 1h, long 6h.
	Windows WindowsConfig `mapstructure:"windows"`
	// DecisionInterval is how often decisions are recomputed and published.
	// Default: 10s.
	DecisionInterval time.Duration `mapstructure:"decision_interval"`
	// WarmupCoverage is the fraction of the short window that must be covered
	// by samples before a key can escalate. Default: 0.8.
	WarmupCoverage float64 `mapstructure:"warmup_coverage"`
	// Tiers are the enforcement levels, 1 to 5 entries with strictly
	// increasing thresholds. Required.
	Tiers []TierConfig `mapstructure:"tiers"`
	// HardCap selects what happens when period to date spend reaches the
	// budget: "highest" (at least the highest tier without divert), "divert"
	// (at least the divert tier), or "off". Default: highest.
	HardCap string `mapstructure:"hard_cap"`
	// Hysteresis controls de-escalation.
	Hysteresis HysteresisConfig `mapstructure:"hysteresis"`
	// StaleAfter is the sync data age after which escalation is frozen.
	// Default: 5m.
	StaleAfter time.Duration `mapstructure:"stale_after"`
	// FailOpenAfter is the sync data age after which every key is at tier 0
	// and the extension reports unhealthy. Must be greater than StaleAfter.
	// Default: 30m.
	FailOpenAfter time.Duration `mapstructure:"fail_open_after"`
	// AlwaysPass is the never drop policy applied by the processor.
	AlwaysPass AlwaysPassConfig `mapstructure:"always_pass"`
	// Exempt lists budget keys, or group ledger IDs as "group:<rule name>",
	// that are accounted but never enforced.
	Exempt []string `mapstructure:"exempt"`
	// Sync selects how spend is shared across Collector replicas.
	Sync SyncConfig `mapstructure:"sync"`
	// Telemetry bounds the cardinality of self telemetry.
	Telemetry TelemetryConfig `mapstructure:"telemetry"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// PricesConfig holds hot and optional cold prices.
type PricesConfig struct {
	// Hot prices apply to data exported normally.
	Hot PriceList `mapstructure:"hot"`
	// Cold prices apply to diverted data. Required when a tier diverts.
	Cold configoptional.Optional[PriceList] `mapstructure:"cold"`
}

// PriceList holds prices in currency per unit.
type PriceList struct {
	// LogsGB is the price per GB (1e9 bytes) of logs.
	LogsGB float64 `mapstructure:"logs_gb"`
	// TracesGB is the price per GB (1e9 bytes) of traces.
	TracesGB float64 `mapstructure:"traces_gb"`
	// TracesMillionSpans is the price per million spans. When > 0 it is used
	// instead of TracesGB.
	TracesMillionSpans float64 `mapstructure:"traces_million_spans"`
	// MetricsMillionPoints is the price per million metric data points.
	MetricsMillionPoints float64 `mapstructure:"metrics_million_points"`
	// SeriesHour is the price per active series per hour.
	SeriesHour float64 `mapstructure:"series_hour"`
}

// CompressionConfig multiplies bytes before pricing, per signal.
type CompressionConfig struct {
	// Logs ratio. Default: 1.0.
	Logs float64 `mapstructure:"logs"`
	// Traces ratio. Default: 1.0.
	Traces float64 `mapstructure:"traces"`
	// Metrics ratio. Default: 1.0.
	Metrics float64 `mapstructure:"metrics"`
}

// BudgetRule assigns a budget to keys.
type BudgetRule struct {
	// Name of the rule, unique. Group ledger IDs are "group:<name>".
	Name string `mapstructure:"name"`
	// Match selects the keys of the rule: a key glob, resource attributes, or
	// the string "default".
	Match Match `mapstructure:"match"`
	// Scope is "key" (every matching key gets its own budget) or "group"
	// (matching keys share one budget). Default: key.
	Scope string `mapstructure:"scope"`
	// Budget is the amount per period in the configured currency. Must be > 0.
	Budget float64 `mapstructure:"budget"`
}

// Match selects keys: a key glob, resource attributes, or the literal
// string "default".
type Match struct {
	// Key is a glob matched against the budget key.
	Key string `mapstructure:"key"`
	// Attribute requires every listed resource attribute to equal its value.
	Attribute map[string]string `mapstructure:"attribute"`
	// Default is set by `match: default` and matches every key.
	Default bool `mapstructure:"-"`
}

// UnmarshalText accepts `match: default`.
func (m *Match) UnmarshalText(b []byte) error {
	if string(b) != "default" {
		return fmt.Errorf("match must be a map with key or attribute, or the string \"default\", got %q", string(b))
	}
	*m = Match{Default: true}
	return nil
}

// WindowsConfig are the burn rate windows.
type WindowsConfig struct {
	// Short window. Default: 1h.
	Short time.Duration `mapstructure:"short"`
	// Long window. Must be greater than Short and at most the period
	// (monthly counts as 28 days). Default: 6h.
	Long time.Duration `mapstructure:"long"`
}

// TierConfig is one enforcement level.
type TierConfig struct {
	// Threshold is the burn rate at which this tier applies, reached on both
	// windows. Must be > 0.
	Threshold float64 `mapstructure:"threshold"`
	// Actions applied at this tier.
	Actions ActionsConfig `mapstructure:"actions"`
}

// ActionsConfig are the actions of a tier.
type ActionsConfig struct {
	// DropBelowSeverity drops log records below TRACE, DEBUG, INFO, WARN,
	// ERROR, or FATAL. Records without a severity count as INFO.
	DropBelowSeverity string `mapstructure:"drop_below_severity"`
	// SampleRatio keeps this fraction of logs and traces, consistently by
	// trace ID. In (0, 1]. Default: 1.0.
	SampleRatio float64 `mapstructure:"sample_ratio"`
	// DropMetrics drops metrics whose name matches any of these globs.
	DropMetrics []string `mapstructure:"drop_metrics"`
	// Divert stamps otelcol.budget.divert = true so the routing connector can send
	// the data to a cold pipeline. Only the last tier may divert.
	Divert bool `mapstructure:"divert"`
}

// HysteresisConfig controls de-escalation.
type HysteresisConfig struct {
	// Ratio: a key drops one tier only while burn_long < threshold * ratio.
	// In (0, 1). Default: 0.8.
	Ratio float64 `mapstructure:"ratio"`
	// MinDwell is how long the condition must hold continuously.
	// Default: 30m.
	MinDwell time.Duration `mapstructure:"min_dwell"`
}

// AlwaysPassConfig is the never drop policy.
type AlwaysPassConfig struct {
	// LogSeverity: log records at or above it are never dropped.
	// Default: ERROR.
	LogSeverity string `mapstructure:"log_severity"`
	// SpanStatusError: spans with status ERROR are never dropped.
	// Default: true.
	SpanStatusError bool `mapstructure:"span_status_error"`
}

// SyncConfig selects how spend is shared.
type SyncConfig struct {
	// Mode is "local", "share", or "storage". Default: local.
	Mode string `mapstructure:"mode"`
	// Local configures local and share mode checkpoints.
	Local LocalSyncConfig `mapstructure:"local"`
	// Share configures share mode.
	Share ShareSyncConfig `mapstructure:"share"`
	// Storage configures storage mode.
	Storage StorageSyncConfig `mapstructure:"storage"`
}

// LocalSyncConfig configures checkpoints for local and share mode.
type LocalSyncConfig struct {
	// Storage is an optional storage extension for checkpoints, restored on
	// start.
	Storage *component.ID `mapstructure:"storage"`
	// CheckpointInterval between checkpoints. Default: 30s.
	CheckpointInterval time.Duration `mapstructure:"checkpoint_interval"`
}

// ShareSyncConfig configures share mode.
type ShareSyncConfig struct {
	// Replicas is N: each replica enforces budget / N.
	Replicas int `mapstructure:"replicas"`
	// ReplicasEnv names an environment variable holding N, used when
	// Replicas is not set.
	ReplicasEnv string `mapstructure:"replicas_env"`
}

// StorageSyncConfig configures storage mode.
type StorageSyncConfig struct {
	// Extension is the storage extension ID, for example
	// redis_storage/budget. Required in storage mode.
	Extension *component.ID `mapstructure:"extension"`
	// Mode is "auto" (increment when the storage client supports
	// BatchIncrementBy, else gcounter), "increment", or "gcounter".
	// Default: auto.
	Mode string `mapstructure:"mode"`
	// NodeID identifies this replica, for example ${env:POD_NAME}. Required
	// in storage mode.
	NodeID string `mapstructure:"node_id"`
	// NodePrefix: replicas are read as <node_prefix>-0 .. <node_prefix>-<max_nodes-1>,
	// so NodeID must be one of those names. Required in storage mode.
	NodePrefix string `mapstructure:"node_prefix"`
	// MaxNodes is the number of replica slots read. Default: 32.
	MaxNodes int `mapstructure:"max_nodes"`
	// FlushInterval between pushes of local spend. Default: 5s.
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	// ReadInterval between reads of fleet spend. Default: 30s.
	ReadInterval time.Duration `mapstructure:"read_interval"`
	// MaxPendingKeys bounds ledger IDs with unflushed spend; overflow is
	// counted and dropped. Default: 10000.
	MaxPendingKeys int `mapstructure:"max_pending_keys"`
}

// TelemetryConfig bounds self telemetry cardinality.
type TelemetryConfig struct {
	// MaxKeys: per key series only for the top keys by spend, the rest
	// aggregate under key="__other__". Default: 100.
	MaxKeys int `mapstructure:"max_keys"`
}

func createDefaultConfig() component.Config {
	return &Config{
		KeyAttributes:    []string{string(semconv.ServiceNameKey)},
		MaxKeys:          10000,
		Period:           "monthly",
		CompressionRatio: CompressionConfig{Logs: 1, Traces: 1, Metrics: 1},
		Windows:          WindowsConfig{Short: time.Hour, Long: 6 * time.Hour},
		DecisionInterval: 10 * time.Second,
		WarmupCoverage:   0.8,
		HardCap:          "highest",
		Hysteresis:       HysteresisConfig{Ratio: 0.8, MinDwell: 30 * time.Minute},
		StaleAfter:       5 * time.Minute,
		FailOpenAfter:    30 * time.Minute,
		AlwaysPass:       AlwaysPassConfig{LogSeverity: "ERROR", SpanStatusError: true},
		Sync: SyncConfig{
			Mode:  spendsync.ModeLocal,
			Local: LocalSyncConfig{CheckpointInterval: 30 * time.Second},
			Storage: StorageSyncConfig{
				Mode:           spendsync.ModeAuto,
				MaxNodes:       32,
				FlushInterval:  5 * time.Second,
				ReadInterval:   30 * time.Second,
				MaxPendingKeys: 10000,
			},
		},
		Telemetry: TelemetryConfig{MaxKeys: 100},
	}
}

// period parses Period.
func (c *Config) period() (ledger.Period, error) {
	if c.Period == "" || c.Period == "monthly" {
		return ledger.Period{Monthly: true}, nil
	}
	d, err := time.ParseDuration(c.Period)
	if err != nil || d <= 0 {
		return ledger.Period{}, fmt.Errorf("period must be \"monthly\" or a positive duration, got %q", c.Period)
	}
	return ledger.Period{Every: d}, nil
}

func (c *Config) hardCap() (decision.HardCap, error) {
	switch c.HardCap {
	case "off":
		return decision.HardCapOff, nil
	case "highest", "":
		return decision.HardCapHighest, nil
	case "divert":
		return decision.HardCapDivert, nil
	}
	return 0, fmt.Errorf("hard_cap must be off, highest, or divert, got %q", c.HardCap)
}

func (c *Config) hasDivertTier() bool {
	for _, t := range c.Tiers {
		if t.Actions.Divert {
			return true
		}
	}
	return false
}

// tiers converts and compiles the tier list.
func (c *Config) tiers() ([]decision.Tier, error) {
	out := make([]decision.Tier, 0, len(c.Tiers))
	for i, t := range c.Tiers {
		a := budgetapi.Action{SampleRatio: t.Actions.SampleRatio, DropMetrics: t.Actions.DropMetrics, Divert: t.Actions.Divert}
		if a.SampleRatio == 0 {
			a.SampleRatio = 1
		}
		if t.Actions.DropBelowSeverity != "" {
			s, err := budgetapi.ParseSeverity(t.Actions.DropBelowSeverity)
			if err != nil {
				return nil, fmt.Errorf("tiers[%d]: %w", i, err)
			}
			a.DropBelowSeverity = s
		}
		if err := a.Compile(); err != nil {
			return nil, fmt.Errorf("tiers[%d]: %w", i, err)
		}
		out = append(out, decision.Tier{Threshold: t.Threshold, Actions: a})
	}
	return out, nil
}

func (c *Config) ruleSpecs() []keying.RuleSpec {
	out := make([]keying.RuleSpec, 0, len(c.Budgets))
	for _, b := range c.Budgets {
		s := keying.RuleSpec{Name: b.Name, Default: b.Match.Default, KeyGlob: b.Match.Key, Attribute: b.Match.Attribute}
		if b.Scope == "group" {
			s.Scope = keying.ScopeGroup
		}
		out = append(out, s)
	}
	return out
}

func (c *Config) costModel() costmodel.Model {
	conv := func(p PriceList) costmodel.Prices {
		return costmodel.Prices{
			LogsGB:               costmodel.ToMicro(p.LogsGB),
			TracesGB:             costmodel.ToMicro(p.TracesGB),
			TracesMillionSpans:   costmodel.ToMicro(p.TracesMillionSpans),
			MetricsMillionPoints: costmodel.ToMicro(p.MetricsMillionPoints),
			SeriesHour:           costmodel.ToMicro(p.SeriesHour),
		}
	}
	var cold costmodel.Prices
	if p := c.Prices.Cold.Get(); p != nil {
		cold = conv(*p)
	}
	cr := c.CompressionRatio
	return costmodel.NewModel(conv(c.Prices.Hot), cold, [budgetapi.NumSignals]float64{cr.Logs, cr.Traces, cr.Metrics})
}

// shareReplicas resolves N for share mode.
func (c *Config) shareReplicas() (int64, error) {
	if c.Sync.Share.Replicas > 0 {
		return int64(c.Sync.Share.Replicas), nil
	}
	if c.Sync.Share.ReplicasEnv == "" {
		return 0, errors.New("sync.share requires replicas or replicas_env")
	}
	v := strings.TrimSpace(os.Getenv(c.Sync.Share.ReplicasEnv))
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("env %s must hold a positive integer, got %q", c.Sync.Share.ReplicasEnv, v)
	}
	return n, nil
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if len(c.KeyAttributes) == 0 {
		add("key_attributes must not be empty")
	}
	if c.MaxKeys < 1 {
		add("max_keys must be at least 1")
	}
	period, err := c.period()
	if err != nil {
		errs = append(errs, err)
	}

	// budgets
	if len(c.Budgets) == 0 {
		add("budgets must not be empty")
	}
	names := map[string]bool{}
	defaults := 0
	for i, b := range c.Budgets {
		if b.Name == "" {
			add("budgets[%d]: name is required", i)
		} else if names[b.Name] {
			add("budgets[%d]: duplicate rule name %q", i, b.Name)
		}
		names[b.Name] = true
		if b.Budget <= 0 {
			add("budgets[%d] %q: budget must be > 0", i, b.Name)
		}
		switch b.Scope {
		case "", "key", "group":
		default:
			add("budgets[%d] %q: scope must be key or group", i, b.Name)
		}
		if b.Match.Default {
			defaults++
			if i != len(c.Budgets)-1 {
				add("budgets[%d] %q: the default rule must be last", i, b.Name)
			}
			if b.Scope == "group" {
				add("budgets[%d] %q: the default rule must have scope key", i, b.Name)
			}
		} else if b.Match.Key == "" && len(b.Match.Attribute) == 0 {
			add("budgets[%d] %q: match needs key, attribute, or default", i, b.Name)
		}
	}
	if len(c.Budgets) > 0 && defaults != 1 {
		add("budgets must contain exactly one default rule, found %d", defaults)
	}
	if _, kerr := keying.New(c.KeyAttributes, max(c.MaxKeys, 1), c.ruleSpecs()); kerr != nil {
		errs = append(errs, kerr)
	}

	// tiers
	if len(c.Tiers) < 1 || len(c.Tiers) > 5 {
		add("tiers must have 1 to 5 entries, got %d", len(c.Tiers))
	}
	divertTiers := 0
	for i, t := range c.Tiers {
		if t.Threshold <= 0 {
			add("tiers[%d]: threshold must be > 0", i)
		}
		if i > 0 && t.Threshold <= c.Tiers[i-1].Threshold {
			add("tiers[%d]: thresholds must be strictly increasing", i)
		}
		if t.Actions.SampleRatio < 0 || t.Actions.SampleRatio > 1 {
			add("tiers[%d]: sample_ratio must be in (0, 1]", i)
		}
		if t.Actions.Divert {
			divertTiers++
			if i != len(c.Tiers)-1 {
				add("tiers[%d]: only the last tier may divert", i)
			}
		}
	}
	if divertTiers > 1 {
		add("at most one tier may divert, found %d", divertTiers)
	}
	if _, terr := c.tiers(); terr != nil {
		errs = append(errs, terr)
	}
	hc, err := c.hardCap()
	if err != nil {
		errs = append(errs, err)
	}
	if hc == decision.HardCapDivert && !c.hasDivertTier() {
		add("hard_cap: divert requires a tier with divert: true")
	}
	if c.hasDivertTier() && !c.Prices.Cold.HasValue() {
		add("prices.cold is required when a tier diverts")
	}

	// prices
	for name, v := range map[string]float64{
		"logs_gb": c.Prices.Hot.LogsGB, "traces_gb": c.Prices.Hot.TracesGB, "traces_million_spans": c.Prices.Hot.TracesMillionSpans,
		"metrics_million_points": c.Prices.Hot.MetricsMillionPoints, "series_hour": c.Prices.Hot.SeriesHour,
	} {
		if v < 0 {
			add("prices.hot.%s must be >= 0", name)
		}
	}
	if cr := c.CompressionRatio; cr.Logs < 0 || cr.Traces < 0 || cr.Metrics < 0 {
		add("compression_ratio values must be >= 0")
	}

	// windows and timing
	periodLen := 28 * 24 * time.Hour
	if !period.Monthly && period.Every > 0 {
		periodLen = period.Every
	}
	if c.Windows.Short <= 0 || c.Windows.Short >= c.Windows.Long || c.Windows.Long > periodLen {
		add("windows must satisfy 0 < short < long <= period (monthly counts as 28 days)")
	}
	if c.DecisionInterval <= 0 {
		add("decision_interval must be > 0")
	}
	if c.WarmupCoverage < 0 || c.WarmupCoverage > 1 {
		add("warmup_coverage must be in [0, 1]")
	}
	if c.Hysteresis.Ratio <= 0 || c.Hysteresis.Ratio >= 1 {
		add("hysteresis.ratio must be in (0, 1)")
	}
	if c.Hysteresis.MinDwell < 0 {
		add("hysteresis.min_dwell must be >= 0")
	}
	if c.StaleAfter <= 0 || c.StaleAfter >= c.FailOpenAfter {
		add("stale_after must be > 0 and < fail_open_after")
	}
	if _, err := budgetapi.ParseSeverity(c.AlwaysPass.LogSeverity); err != nil {
		errs = append(errs, fmt.Errorf("always_pass.log_severity: %w", err))
	}

	// sync
	switch c.Sync.Mode {
	case spendsync.ModeLocal:
	case spendsync.ModeShare:
		if c.Sync.Share.Replicas <= 0 && c.Sync.Share.ReplicasEnv == "" {
			add("sync.mode share requires sync.share.replicas or sync.share.replicas_env")
		}
	case spendsync.ModeStorage:
		s := c.Sync.Storage
		if s.Extension == nil {
			add("sync.mode storage requires sync.storage.extension")
		}
		// every storage mode finds the other replicas as <node_prefix>-N
		if s.NodeID == "" || s.NodePrefix == "" {
			add("sync.mode storage requires sync.storage.node_id and sync.storage.node_prefix")
		}
		switch s.Mode {
		case spendsync.ModeAuto, spendsync.ModeIncrement, spendsync.ModeGCounter:
		default:
			add("sync.storage.mode must be auto, increment, or gcounter")
		}
		if s.MaxNodes < 1 || s.FlushInterval <= 0 || s.ReadInterval <= 0 || s.MaxPendingKeys < 1 {
			add("sync.storage max_nodes, flush_interval, read_interval, and max_pending_keys must be positive")
		}
	default:
		add("sync.mode must be local, share, or storage, got %q", c.Sync.Mode)
	}

	groups := map[string]bool{}
	for _, b := range c.Budgets {
		if b.Scope == "group" {
			groups[b.Name] = true
		}
	}
	for i, k := range c.Exempt {
		if k == "" {
			add("exempt[%d] must not be empty", i)
		} else if name, ok := strings.CutPrefix(k, keying.GroupPrefix); ok && !groups[name] {
			add("exempt[%d]: %q does not name a group rule", i, k)
		}
	}
	if c.Telemetry.MaxKeys < 1 {
		add("telemetry.max_keys must be at least 1")
	}
	return errors.Join(errs...)
}
