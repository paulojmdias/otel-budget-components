package budgetapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestParseSeverity(t *testing.T) {
	for in, want := range map[string]plog.SeverityNumber{
		"TRACE": 1, "debug": 5, "INFO": 9, "Warn": 13, "WARNING": 13, "ERROR": 17, "FATAL": 21,
	} {
		got, err := ParseSeverity(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	_, err := ParseSeverity("LOUD")
	require.Error(t, err)
}

func TestStrings(t *testing.T) {
	assert.Equal(t, "logs", SignalLogs.String())
	assert.Equal(t, "traces", SignalTraces.String())
	assert.Equal(t, "metrics", SignalMetrics.String())
	assert.Equal(t, "unknown", Signal(9).String())
	assert.Equal(t, "hot", PriceHot.String())
	assert.Equal(t, "cold", PriceCold.String())
	assert.Equal(t, "severity", DropSeverity.String())
	assert.Equal(t, "sample", DropSample.String())
	assert.Equal(t, "metric_glob", DropMetricGlob.String())
	c, err := ParsePriceClass("cold")
	require.NoError(t, err)
	assert.Equal(t, PriceCold, c)
	_, err = ParsePriceClass("warm")
	require.Error(t, err)
}

func TestKeyName(t *testing.T) {
	assert.Equal(t, "checkout", KeyName("checkout"))
	assert.Equal(t, "checkout", KeyName("checkout"+KeySeparator+"0;1,2"))
}

func TestActionCompile(t *testing.T) {
	a := Action{DropMetrics: []string{"go_*", "*_bucket"}}
	assert.False(t, a.DropsMetric("go_gc"), "not compiled yet")
	require.NoError(t, a.Compile())
	assert.True(t, a.DropsMetric("go_gc"))
	assert.True(t, a.DropsMetric("http_bucket"))
	assert.False(t, a.DropsMetric("http_requests"))
	bad := Action{DropMetrics: []string{"[a"}}
	require.Error(t, bad.Compile())
	assert.True(t, Action{SampleRatio: 0.5}.Samples())
	assert.False(t, Action{SampleRatio: 1}.Samples())
}

func TestMostRestrictive(t *testing.T) {
	a := Decision{Tier: 1, Actions: Action{SampleRatio: 0.5}, Exempt: true}
	c := Decision{Tier: 2, Actions: Action{SampleRatio: 0.9}}
	assert.Equal(t, 2, MostRestrictive(a, c).Tier)
	assert.True(t, MostRestrictive(a, c).Exempt, "flags come from the first decision")
	assert.Equal(t, 2, MostRestrictive(c, a).Tier)
	assert.Equal(t, a, MostRestrictive(a, Decision{Tier: 1}), "equal tiers keep the first")
}
