package keying

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/paulojmdias/otel-budget-components/extension/budgetextension/budgetapi"
)

func resource(kv ...string) pcommon.Resource {
	r := pcommon.NewResource()
	for i := 0; i < len(kv); i += 2 {
		r.Attributes().PutStr(kv[i], kv[i+1])
	}
	return r
}

func TestKeyAttributes(t *testing.T) {
	r, err := New([]string{"tenant.id", "service.name"}, 100, []RuleSpec{{Name: "default", Default: true}})
	require.NoError(t, err)
	assert.Equal(t, "acme|checkout", r.ResolveKey(resource("tenant.id", "acme", "service.name", "checkout")))
	assert.Equal(t, "unknown|checkout", r.ResolveKey(resource("service.name", "checkout")))
	assert.Equal(t, "default", r.ResolveKey(resource("other", "x")))
	nonStr := pcommon.NewResource()
	nonStr.Attributes().PutInt("tenant.id", 7)
	nonStr.Attributes().PutStr("service.name", "a")
	assert.Equal(t, "7|a", r.ResolveKey(nonStr))
	// cache hit returns the same
	assert.Equal(t, "acme|checkout", r.ResolveKey(resource("tenant.id", "acme", "service.name", "checkout")))
	assert.Equal(t, int64(4), r.Keys())
}

func TestOverflow(t *testing.T) {
	r, err := New([]string{"service.name"}, 2, []RuleSpec{
		{Name: "other", KeyGlob: OtherKey},
		{Name: "default", Default: true},
	})
	require.NoError(t, err)
	assert.Equal(t, "a", r.ResolveKey(resource("service.name", "a")))
	assert.Equal(t, "b", r.ResolveKey(resource("service.name", "b")))
	assert.Equal(t, OtherKey, r.ResolveKey(resource("service.name", "c")))
	assert.Equal(t, OtherKey, r.ResolveKey(resource("service.name", "d")))
	assert.Equal(t, "a", r.ResolveKey(resource("service.name", "a")))
	assert.Equal(t, int64(2), r.Overflows())
	assert.Equal(t, 0, r.Lookup(OtherKey).KeyRule)
	assert.Equal(t, 1, r.Lookup("a").KeyRule)

	r.ResetPeriod()
	assert.Equal(t, "c", r.ResolveKey(resource("service.name", "c")))
	assert.Equal(t, int64(0), r.Overflows())
}

func TestRules(t *testing.T) {
	r, err := New([]string{"tenant.id", "service.name"}, 100, []RuleSpec{
		{Name: "checkout", KeyGlob: "*|checkout"},
		{Name: "payments-team", Scope: ScopeGroup, Attribute: map[string]string{"team": "payments"}},
		{Name: "acme", Scope: ScopeGroup, KeyGlob: "acme|*"},
		{Name: "default", Default: true},
	})
	require.NoError(t, err)
	assert.Equal(t, "checkout", r.RuleName(0))

	tok := r.ResolveKey(resource("tenant.id", "acme", "service.name", "checkout", "team", "payments"))
	assert.Equal(t, "acme|checkout", budgetapi.KeyName(tok))
	res := r.Lookup(tok)
	assert.Equal(t, 0, res.KeyRule)
	assert.Equal(t, []int{1, 2}, res.Groups)
	assert.Equal(t, []string{"acme|checkout", "group:payments-team", "group:acme"}, res.LedgerIDs(r))

	tok2 := r.ResolveKey(resource("tenant.id", "acme", "service.name", "checkout", "team", "search"))
	assert.NotEqual(t, tok, tok2, "different group membership, different token")
	assert.Equal(t, []int{2}, r.Lookup(tok2).Groups)

	tok3 := r.ResolveKey(resource("tenant.id", "x", "service.name", "search"))
	res3 := r.Lookup(tok3)
	assert.Equal(t, 3, res3.KeyRule)
	assert.Empty(t, res3.Groups)

	n := 0
	r.RangeResolutions(func(*Resolution) { n++ })
	assert.Equal(t, 3, n)

	// tokens survive a period reset by decoding
	r.ResetPeriod()
	again := r.Lookup(tok)
	assert.Equal(t, 0, again.KeyRule)
	assert.Equal(t, []int{1, 2}, again.Groups)
	assert.Equal(t, "acme|checkout", again.Key)
	assert.Same(t, again, r.Lookup(tok))
	bad := r.Lookup("k" + budgetapi.KeySeparator + "x;99,1")
	assert.Equal(t, -1, bad.KeyRule)
	assert.Equal(t, []int{1}, bad.Groups)
}

func TestLookupWithoutAttributeRules(t *testing.T) {
	r, err := New([]string{"service.name"}, 100, []RuleSpec{
		{Name: "g", Scope: ScopeGroup, KeyGlob: "check*"},
		{Name: "default", Default: true},
	})
	require.NoError(t, err)
	res := r.Lookup("checkout")
	assert.Equal(t, "checkout", res.Token)
	assert.Equal(t, 1, res.KeyRule)
	assert.Equal(t, []int{0}, res.Groups)
	assert.Equal(t, "group:g", r.GroupLedgerID(0))
}

func TestBadGlob(t *testing.T) {
	_, err := New(nil, 1, []RuleSpec{{Name: "x", KeyGlob: "[a"}})
	require.Error(t, err)
}

func TestNoMatchWithoutDefault(t *testing.T) {
	r, err := New([]string{"service.name"}, 10, []RuleSpec{{Name: "empty"}})
	require.NoError(t, err)
	assert.Equal(t, -1, r.Lookup(r.ResolveKey(resource("service.name", "a"))).KeyRule)
}

func TestConcurrentResolve(t *testing.T) {
	r, err := New([]string{"service.name"}, 50, []RuleSpec{{Name: "default", Default: true}})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				k := r.ResolveKey(resource("service.name", fmt.Sprintf("s%d", (i+g)%100)))
				_ = r.Lookup(k)
			}
		})
	}
	wg.Wait()
	assert.LessOrEqual(t, r.Keys(), int64(50+8), "bounded with small concurrent slack")
}

func BenchmarkResolveKeyHit(b *testing.B) {
	r, _ := New([]string{"tenant.id", "service.name"}, 10000, []RuleSpec{{Name: "default", Default: true}})
	res := resource("tenant.id", "acme", "service.name", "checkout", "host.name", "h1")
	r.ResolveKey(res)
	b.ReportAllocs()
	for b.Loop() {
		r.ResolveKey(res)
	}
}

func BenchmarkResolveKeyMiss(b *testing.B) {
	r, _ := New([]string{"tenant.id", "service.name"}, 1<<30, []RuleSpec{{Name: "default", Default: true}})
	rs := make([]pcommon.Resource, 1<<16)
	for i := range rs {
		rs[i] = resource("tenant.id", "acme", "service.name", fmt.Sprintf("svc-%d", i))
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		r.ResolveKey(rs[i&(len(rs)-1)])
		i++
		if i%len(rs) == 0 {
			r.ResetPeriod()
		}
	}
}
