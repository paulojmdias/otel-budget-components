// Package keying turns resource attributes into budget keys and matches keys
// against budget rules.
package keying // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/internal/keying"

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/gobwas/glob"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"
)

const (
	// UnknownValue replaces a missing key attribute.
	UnknownValue = "unknown"
	// DefaultKey is used when all key attributes are missing.
	DefaultKey = "default"
	// OtherKey collects keys beyond max_keys.
	OtherKey = "__other__"
	// GroupPrefix prefixes ledger IDs of group scoped rules.
	GroupPrefix = "group:"
	// OfferedPrefix prefixes the ledger IDs that track offered (pre action)
	// spend of a key or group.
	OfferedPrefix = "offered:"

	cacheSize = 4096 // power of two
)

// Scope of a budget rule.
type Scope uint8

const (
	ScopeKey Scope = iota
	ScopeGroup
)

// RuleSpec describes one budget rule.
type RuleSpec struct {
	Name      string
	Scope     Scope
	Default   bool
	KeyGlob   string
	Attribute map[string]string
}

type attrMatch struct {
	idx   int // index into Resolver.relevant
	value string
}

type rule struct {
	RuleSpec
	glob  glob.Glob
	attrs []attrMatch
}

// Resolution is the budget identity of a resource.
type Resolution struct {
	Key     string // plain key, stamped as otelcol.budget.key
	KeyRule int    // index of the first matching key scoped rule, -1 if none
	Groups  []int  // indexes of all matching group scoped rules
	Token   string // what ResolveKey returns
	// OfferedKey is the ledger ID of the key's offered spend.
	OfferedKey string
}

// LedgerIDs returns the key followed by the ledger IDs of matched groups.
func (r *Resolution) LedgerIDs(rv *Resolver) []string {
	ids := make([]string, 0, 1+len(r.Groups))
	ids = append(ids, r.Key)
	for _, g := range r.Groups {
		ids = append(ids, rv.GroupLedgerID(g))
	}
	return ids
}

// Resolver resolves resources to keys. ResolveKey and Lookup are lock free
// on the hit path.
type Resolver struct {
	keyAttrs        []string
	relevant        []string // key attributes then attributes used by rules
	rules           []rule
	groupIDs        []string
	offeredGroupIDs []string
	attrRules       bool
	maxKeys         int64
	state           atomic.Pointer[periodState]
}

type periodState struct {
	keys      sync.Map // plain key -> struct{}
	nkeys     atomic.Int64
	overflows atomic.Int64
	tokens    sync.Map // token -> *Resolution
	cache     [cacheSize]atomic.Pointer[cacheEntry]
}

type cacheEntry struct {
	hash uint64
	vals []string
	res  *Resolution
}

// New builds a resolver. Rules are matched in order.
func New(keyAttrs []string, maxKeys int, specs []RuleSpec) (*Resolver, error) {
	r := &Resolver{keyAttrs: keyAttrs, maxKeys: int64(maxKeys)}
	r.relevant = append(r.relevant, keyAttrs...)
	idx := map[string]int{}
	for i, a := range keyAttrs {
		if _, ok := idx[a]; !ok {
			idx[a] = i
		}
	}
	for _, s := range specs {
		ru := rule{RuleSpec: s}
		if s.KeyGlob != "" {
			g, err := glob.Compile(s.KeyGlob)
			if err != nil {
				return nil, fmt.Errorf("rule %q: invalid key glob: %w", s.Name, err)
			}
			ru.glob = g
		}
		for name, v := range s.Attribute {
			i, ok := idx[name]
			if !ok {
				i = len(r.relevant)
				idx[name] = i
				r.relevant = append(r.relevant, name)
			}
			ru.attrs = append(ru.attrs, attrMatch{idx: i, value: v})
			r.attrRules = true
		}
		r.rules = append(r.rules, ru)
		r.groupIDs = append(r.groupIDs, GroupPrefix+s.Name)
		r.offeredGroupIDs = append(r.offeredGroupIDs, OfferedPrefix+GroupPrefix+s.Name)
	}
	r.state.Store(&periodState{})
	return r, nil
}

// RuleName returns the name of rule i.
func (r *Resolver) RuleName(i int) string { return r.rules[i].Name }

// GroupLedgerID returns the ledger ID of group rule i.
func (r *Resolver) GroupLedgerID(i int) string { return r.groupIDs[i] }

// OfferedGroupLedgerID returns the ledger ID of group rule i's offered spend.
func (r *Resolver) OfferedGroupLedgerID(i int) string { return r.offeredGroupIDs[i] }

// ResetPeriod forgets all keys, so max_keys applies per period.
func (r *Resolver) ResetPeriod() { r.state.Store(&periodState{}) }

// Keys returns the number of distinct keys this period.
func (r *Resolver) Keys() int64 { return r.state.Load().nkeys.Load() }

// Overflows returns how many new keys were folded into __other__ this period.
func (r *Resolver) Overflows() int64 { return r.state.Load().overflows.Load() }

// RangeResolutions calls f for every resolution seen this period.
func (r *Resolver) RangeResolutions(f func(*Resolution)) {
	r.state.Load().tokens.Range(func(_, v any) bool {
		f(v.(*Resolution))
		return true
	})
}

func attrString(m pcommon.Map, name string) (string, bool) {
	v, ok := m.Get(name)
	if !ok {
		return "", false
	}
	if v.Type() == pcommon.ValueTypeStr {
		return v.Str(), true
	}
	return v.AsString(), true
}

// ResolveKey returns the resolved key token for a resource.
func (r *Resolver) ResolveKey(res pcommon.Resource) string {
	attrs := res.Attributes()
	var d xxhash.Digest
	d.Reset()
	for _, name := range r.relevant {
		if s, ok := attrString(attrs, name); ok {
			_, _ = d.WriteString("\x01")
			_, _ = d.WriteString(s)
		} else {
			_, _ = d.WriteString("\x00")
		}
		_, _ = d.WriteString("\x1e")
	}
	h := d.Sum64()
	st := r.state.Load()
	slot := &st.cache[h&(cacheSize-1)]
	if e := slot.Load(); e != nil && e.hash == h && sameValues(attrs, r.relevant, e.vals) {
		return e.res.Token
	}
	vals := make([]string, len(r.relevant))
	for i, name := range r.relevant {
		if s, ok := attrString(attrs, name); ok {
			vals[i] = s
		} else {
			vals[i] = "\x00"
		}
	}
	resolved := r.resolve(st, vals)
	slot.Store(&cacheEntry{hash: h, vals: vals, res: resolved})
	return resolved.Token
}

func sameValues(attrs pcommon.Map, names, vals []string) bool {
	for i, name := range names {
		s, ok := attrString(attrs, name)
		if !ok {
			s = "\x00"
		}
		if s != vals[i] {
			return false
		}
	}
	return true
}

func (r *Resolver) buildKey(vals []string) string {
	parts := make([]string, len(r.keyAttrs))
	missing := 0
	for i := range r.keyAttrs {
		if vals[i] == "\x00" {
			parts[i] = UnknownValue
			missing++
		} else {
			parts[i] = vals[i]
		}
	}
	if missing == len(r.keyAttrs) {
		return DefaultKey
	}
	return strings.Join(parts, "|")
}

func (r *Resolver) admit(st *periodState, key string) string {
	if key == OtherKey {
		return key
	}
	if _, ok := st.keys.Load(key); ok {
		return key
	}
	if st.nkeys.Load() >= r.maxKeys {
		st.overflows.Add(1)
		return OtherKey
	}
	if _, loaded := st.keys.LoadOrStore(key, struct{}{}); !loaded {
		st.nkeys.Add(1)
	}
	return key
}

func (r *Resolver) resolve(st *periodState, vals []string) *Resolution {
	key := r.admit(st, r.buildKey(vals))
	res := &Resolution{Key: key, KeyRule: -1, OfferedKey: OfferedPrefix + key}
	for i := range r.rules {
		ru := &r.rules[i]
		if !ru.matches(key, vals) {
			continue
		}
		if ru.Scope == ScopeGroup {
			res.Groups = append(res.Groups, i)
		} else if res.KeyRule < 0 {
			res.KeyRule = i
		}
	}
	res.Token = r.token(res)
	if v, loaded := st.tokens.LoadOrStore(res.Token, res); loaded {
		return v.(*Resolution)
	}
	return res
}

func (ru *rule) matches(key string, vals []string) bool {
	if ru.Default {
		return true
	}
	if ru.glob != nil && !ru.glob.Match(key) {
		return false
	}
	for _, a := range ru.attrs {
		if a.idx >= len(vals) || vals[a.idx] != a.value {
			return false
		}
	}
	return ru.glob != nil || len(ru.attrs) > 0
}

// token encodes rule membership only when attribute rules exist, because
// otherwise the key alone determines it.
func (r *Resolver) token(res *Resolution) string {
	if !r.attrRules {
		return res.Key
	}
	var b strings.Builder
	b.WriteString(res.Key)
	b.WriteString(budgetapi.KeySeparator)
	b.WriteString(strconv.Itoa(res.KeyRule))
	b.WriteByte(';')
	for i, g := range res.Groups {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(g))
	}
	return b.String()
}

// Lookup returns the resolution for a token. Tokens from an earlier period
// are decoded and registered again.
func (r *Resolver) Lookup(token string) *Resolution {
	st := r.state.Load()
	if v, ok := st.tokens.Load(token); ok {
		return v.(*Resolution)
	}
	res := r.decode(token)
	if v, loaded := st.tokens.LoadOrStore(res.Token, res); loaded {
		return v.(*Resolution)
	}
	return res
}

func (r *Resolver) decode(token string) *Resolution {
	key, enc, found := strings.Cut(token, budgetapi.KeySeparator)
	if !found || !r.attrRules {
		res := &Resolution{Key: key, KeyRule: -1, OfferedKey: OfferedPrefix + key}
		for i := range r.rules {
			ru := &r.rules[i]
			if len(ru.attrs) > 0 || !ru.matches(key, nil) {
				continue
			}
			if ru.Scope == ScopeGroup {
				res.Groups = append(res.Groups, i)
			} else if res.KeyRule < 0 {
				res.KeyRule = i
			}
		}
		res.Token = r.token(res)
		return res
	}
	res := &Resolution{Key: key, KeyRule: -1, Token: token, OfferedKey: OfferedPrefix + key}
	kr, groups, _ := strings.Cut(enc, ";")
	if n, err := strconv.Atoi(kr); err == nil && n < len(r.rules) {
		res.KeyRule = n
	}
	if groups != "" {
		for g := range strings.SplitSeq(groups, ",") {
			if n, err := strconv.Atoi(g); err == nil && n >= 0 && n < len(r.rules) {
				res.Groups = append(res.Groups, n)
			}
		}
	}
	return res
}
