package budgetapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestHashes(t *testing.T) {
	a := pcommon.NewMap()
	a.PutStr("x", "1")
	a.PutInt("y", 2)
	b := pcommon.NewMap()
	b.PutInt("y", 2)
	b.PutStr("x", "1")
	assert.Equal(t, MapHash(a, ""), MapHash(b, ""), "order independent")
	b.PutStr("x", "2")
	assert.NotEqual(t, MapHash(a, ""), MapHash(b, ""))
	assert.Zero(t, MapHash(pcommon.NewMap(), ""))
	c := pcommon.NewMap()
	c.PutStr("x", "1")
	c.PutInt("y", 2)
	c.PutStr("otelcol.budget.tier", "3")
	assert.Equal(t, MapHash(a, "otelcol.budget."), MapHash(c, "otelcol.budget."))
	h1 := SeriesHash(1, NameHash("s"), NameHash("m"), 3)
	assert.Equal(t, h1, SeriesHash(1, NameHash("s"), NameHash("m"), 3))
	assert.NotEqual(t, h1, SeriesHash(1, NameHash("s"), NameHash("n"), 3))
	assert.NotEqual(t, h1, SeriesHash(2, NameHash("s"), NameHash("m"), 3))

	// a key and value swapped is another attribute
	d, e := pcommon.NewMap(), pcommon.NewMap()
	d.PutStr("a", "b")
	e.PutStr("b", "a")
	assert.NotEqual(t, MapHash(d, ""), MapHash(e, ""))
	// an int hashes like its string form, without allocating
	f, g := pcommon.NewMap(), pcommon.NewMap()
	f.PutInt("code", 200)
	g.PutStr("code", "200")
	assert.Equal(t, MapHash(f, ""), MapHash(g, ""))
	assert.Zero(t, testing.AllocsPerRun(100, func() { MapHash(f, "") }))
}
