package budgetapi // import "github.com/paulojmdias/otel-budget-processor/extension/budgetextension/budgetapi"

import (
	"encoding/binary"
	"math/bits"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Series identities are part of the contract: every processor must hash the
// same series to the same value so the extension can count them across
// pipelines and replicas.

// MapHash hashes a map independently of key order by summing per entry
// hashes, which avoids sorting and allocation on the hot path. Keys starting
// with skipPrefix (when non empty) are ignored, so budget stamps do not
// change series identity.
func MapHash(m pcommon.Map, skipPrefix string) uint64 {
	var sum uint64
	for k, v := range m.All() {
		if skipPrefix != "" && strings.HasPrefix(k, skipPrefix) {
			continue
		}
		sum += mix(xxhash.Sum64String(k), valueHash(v))
	}
	return sum
}

// valueHash hashes a value's string form; ints are formatted on the stack,
// so they hash like their AsString without allocating.
func valueHash(v pcommon.Value) uint64 {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return xxhash.Sum64String(v.Str())
	case pcommon.ValueTypeInt:
		var b [20]byte
		return xxhash.Sum64(strconv.AppendInt(b[:0], v.Int(), 10))
	}
	return xxhash.Sum64String(v.AsString())
}

// mix combines a key and a value hash asymmetrically (x=y differs from y=x),
// with the splitmix64 finalizer.
func mix(k, v uint64) uint64 {
	x := k*0x9e3779b97f4a7c15 ^ bits.RotateLeft64(v, 32)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ x>>31
}

// SeriesHash combines the identity parts of one series. metric is
// NameHash(metric name), computed once per metric rather than per point.
func SeriesHash(resource, scope, metric, point uint64) uint64 {
	var b [32]byte
	binary.LittleEndian.PutUint64(b[0:], resource)
	binary.LittleEndian.PutUint64(b[8:], scope)
	binary.LittleEndian.PutUint64(b[16:], metric)
	binary.LittleEndian.PutUint64(b[24:], point)
	return xxhash.Sum64(b[:])
}

// NameHash hashes a scope or metric name.
func NameHash(name string) uint64 { return xxhash.Sum64String(name) }
