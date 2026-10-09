package seriescount

import (
	"math"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
)

func TestEstimateAccuracy(t *testing.T) {
	for _, n := range []int{0, 1, 100, 1000, 10000, 200000} {
		var s Sketch
		for i := range n {
			s.Insert(xxhash.Sum64String("series-" + string(rune(i)) + "-" + itoa(i)))
		}
		est := float64(s.Estimate())
		if n == 0 {
			assert.Zero(t, est)
			continue
		}
		relErr := math.Abs(est-float64(n)) / float64(n)
		assert.Less(t, relErr, 0.05, "n=%d est=%v", n, est)
	}
}

func itoa(i int) string {
	b := []byte{}
	for {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
		if i == 0 {
			return string(b)
		}
	}
}

func TestDuplicatesAndConcurrency(t *testing.T) {
	var c Counter
	assert.Zero(t, c.Rotate())
	c.Insert(nil)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			hs := make([]uint64, 0, 5000)
			for i := range 5000 {
				hs = append(hs, xxhash.Sum64String(itoa(i)))
			}
			c.Insert(hs)
		})
	}
	wg.Wait()
	est := c.Rotate()
	assert.InDelta(t, 5000, float64(est), 250)
	assert.Zero(t, c.Rotate(), "rotated sketch starts empty")
}
