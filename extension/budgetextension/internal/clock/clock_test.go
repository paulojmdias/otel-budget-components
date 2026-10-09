package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFake(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(start)
	assert.Equal(t, start, f.Now())
	tk := f.NewTicker(10 * time.Second)
	f.Advance(5 * time.Second)
	select {
	case <-tk.C():
		t.Fatal("ticked early")
	default:
	}
	f.Advance(5 * time.Second)
	assert.Equal(t, start.Add(10*time.Second), <-tk.C())
	f.Advance(30 * time.Second) // drops ticks for slow receivers
	assert.Equal(t, start.Add(20*time.Second), <-tk.C())
	tk.Stop()
	f.Advance(time.Minute)
	select {
	case <-tk.C():
		t.Fatal("ticked after stop")
	default:
	}
	f.Set(start)
	assert.Equal(t, start, f.Now())
}

func TestReal(t *testing.T) {
	c := Real()
	assert.WithinDuration(t, time.Now(), c.Now(), time.Second)
	tk := c.NewTicker(time.Millisecond)
	<-tk.C()
	tk.Stop()
}
