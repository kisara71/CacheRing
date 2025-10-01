package types

import (
	"sync/atomic"
	"time"
)

type PromoteGate struct {
	windowReads        atomic.Int64
	windowMisses       atomic.Int64
	missSinceLastCheck atomic.Int64

	lastCheckUnixNano atomic.Int64
	checking          atomic.Int32

	minPromoteInterval time.Duration
	targetMissHardPct  int64
	minDirtyBuckets    int
	minDirtyKeys       int64
	sampleMask         uint64
}

func NewPromoteGate() *PromoteGate {
	return &PromoteGate{
		minPromoteInterval: 20 * time.Millisecond,
		targetMissHardPct:  2,
		minDirtyBuckets:    8,
		minDirtyKeys:       4096,
		sampleMask:         1023,
	}
}

func (g *PromoteGate) OnRead() {
	g.windowReads.Add(1)
}

func (g *PromoteGate) OnMiss() {
	g.windowMisses.Add(1)
	g.missSinceLastCheck.Add(1)
}

func (g *PromoteGate) ShouldPromote(now time.Time, hashCounter uint64, dirtyBuckets int, dirtyKeys int64) bool {
	if (hashCounter & g.sampleMask) != 0 {
		return false
	}
	last := time.Unix(0, g.lastCheckUnixNano.Load())
	if !last.IsZero() && now.Sub(last) < g.minPromoteInterval {
		return false
	}
	if dirtyBuckets < g.minDirtyBuckets && dirtyKeys < g.minDirtyKeys {
		reads := g.windowReads.Load()
		if reads <= 0 {
			return false
		}
		misses := g.windowMisses.Load()
		if misses*100 < g.targetMissHardPct*reads {
			return false
		}
	}

	if !g.checking.CompareAndSwap(0, 1) {
		return false
	}
	defer g.checking.Store(0)

	last2 := time.Unix(0, g.lastCheckUnixNano.Load())
	if !last2.IsZero() && now.Sub(last2) < g.minPromoteInterval {
		return false
	}

	missDelta := g.missSinceLastCheck.Swap(0)
	trigger := missDelta > 0 || dirtyBuckets >= g.minDirtyBuckets || dirtyKeys >= g.minDirtyKeys
	if trigger {
		g.lastCheckUnixNano.Store(now.UnixNano())
		return true
	}
	return false
}

func (g *PromoteGate) RollWindow() (reads int64, misses int64) {
	reads = g.windowReads.Swap(0)
	misses = g.windowMisses.Swap(0)
	return
}
