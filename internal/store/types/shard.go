package types

import (
	"github.com/cespare/xxhash/v2"
	"github.com/kisara71/CacheRing/pkg/err"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bucketCount uint64 = 1024
)

type Shard struct {
	fastRead atomic.Pointer[fastView]

	promotePerCnt int
	promoteBudget time.Duration

	dirty             []*bucket
	dirtyLock         []sync.RWMutex
	dirtyBuckets      []atomic.Bool
	dirtyBucketsCount atomic.Int32
	dirtyKeyCount     atomic.Int64

	pg     *PromoteGate
	reqSeq atomic.Uint64
}

type fastView struct {
	buckets []*bucket
}
type bucket struct {
	dict map[string]*elem
}

func bucketIndex(key string) int {
	h := xxhash.Sum64String(key)
	return int(h & (bucketCount - 1))
}
func NewShard() *Shard {
	sh := &Shard{
		dirty:         make([]*bucket, int(bucketCount)),
		dirtyBuckets:  make([]atomic.Bool, int(bucketCount)),
		dirtyLock:     make([]sync.RWMutex, int(bucketCount)),
		pg:            NewPromoteGate(),
		promoteBudget: time.Millisecond * 5,
		promotePerCnt: 8,
	}
	viewBuckets := make([]*bucket, int(bucketCount))
	dirtyBuckets := make([]*bucket, int(bucketCount))
	for i := 0; i < int(bucketCount); i++ {
		vb := &bucket{
			dict: make(map[string]*elem),
		}
		db := &bucket{
			dict: make(map[string]*elem),
		}
		viewBuckets[i] = vb
		dirtyBuckets[i] = db
	}
	sh.fastRead.Store(&fastView{
		buckets: viewBuckets,
	})
	sh.dirty = dirtyBuckets
	return sh
}
func (s *Shard) Set(key string, entry *Entry) {
	i := bucketIndex(key)
	s.dirtyLock[i].Lock()
	defer s.dirtyLock[i].Unlock()
	if e, ok := s.dirty[i].dict[key]; ok {
		e.entry.Store(entry)
	} else {
		s.dirtyKeyCount.Add(1)
		e := &elem{}
		e.entry.Store(entry)
		s.dirty[i].dict[key] = e
	}
	if !s.dirtyBuckets[i].Swap(true) {
		s.dirtyBucketsCount.Add(1)
	}
}
func (s *Shard) Get(key string) (*Entry, error) {
	s.pg.OnRead()

	view := s.fastRead.Load()
	i := bucketIndex(key)
	b := view.buckets[i]
	if e, ok := b.dict[key]; ok {
		entry := e.entry.Load()
		if !entry.IsExpired() && entry.Valid.Load() {
			return entry, nil
		}
	}
	if !s.dirtyBuckets[i].Swap(true) {
		s.dirtyBucketsCount.Add(1)
	}
	s.pg.OnMiss()
	entry, ok := s.getDirty(i, key)
	if !ok {
		return nil, err.KeyNotFound
	}
	if entry.IsExpired() {
		entry.Valid.Store(false)
		s.delete(key)
		return nil, err.KeyNotFound
	}
	if s.pg.ShouldPromote(time.Now(), s.reqSeq.Add(1), int(s.dirtyBucketsCount.Load()), s.dirtyKeyCount.Load()) {
		go s.promote()
	}
	return entry, nil
}

func (s *Shard) delete(key string) {
	i := bucketIndex(key)
	s.dirtyLock[i].Lock()
	defer s.dirtyLock[i].Unlock()
	if _, ok := s.dirty[i].dict[key]; !ok {
		return
	}
	if !s.dirtyBuckets[i].Swap(true) {
		s.dirtyBucketsCount.Add(1)
	}
	delete(s.dirty[i].dict, key)
}
func (s *Shard) getDirty(i int, key string) (entry *Entry, ok bool) {
	s.dirtyLock[i].RLock()
	defer s.dirtyLock[i].RUnlock()
	snap, ok := s.dirty[i].dict[key]
	if !ok {
		return nil, false
	}
	if !s.dirtyBuckets[i].Swap(true) {
		s.dirtyBucketsCount.Add(1)
	}
	entry = snap.entry.Load()
	return entry, ok
}
func (s *Shard) promote() {
	start := time.Now()
	oldView := s.fastRead.Load()
	var newView fastView
	newView.buckets = make([]*bucket, len(oldView.buckets))
	copy(newView.buckets, oldView.buckets)

	rebuild := 0
	for i := 0; i < len(oldView.buckets) && rebuild < s.promotePerCnt; i++ {
		if !s.dirtyBuckets[i].Load() {
			continue
		}
		rebuild++
		newBucket := make(map[string]*elem, len(oldView.buckets[i].dict))
		for k, v := range oldView.buckets[i].dict {
			e := v.entry.Load()
			if e.Valid.Load() && !e.IsExpired() {
				newBucket[k] = v
			}
		}

		s.dirtyLock[i].Lock()
		for k, v := range s.dirty[i].dict {
			entry := v.entry.Load()
			if entry.IsExpired() || !entry.Valid.Load() {
				continue
			}
			newBucket[k] = v
		}
		s.dirtyKeyCount.Add(-int64(len(s.dirty[i].dict)))
		s.dirty[i].dict = make(map[string]*elem)
		if s.dirtyBuckets[i].Swap(false) {
			s.dirtyBucketsCount.Add(-1)
		}
		newView.buckets[i] = &bucket{
			dict: newBucket,
		}
		s.dirtyLock[i].Unlock()
		if time.Since(start) > s.promoteBudget {
			break
		}
	}
	s.fastRead.Store(&newView)
}
