package store

import (
	"github.com/kisara71/CacheRing/pkg/err"
	"sync"
	"sync/atomic"
)

type Shard struct {
	fastRead atomic.Value
	dirty    map[string]*elem
	miss     atomic.Int32
	lock     sync.RWMutex
}
type elem struct {
	entry atomic.Value
}

func NewShard() *Shard {
	fr := atomic.Value{}
	fr.Store(make(map[string]*elem))
	return &Shard{
		fastRead: fr,
		dirty:    make(map[string]*elem),
		miss:     atomic.Int32{},
		lock:     sync.RWMutex{},
	}
}
func (s *Shard) Set(key string, entry *Entry) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if e, ok := s.dirty[key]; ok {
		e.entry.Store(entry)
	} else {
		entry.Valid.Store(true)
		e := &elem{}
		e.entry.Store(entry)
		s.dirty[key] = e
	}
}
func (s *Shard) Get(key string) (*Entry, error) {
	if s.shouldPromote() {
		s.promote()
	}
	readSnap := s.fastRead.Load().(map[string]*elem)
	if e, ok := readSnap[key]; ok {
		entry := e.entry.Load().(*Entry)
		if !entry.IsExpired() && entry.Valid.Load() {
			return entry, nil
		}
	}
	s.miss.Add(1)
	entry, ok := s.getDirty(key)
	if !ok {
		return nil, err.KeyNotFound
	}
	if entry.IsExpired() {
		entry.Valid.Store(false)
		s.delete(key)
		return nil, err.KeyNotFound
	}
	return entry, nil
}

func (s *Shard) delete(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.dirty, key)
}
func (s *Shard) getDirty(key string) (entry *Entry, ok bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	snap, ok := s.dirty[key]
	if !ok {
		return nil, false
	}
	entry = snap.entry.Load().(*Entry)
	return entry, ok
}
func (s *Shard) shouldPromote() bool {
	return s.miss.Load() > 64
}
func (s *Shard) promote() {
	s.lock.Lock()
	defer s.lock.Unlock()

	newFast := make(map[string]*elem, len(s.dirty))
	for k, v := range s.dirty {
		entry := v.entry.Load().(*Entry)
		if entry.IsExpired() || !entry.Valid.Load() {
			delete(s.dirty, k)
			continue
		}
		newFast[k] = v
	}
	s.fastRead.Store(newFast)
	s.miss.Store(0)
}
