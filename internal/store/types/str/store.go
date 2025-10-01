package str

import (
	"github.com/kisara71/CacheRing/internal/store/types"
	"sync/atomic"
	"time"
)

type Store interface {
	Set(key string, val []byte, ttl *time.Duration) error
	Get(key string) ([]byte, error)
}

type stringStore struct {
	dict *types.Shard
}

func NewStore() Store {
	return &stringStore{dict: types.NewShard()}
}
func (s *stringStore) Set(key string, val []byte, ttl *time.Duration) error {
	entry := &types.Entry{
		Value: val,
		Type:  types.String,
		ExpireAt: func() *time.Time {
			if ttl == nil {
				return nil
			}
			expireAt := time.Now().Add(*ttl)
			return &expireAt
		}(),
		Valid: atomic.Bool{},
	}
	entry.Valid.Store(true)
	s.dict.Set(key, entry)
	return nil
}

func (s *stringStore) Get(key string) ([]byte, error) {
	entry, err := s.dict.Get(key)
	if err != nil {
		return nil, err
	}
	return entry.Value.([]byte), nil
}
