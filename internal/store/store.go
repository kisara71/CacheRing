package store

import "github.com/kisara71/CacheRing/internal/store/types/str"

type Store interface {
	str.Store
}

type ShardStore struct {
	shard []*Shard
	count uint32
}
