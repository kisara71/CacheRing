package store

import (
	"sync/atomic"
	"time"
)

type TYPE uint8

const (
	String TYPE = iota
	Hash
	SortedSet
)

type Entry struct {
	Value    any
	Type     TYPE
	ExpireAt *time.Time
	Valid    atomic.Bool
}

func (e *Entry) IsExpired() bool {
	if e.ExpireAt == nil {
		return false
	}
	return time.Now().After(*e.ExpireAt)
}
