package str

import (
	"time"
)

type Store interface {
	Set(key string, val []byte, ttl *time.Duration) error
	Get(key string) ([]byte, error)
}
