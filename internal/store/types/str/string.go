package str

import "time"

type Entry struct {
	value []byte
	ttl   *time.Time
}
