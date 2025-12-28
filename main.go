package main

import (
	"sync/atomic"
	"time"
)

type Lock struct {
	ID           string
	Owner        string
	fencingToken atomic.Uint64
	ExpiresAt    time.Time
	Held         bool
}

func (l *Lock) isExpired(now time.Time) bool {
	return l.Held && now.After(l.ExpiresAt)
}

func (l *Lock) acquireLock(owner string, ttl time.Duration, now time.Time) (uint64, bool) {

	if l.isExpired(now) {
		l.Held = false
		l.Owner = ""
		l.ExpiresAt = time.Time{}

	}

	if l.Held {
		return 0, false
	}

	l.Held = true
	l.Owner = owner
	l.fencingToken.Add(1)
	l.ExpiresAt = now.Add(ttl)

	return l.fencingToken.Load(), true
}

func (l *Lock) releaseLock(owner string, token uint64, now time.Time) bool {

	if l.isExpired(now) {
		l.Held = false
		l.Owner = ""
		l.ExpiresAt = time.Time{}
	}

	if !l.Held {
		return false
	}

	if l.Owner != owner || l.fencingToken.Load() != token {
		return false
	}

	l.Held = false
	l.Owner = ""
	l.ExpiresAt = time.Time{}

	return true

}

func main() {

}
