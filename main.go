package main

import (
	"container/heap"
	"sync"
	"time"
)

type Lock struct {
	mu           sync.Mutex
	ID           string
	Owner        string
	FencingToken uint64
	ExpiresAt    time.Time
	Held         bool
	heapEntry    *LockEntry
}

type LockEntry struct {
	lock      *Lock
	expiresAt time.Time
	index     int
}

type ExpiryHeap []*LockEntry

func (h ExpiryHeap) Len() int {
	return len(h)
}

func (h ExpiryHeap) Less(i, j int) bool {
	return h[i].expiresAt.Before(h[j].expiresAt)
}

func (h ExpiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *ExpiryHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*LockEntry)
	entry.index = n
	*h = append(*h, entry)
}

func (h *ExpiryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.index = -1
	*h = old[:n-1]
	return entry
}

var (
	lockTable sync.Map
)

var (
	expiryHeap ExpiryHeap
	expiryMu   sync.Mutex
)

func expiryWorker() {

	for {
		expiryMu.Lock()

		if expiryHeap.Len() == 0 {
			expiryMu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		entry := expiryHeap[0]
		now := time.Now()

		if entry.expiresAt.After(now) {
			/*
				instead of going for few more checking, just simply take how much time still remains for the first expiry
				 to be done and and can simply be put in sleep after releasing the heap lock.
			*/
			sleep := entry.expiresAt.Sub(now)
			expiryMu.Unlock()
			time.Sleep(sleep)
			continue
		}

		heap.Pop(&expiryHeap)
		expiryMu.Unlock()

		l := entry.lock
		l.mu.Lock()

		if l.Held &&
			l.ExpiresAt.Equal(entry.expiresAt) &&
			now.After(l.ExpiresAt) {

			l.Held = false
			l.Owner = ""
			l.ExpiresAt = time.Time{}
			l.heapEntry = nil
		}

		l.mu.Unlock()
	}
}

func getLock(id string) *Lock {
	val, _ := lockTable.LoadOrStore(id, &Lock{ID: id})
	return val.(*Lock)
}

func (l *Lock) isExpired(now time.Time) bool {
	return l.Held && now.After(l.ExpiresAt)
}

func AcquireLock(id, owner string, ttl time.Duration) (uint64, bool) {
	lock := getLock(id)

	now := time.Now()
	lock.mu.Lock()

	if lock.Held && now.After(lock.ExpiresAt) {
		if lock.heapEntry != nil && lock.heapEntry.index >= 0 && lock.heapEntry.expiresAt.Equal(lock.ExpiresAt) {
			expiryMu.Lock()
			heap.Remove(&expiryHeap, lock.heapEntry.index)
			expiryMu.Unlock()
		}

		lock.Held = false
		lock.Owner = ""
		lock.ExpiresAt = time.Time{}
		lock.heapEntry = nil
	}

	if lock.Held {
		lock.mu.Unlock()
		return 0, false
	}

	lock.Held = true
	lock.Owner = owner
	lock.FencingToken++
	lock.ExpiresAt = now.Add(ttl)

	token := lock.FencingToken

	// store heap entry in each lock so that when the release gets expired we exactly
	//  know what index to remove the lease from in the heap.

	entry := &LockEntry{
		lock:      lock,
		expiresAt: lock.ExpiresAt,
	}

	lock.heapEntry = entry

	lock.mu.Unlock()

	expiryMu.Lock()
	heap.Push(&expiryHeap, entry)
	expiryMu.Unlock()

	return token, true
}

func ReleaseLock(id, owner string, token uint64) bool {

	lock := getLock(id)

	now := time.Now()
	lock.mu.Lock()

	if !lock.Held || lock.Owner != owner || lock.FencingToken != token || now.After(lock.ExpiresAt) {
		return false
	}

	lock.Held = false
	lock.Owner = ""
	lock.ExpiresAt = time.Time{}

	if lock.heapEntry != nil && lock.heapEntry.index >= 0 && lock.heapEntry.expiresAt.Equal(lock.ExpiresAt) {
		expiryMu.Lock()
		heap.Remove(&expiryHeap, lock.heapEntry.index)
		expiryMu.Unlock()

	}

	lock.heapEntry = nil
	return true
}

func CreateLock(id string) bool {
	_, loaded := lockTable.LoadOrStore(id, &Lock{ID: id})
	return !loaded
}

func init() {
	heap.Init(&expiryHeap)
}

func main() {

}
