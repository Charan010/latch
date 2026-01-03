package lease

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

var (
	lockTable  sync.Map
	expiryHeap ExpiryHeap
	expiryMu   sync.Mutex
)

func Init() {
	heap.Init(&expiryHeap)
}

func CreateLock(id string) bool {
	_, loaded := lockTable.LoadOrStore(id, &Lock{ID: id})
	return !loaded
}

func AcquireLock(id, owner string, ttl time.Duration) (uint64, bool) {
	lock := getLock(id)
	now := time.Now()

	var staleEntry *LockEntry

	lock.mu.Lock()

	// Expired cleanup
	if lock.Held && now.After(lock.ExpiresAt) {
		staleEntry = lock.heapEntry
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

	entry := &LockEntry{
		lock:      lock,
		expiresAt: lock.ExpiresAt,
	}
	lock.heapEntry = entry

	lock.mu.Unlock()

	if staleEntry != nil && staleEntry.index >= 0 {
		expiryMu.Lock()
		heap.Remove(&expiryHeap, staleEntry.index)
		expiryMu.Unlock()
	}

	expiryMu.Lock()
	heap.Push(&expiryHeap, entry)
	expiryMu.Unlock()

	return token, true
}

func ReleaseLock(id, owner string, token uint64) bool {
	lock := getLock(id)
	now := time.Now()

	lock.mu.Lock()
	defer lock.mu.Unlock()

	if !lock.Held ||
		lock.Owner != owner ||
		lock.FencingToken != token ||
		now.After(lock.ExpiresAt) {
		return false
	}

	entry := lock.heapEntry

	lock.Held = false
	lock.Owner = ""
	lock.ExpiresAt = time.Time{}
	lock.heapEntry = nil

	if entry != nil && entry.index >= 0 {
		expiryMu.Lock()
		heap.Remove(&expiryHeap, entry.index)
		expiryMu.Unlock()
	}

	return true
}

func ExpiryWorker() {

	Init()

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
			time.Now().After(l.ExpiresAt) {
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
