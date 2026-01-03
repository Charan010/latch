package lease

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
	entry := x.(*LockEntry)
	entry.index = len(*h)
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
