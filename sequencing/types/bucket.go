package types

import (
	"container/heap"
	"sort"
	"sync"

	"github.com/cometbft/cometbft/p2p"
)

// SELF_PEER_ID marks entries originating from the local node.
const SELF_PEER_ID p2p.ID = "self"

type bucketEntry[T any] struct {
	height int64
	value  T
}

type heapItem struct {
	peer   p2p.ID
	height int64
}

type peerHeap struct {
	items []heapItem
	pos   map[p2p.ID]int
}

func newPeerHeap() peerHeap {
	return peerHeap{
		items: make([]heapItem, 0),
		pos:   make(map[p2p.ID]int),
	}
}

func (h peerHeap) Len() int { return len(h.items) }

func (h peerHeap) Less(i, j int) bool {
	if h.items[i].height != h.items[j].height {
		return h.items[i].height < h.items[j].height
	}
	return h.items[i].peer < h.items[j].peer
}

func (h peerHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.pos[h.items[i].peer] = i
	h.pos[h.items[j].peer] = j
}

func (h *peerHeap) Push(x any) {
	item := x.(heapItem)
	h.items = append(h.items, item)
	h.pos[item.peer] = len(h.items) - 1
}

func (h *peerHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	delete(h.pos, item.peer)
	return item
}

func (h *peerHeap) peek() (heapItem, bool) {
	if len(h.items) == 0 {
		return heapItem{}, false
	}
	return h.items[0], true
}

func (h *peerHeap) updateHeight(peer p2p.ID, height int64) {
	idx, ok := h.pos[peer]
	if !ok {
		return
	}
	h.items[idx].height = height
	heap.Fix(h, idx)
}

func (h *peerHeap) remove(peer p2p.ID) {
	idx, ok := h.pos[peer]
	if !ok {
		return
	}
	heap.Remove(h, idx)
}

// P2PBucket maintains per-peer, height-ordered queues and serves the lowest
// height across all peers.
type P2PBucket[T any] struct {
	mu     sync.RWMutex
	queues map[p2p.ID][]bucketEntry[T]
	heap   peerHeap
	total  int
}

// NewP2PBucket constructs an empty bucket.
func NewP2PBucket[T any]() *P2PBucket[T] {
	return &P2PBucket[T]{
		queues: make(map[p2p.ID][]bucketEntry[T]),
		heap:   newPeerHeap(),
	}
}

// Add inserts the value for the peer at the given height while keeping the
// peer queue sorted by height.
func (b *P2PBucket[T]) Add(id p2p.ID, height int64, value T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue, existed := b.queues[id]
	if !existed {
		queue = nil
	}

	entry := bucketEntry[T]{height: height, value: value}
	idx := sort.Search(len(queue), func(i int) bool {
		return entry.height < queue[i].height
	})
	frontChanged := idx == 0
	if idx == len(queue) {
		queue = append(queue, entry)
	} else {
		queue = append(queue, bucketEntry[T]{})
		copy(queue[idx+1:], queue[idx:])
		queue[idx] = entry
	}
	b.queues[id] = queue
	b.total++

	if !existed {
		heap.Push(&b.heap, heapItem{peer: id, height: queue[0].height})
		return
	}

	if frontChanged {
		b.heap.updateHeight(id, queue[0].height)
	}
}

// PopLowest removes and returns the lowest-height entry across all peers.
func (b *P2PBucket[T]) PopLowest() (id p2p.ID, height int64, value T, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.heap.Len() == 0 {
		var zero T
		return "", 0, zero, false
	}

	item := heap.Pop(&b.heap).(heapItem)
	queue := b.queues[item.peer]
	entry := queue[0]
	queue = queue[1:]
	b.total--

	if len(queue) == 0 {
		delete(b.queues, item.peer)
	} else {
		b.queues[item.peer] = queue
		heap.Push(&b.heap, heapItem{peer: item.peer, height: queue[0].height})
	}

	return item.peer, entry.height, entry.value, true
}

// PeekLowest returns the lowest-height entry without removing it.
func (b *P2PBucket[T]) PeekLowest() (id p2p.ID, height int64, value T, ok bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	item, ok := b.heap.peek()
	if !ok {
		var zero T
		return "", 0, zero, false
	}

	entry := b.queues[item.peer][0]
	return item.peer, entry.height, entry.value, true
}

// RemovePeer drops all entries associated with the peer.
func (b *P2PBucket[T]) RemovePeer(id p2p.ID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue, ok := b.queues[id]
	if !ok {
		return false
	}

	b.total -= len(queue)
	delete(b.queues, id)
	b.heap.remove(id)
	return true
}

// HasPeer reports whether the peer has queued entries.
func (b *P2PBucket[T]) HasPeer(id p2p.ID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	_, ok := b.queues[id]
	return ok
}

// Len returns the total number of queued entries.
func (b *P2PBucket[T]) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.total
}

// PeerLen reports the number of entries queued for the peer.
func (b *P2PBucket[T]) PeerLen(id p2p.ID) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	queue, ok := b.queues[id]
	if !ok {
		return 0
	}
	return len(queue)
}

// IsEmpty reports whether the bucket holds any entries.
func (b *P2PBucket[T]) IsEmpty() bool {
	return b.Len() == 0
}

// HasHeight reports whether any peer has an entry for the height.
func (b *P2PBucket[T]) HasHeight(height int64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, queue := range b.queues {
		idx := sort.Search(len(queue), func(i int) bool {
			return queue[i].height >= height
		})
		if idx < len(queue) && queue[idx].height == height {
			return true
		}
	}

	return false
}

// Remove drops the entry for the given height and returns its peer and value.
func (b *P2PBucket[T]) Remove(height int64) (id p2p.ID, value T, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for id, queue := range b.queues {
		idx := sort.Search(len(queue), func(i int) bool {
			return queue[i].height >= height
		})
		if idx == len(queue) || queue[idx].height != height {
			continue
		}

		entry := queue[idx]
		frontChanged := idx == 0

		copy(queue[idx:], queue[idx+1:])
		queue = queue[:len(queue)-1]
		b.total--

		if len(queue) == 0 {
			delete(b.queues, id)
			b.heap.remove(id)
		} else {
			b.queues[id] = queue
			if frontChanged {
				b.heap.updateHeight(id, queue[0].height)
			}
		}

		return id, entry.value, true
	}

	var zero T
	return "", zero, false
}
