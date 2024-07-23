package frankenphp

import (
	"sync"
	"sync/atomic"
)

type Handle uintptr
type HandleKey uint64

type handlePartition struct {
	value []atomic.Value
	idx   uintptr
}

// concurrentHandle represents a concurrent handle data structure for storing values associated with handles.
//
// Fields:
// - handles: A map of handle keys to handle partitions
// - nextKey: The next available handle key
// - mu: A read-write mutex for thread-safe access to the handles
// - nll: A nil slot used for deleting values
type concurrentHandle struct {
	handles map[HandleKey]*handlePartition
	nextKey HandleKey
	mu      sync.RWMutex
	nll     *slot
}

type slot struct {
	value any
}

var (
	ConcurrentHandle = concurrentHandle{
		handles: make(map[HandleKey]*handlePartition),
		nextKey: 0,
		nll:     nil,
	}
)

// NewConcurrentHandle creates a new concurrent handle with the specified number of handles.
// It acquires a lock, generates a new unique key, and initializes a new handle partition with the given number of handles.
// The new handle partition is then added to the concurrent handle map, and the lock is released.
// Finally, it returns the newly generated handle key.
//
// Go Doc:
// func (h *concurrentHandle) NewConcurrentHandle(nHandles int) HandleKey
//
// Parameters:
// - h: A pointer to the concurrentHandle struct
// - nHandles: The number of handles to initialize in the new handle partition
//
// Returns:
// - HandleKey: The newly generated handle key
//
// Example Usage:
// requestHandles = ConcurrentHandle.NewConcurrentHandle(opt.numThreads * 4)
func (h *concurrentHandle) NewConcurrentHandle(nHandles int) HandleKey {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := atomic.AddUint64((*uint64)(&h.nextKey), 1)
	h.handles[HandleKey(key)] = &handlePartition{
		value: make([]atomic.Value, nHandles+1),
		idx:   0,
	}

	return HandleKey(key)
}

// NewHandle creates a new handle with the specified value.
// It acquires a read lock, retrieves the handle partition associated with the given handle key,
// creates a new slot with the specified value, and loops to find the next available slot to store the new value.
// Once a slot is found, it uses atomic operations to swap or set the value in the slot.
// Finally, it returns the handle corresponding to the slot where the value was stored.
func (k *HandleKey) NewHandle(v any) Handle {
	ConcurrentHandle.mu.RLock()
	defer ConcurrentHandle.mu.RUnlock()

	h := ConcurrentHandle.handles[*k]
	s := &slot{value: v}

	for {
		next := atomic.AddUintptr(&h.idx, 1) % uintptr(len(h.value))
		if next == 0 {
			continue
		}

		if h.value[next].CompareAndSwap(ConcurrentHandle.nll, s) {
			return Handle(next)
		} else if h.value[next].CompareAndSwap(nil, s) {
			return Handle(next)
		}
	}
}

// Value returns the value associated with the given Handle in the handle partition associated with the given HandleKey.
// It acquires a read lock, retrieves the value from the handle partition using the provided Handle,
// and returns the value if it is not nil.
//
// Parameters:
// - k: A pointer to the HandleKey struct
// - h: The Handle to retrieve the value for
//
// Returns:
// - any: The value associated with the Handle, or nil if the value is nil
func (k *HandleKey) Value(h Handle) any {
	ConcurrentHandle.mu.RLock()
	defer ConcurrentHandle.mu.RUnlock()

	val := ConcurrentHandle.handles[*k].value[h].Load()
	if val == nil {
		return nil
	}
	return val.(*slot).value
}

// Delete deletes the value stored in the specified handle of the handle partition associated with the given HandleKey.
// It acquires a read lock, sets the value at the specified handle index to nil using atomic operations, and releases the lock.
// Parameters:
// - h: The handle key associated with the handle partition containing the value to delete.
// - h: The handle index of the value to delete.
func (k *HandleKey) Delete(h Handle) {
	ConcurrentHandle.mu.RLock()
	defer ConcurrentHandle.mu.RUnlock()

	ConcurrentHandle.handles[*k].value[h].Store(ConcurrentHandle.nll)
}
