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

func (k *HandleKey) Value(h Handle) any {
	ConcurrentHandle.mu.RLock()
	defer ConcurrentHandle.mu.RUnlock()

	return ConcurrentHandle.handles[*k].value[h].Load().(*slot).value
}

func (k *HandleKey) Delete(h Handle) {
	ConcurrentHandle.mu.RLock()
	defer ConcurrentHandle.mu.RUnlock()

	ConcurrentHandle.handles[*k].value[h].Store(ConcurrentHandle.nll)
}
