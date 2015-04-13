package cuckoo

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"sync/atomic"
	"time"
	"unsafe"
)

func (m *cmap) bin(n int, key keyt) int {
	h := fnv.New64a()
	h.Write(key)

	var bs [8]byte
	binary.PutVarint(bs[:], int64(n))
	h.Write(bs[:])

	return int(h.Sum64() % uint64(len(m.bins)))
}

func (m *cmap) kbins(key keyt) []int {
	bins := make([]int, 0, m.hashes)
	h := fnv.New64a()
	for i := 0; i < int(m.hashes); i++ {
		h.Reset()
		h.Write(key)

		var bs [8]byte
		binary.PutVarint(bs[:], int64(i))
		h.Write(bs[:])

		bins = append(bins, int(h.Sum64()%uint64(len(m.bins))))
	}
	return bins
}

type cbin struct {
	vals [ASSOCIATIVITY]unsafe.Pointer
	mx   SpinLock
}

func (b *cbin) v(i int) *cval {
	v := atomic.LoadPointer(&b.vals[i])
	return (*cval)(v)
}

func (b *cbin) vpresent(i int, now time.Time) bool {
	v := b.v(i)
	return v != nil && v.present(now)
}

func (b *cbin) setv(i int, v *cval) {
	atomic.StorePointer(&b.vals[i], unsafe.Pointer(v))
}

func (b *cbin) subin(v *cval, now time.Time) {
	for i := 0; i < ASSOCIATIVITY; i++ {
		if !b.vpresent(i, now) {
			b.setv(i, v)
			return
		}
	}
}

func (b *cbin) kill(i int) {
	b.setv(i, nil)
}

func (b *cbin) available(now time.Time) bool {
	for i := 0; i < ASSOCIATIVITY; i++ {
		if !b.vpresent(i, now) {
			return true
		}
	}
	return false
}

func (b *cbin) add(val *cval, upd Memop, now time.Time) (ret MemopRes) {
	b.mx.Lock()
	defer b.mx.Unlock()

	ret.T = SERVER_ERROR
	if b.available(now) {
		val.val, ret = upd(val.val, false)
		if ret.T == STORED {
			b.subin(val, now)
		}
		return
	}
	return
}

func (b *cbin) has(key keyt, now time.Time) int {
	for i := 0; i < ASSOCIATIVITY; i++ {
		v := b.v(i)
		if v != nil && v.present(now) && bytes.Equal(v.key, key) {
			return i
		}
	}
	return -1
}
