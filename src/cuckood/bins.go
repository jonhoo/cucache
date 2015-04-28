package cuckoo

import (
	"sync/atomic"
	"time"
	"unsafe"
)

// offset64 is the fnv1a 64-bit offset
var offset64 uint64 = 14695981039346656037

// prime64 is the fnv1a 64-bit prime
var prime64 uint64 = 1099511628211

var intoffs []uint = []uint{0, 8, 16, 24}

// bin returns the nth hash of the given key
func (m *cmap) bin(n int, key keyt) int {
	s := offset64
	for _, c := range key {
		s ^= uint64(c)
		s *= prime64
	}
	for _, i := range intoffs {
		s ^= uint64(n >> i)
		s *= prime64
	}
	return int(s & (uint64(len(m.bins)) - 1))
}

// kbins returns all hashes of the given key.
// as m.hashes increases, this function will return more hashes.
func (m *cmap) kbins(key keyt, into []int) {
	nb := uint64(len(m.bins)) - 1

	// only hash the key once
	s := offset64
	for _, c := range key {
		s ^= uint64(c)
		s *= prime64
	}

	for i := 0; i < len(into); i++ {
		// compute key for this i
		s_ := s
		for _, o := range intoffs {
			s_ ^= uint64(i >> o)
			s_ *= prime64
		}
		into[i] = int(s_ & nb)
	}
}

type aval struct {
	val  unsafe.Pointer
	tag  byte
	read bool
}

// cbin is a single Cuckoo map bin holding up to ASSOCIATIVITY values.
// each bin has a lock that must be used for *writes*.
// values should never be accessed directly, but rather through v()
type cbin struct {
	vals [ASSOCIATIVITY]aval
	mx   SpinLock
}

// v returns a pointer to the current key data for a given slot (if any).
// this function may return nil if no key data is set for the given slot.
// this function is safe in the face of concurrent updates, assuming writers
// use setv().
func (b *cbin) v(i int) *cval {
	return (*cval)(atomic.LoadPointer(&b.vals[i].val))
}

// vpresent returns true if the given slot contains unexpired key data
func (b *cbin) vpresent(i int, now time.Time) bool {
	v := b.v(i)
	return v != nil && v.present(now)
}

// setv will atomically update the key data for the given slot
func (b *cbin) setv(i int, v *cval) {
	tov := &b.vals[i]
	if v != nil {
		tov.tag = v.key[0]
	}
	atomic.StorePointer(&tov.val, unsafe.Pointer(v))
}

// subin atomically replaces the first free slot in this bin with the given key
// data
func (b *cbin) subin(v *cval, now time.Time) {
	for i := 0; i < ASSOCIATIVITY; i++ {
		if !b.vpresent(i, now) {
			b.setv(i, v)
			return
		}
	}
}

// kill will immediately and atomically invalidate the given slot's key data
func (b *cbin) kill(i int) {
	b.setv(i, nil)
}

// available returns true if this bin has a slot that is currently unoccupied
// or expired
func (b *cbin) available(now time.Time) bool {
	for i := 0; i < ASSOCIATIVITY; i++ {
		if !b.vpresent(i, now) {
			return true
		}
	}
	return false
}

// add will atomically replace the first available slot in this bin with the
// given key data. this function may return an error if there are no free
// slots.
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

// has returns the slot holding the key data for the given key in this bin.
// if no slot has the relevant key data, -1 is returned.
func (b *cbin) has(key keyt, now time.Time) (i int, v *cval) {
	for i = 0; i < ASSOCIATIVITY; i++ {
		if b.vals[i].tag == key[0] {
			v = b.v(i)
			if v != nil && v.holds(key, now) {
				return
			}
		}
	}
	return -1, nil
}
