package cuckoo

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"
)

// ASSOCIATIVITY is the set-associativity of each Cuckoo bin
const ASSOCIATIVITY int = 8

// cmap holds a number of Cuckoo bins (each with room for ASSOCIATIVITY values),
// and keeps track of the number of hashes being used.
type cmap struct {
	bins   []cbin
	hashes uint32
}

// iterate returns a channel that contains every currently set value.
// in the face of concurrent updates, some elements may be repeated or lost.
func (m *cmap) iterate() <-chan cval {
	now := time.Now()
	ch := make(chan cval)
	go func() {
		for i, bin := range m.bins {
			vals := make([]cval, 0, ASSOCIATIVITY)
			m.bins[i].mx.Lock()
			for vi := 0; vi < ASSOCIATIVITY; vi++ {
				v := bin.v(vi)
				if v != nil && v.present(now) {
					vals = append(vals, *v)
				}
			}
			m.bins[i].mx.Unlock()
			for _, cv := range vals {
				ch <- cv
			}
		}
		close(ch)
	}()
	return ch
}

// touchall changes the expiry time of all entries to be at the latest the
// given value. all concurrent modifications are blocked.
func (m *cmap) touchall(exp time.Time) {
	for i := range m.bins {
		m.bins[i].mx.Lock()
	}

	for i := range m.bins {
		for vi := 0; vi < ASSOCIATIVITY; vi++ {
			v := m.bins[i].v(vi)
			if v != nil && v.present(exp) {
				v.val.Expires = exp
			}
		}
	}

	for i := range m.bins {
		m.bins[i].mx.Unlock()
	}
}

// del removes the entry with the given key (if any), and returns its value. if
// casid is non-zero, the element will only be deleted if its id matches.
func (m *cmap) del(key keyt, casid uint64) (ret MemopRes) {
	now := time.Now()
	bins := m.kbins(key)
	defer binP.Put(bins)

	m.lock_in_order(bins...)
	defer m.unlock(bins...)

	ret.T = NOT_FOUND
	for _, bin := range bins {
		ki := m.bins[bin].has(key, now)
		if ki != -1 {
			v := &m.bins[bin].v(ki).val

			if casid != 0 && v.Casid != casid {
				ret.T = EXISTS
				return
			}

			ret.T = STORED
			ret.V = v
			m.bins[bin].kill(ki)
			return
		}
	}
	return
}

// insert sets or updates the entry with the given key.
// the update function is used to determine the new value, and is passed the
// old value under a lock.
func (m *cmap) insert(key keyt, upd Memop) (ret MemopRes) {
	now := time.Now()
	bins := m.kbins(key)
	ival := cval{key: key}

	// Check if this element is already present
	m.lock_in_order(bins...)
	for bi, bin := range bins {
		b := &m.bins[bin]
		ki := b.has(key, now)
		if ki != -1 {
			ival.bno = bi
			ival.val, ret = upd(b.v(ki).val, true)
			if ret.T == STORED {
				b.setv(ki, &ival)
			}
			m.unlock(bins...)
			binP.Put(bins)
			return
		}
	}
	m.unlock(bins...)

	// Item not currently present, is there room without a search?
	for i, b := range bins {
		if m.bins[b].available(now) {
			ival.bno = i
			ret = m.bins[b].add(&ival, upd, now)
			if ret.T != SERVER_ERROR {
				binP.Put(bins)
				return
			}
		}
	}

	// Keep trying to find a cuckoo path of replacements
	for {
		path := m.search(now, bins...)
		if path == nil {
			// XXX: ideally we'd do a resize here, but without
			// locking everything...
			binP.Put(bins)
			return MemopRes{
				T: SERVER_ERROR,
				V: errors.New("no storage space found for element"),
			}
		}

		freeing := path[0].from

		// recompute bins because #hashes might have changed
		binP.Put(bins)
		bins = m.kbins(key)

		// sanity check that this path will make room in the right bin
		tobin := -1
		for i, bin := range bins {
			if freeing == bin {
				tobin = i
			}
		}
		if tobin == -1 {
			panic(fmt.Sprintf("path %v leads to occupancy in bin %v, but is unhelpful for key %s with bins: %v", path, freeing, key, bins))
		}

		// only after the search do we acquire locks
		if m.validate_execute(path, now) {
			ival.bno = tobin

			// after replacements, someone else might have beaten
			// us to the free slot, so we need to do add under a
			// lock too
			ret = m.bins[freeing].add(&ival, upd, now)
			if ret.T != SERVER_ERROR {
				binP.Put(bins)
				return
			}
		}
	}
}

// get returns the current value (if any) for the given key
func (m *cmap) get(key keyt) (ret MemopRes) {
	now := time.Now()
	bins := m.kbins(key)
	defer binP.Put(bins)

	ret.T = NOT_FOUND
	for _, bin := range bins {
		b := &m.bins[bin]
		for i := 0; i < ASSOCIATIVITY; i++ {
			s := b.v(i)
			if s != nil && s.present(now) && bytes.Equal(s.key, key) {
				ret.T = EXISTS
				ret.V = &s.val
				return
			}
		}
	}
	return
}

// lock_in_order will acquire the given locks in a fixed order that ensures
// competing lockers will not deadlock.
func (m *cmap) lock_in_order(bins ...int) {
	locks := make([]int, len(bins))
	for i := range bins {
		locks[i] = bins[i]
	}

	sort.Ints(locks)
	last := -1
	for _, bin := range locks {
		if bin != last {
			m.bins[bin].mx.Lock()
			last = bin
		}
	}
}

// unlock will release the given locks while ensuring no lock is released
// multiple times.
func (m *cmap) unlock(bins ...int) {
	locks := make([]int, len(bins))
	for i := range bins {
		locks[i] = bins[i]
	}

	sort.Ints(locks)
	last := -1
	for _, bin := range locks {
		if bin != last {
			m.bins[bin].mx.Unlock()
			last = bin
		}
	}
}
