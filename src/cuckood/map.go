package cuckoo

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"
)

const ASSOCIATIVITY_E uint = 3

// ASSOCIATIVITY is the set-associativity of each Cuckoo bin
const ASSOCIATIVITY int = 1 << ASSOCIATIVITY_E

// cmap holds a number of Cuckoo bins (each with room for ASSOCIATIVITY values),
// and keeps track of the number of hashes being used.
type cmap struct {
	bins         []cbin
	hashes       uint32
	requestEvict chan chan struct{}
	evicted      uint
}

// create allocates a new Cuckoo map of the given size.
// Two hash functions are used.
func create(bins uint64) *cmap {
	if bins == 0 {
		panic("tried to create empty cuckoo map")
	}

	if bins&(bins-1) != 0 {
		// unless explicitly told otherwise, we'll create a decently
		// sized table by default
		var shift uint64 = 1 << 10
		for bins > shift {
			shift <<= 1
		}
		bins = shift
	}

	// since each bin can hold ASSOCIATIVITY elements
	// we don't need as many bins
	bins >>= ASSOCIATIVITY_E

	if bins == 0 {
		bins = 1
	}
	fmt.Fprintln(os.Stderr, "will initialize with", bins, "bins")

	m := new(cmap)
	m.bins = make([]cbin, bins)
	m.hashes = 2
	return m
}

func (m *cmap) enableEviction() {
	if m.requestEvict == nil {
		m.requestEvict = make(chan chan struct{})
		go m.processEvictions()
	}
}

func (m *cmap) processEvictions() {
	var echan chan struct{}
	now := time.Now()
	for {
		for i, bin := range m.bins {
			for vi := 0; vi < ASSOCIATIVITY; vi++ {
				v := bin.v(vi)
				// evict should be required to actually
				// evict an item. just because this
				// slot is free doesn't mean there is
				// room for a new element of a
				// particular key!
				if v == nil || !v.present(now) {
					continue
				}

				// we don't evict recently read items.
				// if they're recently read, we label
				// them as not recently read.
				if bin.vals[vi].read {
					bin.vals[vi].read = false
					continue
				}

				// we've moved to the first evictable
				// record. now we just wait for someone
				// to tell us to evict (unless we're
				// already trying to evict something).
				if echan == nil {
					var ok bool
					if echan, ok = <-m.requestEvict; !ok {
						// make sure we haven't
						// been told to
						// terminate
						return
					}
				}

				// new eviction request came in! make
				// sure we have an up-to-date time
				// estimate -- we might have slept for
				// a long time
				now = time.Now()

				// we now need to redo the checks under
				// a lock to ensure the element hasn't
				// changed. note that we need m.bins[i]
				// here because bin is a *copy*.
				m.bins[i].mx.Lock()
				v = bin.v(vi)
				if v != nil && v.present(now) && !bin.vals[vi].read {
					//fmt.Fprintln(os.Stderr, "evicting", v.key)
					m.bins[i].kill(vi)
					m.evicted++

					m.bins[i].mx.Unlock()

					echan <- struct{}{}
					echan = nil
				} else {
					m.bins[i].mx.Unlock()
				}
			}
		}
	}
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
	bins := make([]int, int(m.hashes))
	m.kbins(key, bins)

	m.lock_in_order(bins...)
	defer m.unlock(bins...)

	ret.T = NOT_FOUND
	for _, bin := range bins {
		ki, v := m.bins[bin].has(key, now)
		if ki != -1 {
			v := &v.val

			if casid != 0 && v.Casid != casid {
				ret.T = EXISTS
				return
			}

			ret.T = STORED
			ret.M = v
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
	ival := cval{key: key}

	// we do some additional trickery here so that when we recompute bins
	// in the loop below, we don't need to do further allocations
	nh := int(m.hashes)
	var bins_ [MAX_HASHES]int
	bins := bins_[0:nh]
	m.kbins(key, bins)

	// Check if this element is already present
	m.lock_in_order(bins...)
	for bi, bin := range bins {
		b := &m.bins[bin]
		ki, v := b.has(key, now)
		if ki != -1 {
			ival.bno = bi
			ival.val, ret = upd(v.val, true)
			if ret.T == STORED {
				b.setv(ki, &ival)
				b.vals[ki].read = true
				b.vals[ki].tag = key[0]
			}
			m.unlock(bins...)
			return
		}
	}
	m.unlock(bins...)

	// if the operation fails if a current element does not exist,
	// there is no point doing the expensive insert search
	_, ret = upd(Memval{}, false)
	if ret.T != STORED {
		return ret
	}

	// Item not currently present, is there room without a search?
	for i, b := range bins {
		if m.bins[b].available(now) {
			ival.bno = i
			ret = m.bins[b].add(&ival, upd, now)
			if ret.T != SERVER_ERROR {
				return
			}
		}
	}

	// Keep trying to find a cuckoo path of replacements
	for {
		path := m.search(now, bins...)
		if path == nil {
			if m.evict() {
				return m.insert(key, upd)
			}
			return MemopRes{
				T: SERVER_ERROR,
				E: errors.New("no storage space found for element"),
			}
		}

		freeing := path[0].from

		// recompute bins because #hashes might have changed
		if nh != int(m.hashes) {
			nh = int(m.hashes)
			bins = bins_[0:nh]
			m.kbins(key, bins)
		}

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
				return
			}
		}
	}
}

// get returns the current value (if any) for the given key
func (m *cmap) get(key keyt) (ret MemopRes) {
	now := time.Now()
	var bins_ [MAX_HASHES]int
	bins := bins_[0:int(m.hashes)]
	m.kbins(key, bins)

	ret.T = NOT_FOUND
	for _, bin := range bins {
		if i, v := m.bins[bin].has(key, now); v != nil {
			m.bins[bin].vals[i].read = true
			ret.T = EXISTS
			ret.M = &v.val
			return
		}
	}
	return
}

func (m *cmap) evict() bool {
	if m.requestEvict != nil {
		ret := make(chan struct{})
		m.requestEvict <- ret
		<-ret
		return true
	}
	return false
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
